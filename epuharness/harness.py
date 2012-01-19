import os
import uuid
import time
import yaml
import logging
import tempfile
import dashi.bootstrap as bootstrap 

from socket import timeout
from pidantic.supd.pidsupd import SupDPidanticFactory
from epu.states import InstanceState
from epu.dashiproc.processdispatcher import ProcessDispatcherClient

from util import get_config_paths
from deployment import parse_deployment, DEFAULT_DEPLOYMENT

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

ADVERTISE_RETRIES = 10

class EPUHarness(object):
    """EPUHarness. Sets up Process Dispatchers and EEAgents for testing.
    """
    #TODO: add frameowrk for pyon messaging


    def __init__(self, exchange=None, pidantic_dir=None):

        configs = ["epuharness"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        self.pidantic_dir = pidantic_dir or self.CFG.epuharness.pidantic_dir
        self.exchange = exchange or str(uuid.uuid4())
        self.CFG.dashi.exchange = self.exchange
        self.dashi = bootstrap.dashi_connect(self.CFG.dashi.topic, self.CFG)
        self.process_dispatchers = []

        try:
            os.makedirs(self.pidantic_dir)
        except OSError:
            pass

        self.factory = SupDPidanticFactory(directory=self.pidantic_dir, name="epu-harness")

    def stop(self, force=False):
        """Stop services that were previously started by epuharness

        @param force: When False raises an exception when there is something
                      that can't be killed.
        """
        instances = self.factory.reload_instances()
        if instances:
            log.debug("Killing %s" % ", ".join(instances.keys()))
        for instance in instances.values():
            instance.cleanup()
        self.factory.terminate()

    def start(self, deployment_file=None):
        """Start services defined in the deployment file provided. If a
        deployment file isn't provided, then start a standard set of one 
        Process Dispatcher and one eeagent.

        @param deployment_file: The path to a deployment file. Format is in the
                                README
        """

        if deployment_file:
            deployment = parse_deployment(yaml_path=deployment_file)
        else:
            deployment = parse_deployment(yaml_str=DEFAULT_DEPLOYMENT)


        self.process_dispatchers = deployment.get('process-dispatchers', {})
        for pd_name, pd in self.process_dispatchers.iteritems():
            log.debug("STARTING PD")
            self._start_process_dispatcher(pd_name, pd.get('engines', {}))


        nodes = deployment.get('nodes', {})
        for node_name, node in nodes.iteritems():

            self.announce_node(node_name, node.get('dt', ''))

            for eeagent in node.get('eeagents', []):
                self._start_eeagent(eeagent['name'], eeagent['process-dispatcher'])

        
    def _start_process_dispatcher(self, name, engines,
            exe_name="epu-processdispatcher-service"):
        """Starts a process dispatcher with SupervisorD

        @param name: Name of process dispatcher to start
        @param engines: a dictionary of eeagent configs. Same format as the 
                Process Dispatcher config file
        @param exe_name: the name of the process dispatcher executable
        """

        config_file = self._build_process_dispatcher_config(self.exchange,
                name, engines)

        cmd = "%s %s" % (exe_name, config_file)
        log.debug(cmd)
        pid = self.factory.get_pidantic(command=cmd, process_name=name,
                directory=self.pidantic_dir)
        pid.start()


    def _build_process_dispatcher_config(self, exchange, name, engines,
            log_file=None):
        """Builds a yaml config file to feed to the process dispatcher

        @param exchange: the AMQP exchange the service should be on
        @param name: name of the process dispatcher, used as the topic to be
                addressed on AMQP
        @param engines: a dictionary of eeagent configs. Same format as the     
                Process Dispatcher config file
        @param log_file: the log file for the Process Dispatcher
        """
        if not log_file:
            log_file = "/tmp/pd.log"

        config = {
          'dashi': {
            'exchange': exchange,
          },
          'processdispatcher': {
            'topic': name,
            'engines': engines,
          },
          'logging': {
            'loggers': {
              'processdispatcher': {
                'handlers': ['file', 'console']
              }
            },
            'handlers': {
              'file': {
                'filename': log_file,
              }
            },
            'root': {
              'handlers': ['file', 'console']
            }
          }
        }

        config_yaml = yaml.dump(config)

        (os_handle, config_filename) = tempfile.mkstemp(suffix='.yml')
        os.write(os_handle, config_yaml)
        os.close(os_handle)

        return config_filename

    def _start_eeagent(self, name, process_dispatcher, exe_name="eeagent"):
        """Starts an eeagent with SupervisorD

        @param name: Name of process dispatcher to start
        @param process_dispatcher: The name of the parent Process Dispatcher to
                connect to
        @param exe_name: the name of the eeagent executable
        """

        config_file = self._build_eeagent_config(self.exchange, name, process_dispatcher)
        cmd = "%s %s" % (exe_name, config_file)
        pid = self.factory.get_pidantic(command=cmd, process_name=name,
                directory=self.pidantic_dir)
        pid.start()

    def _build_eeagent_config(self, exchange, name, process_dispatcher, log_file=None):
        """Builds a yaml config file to feed to the eeagent

        @param exchange: the AMQP exchange the service should be on
        @param name: name of the eeagent, used as the topic to be addressed
                on AMQP
        @param process_dispatcher: the name of the parent Process Dispatcher to 
                connect to          
        @param log_file: the log file for the eeagent
        """
        if not log_file:
            log_file="/tmp/pd.log"

        config = {
          'dashi': {
            'exchange': exchange,
          },
          'eeagent': {
            'name': name,
          },
          'pd': {
            'name': process_dispatcher,
          },
          'logging': {
            'loggers': {
              'eeagent': {
                'handlers': ['file', 'console']
              }
            },
            'handlers': {
              'file': {
                'filename': log_file,
              }
            }
          }
        }

        config_yaml = yaml.dump(config)

        (os_handle, config_filename) = tempfile.mkstemp(suffix='.yml')
        os.write(os_handle, config_yaml)
        os.close(os_handle)

        return config_filename


    def announce_node(self, node_name, deployable_type):
        """Announce a node to each process dispatcher.
        TODO: This should only announce to one PD

        @param node_name: the name of the node to advertise
        @param deployable_type: the deployable type of the node
        """
        state = InstanceState.RUNNING

        for pd_name, pd in self.process_dispatchers.iteritems():
            pd_client = ProcessDispatcherClient(self.dashi, pd_name)
            log.debug("Announcing %s of type %s is '%s' to %s" % (node_name, deployable_type, state, pd_name))
            
            for i in range(0, ADVERTISE_RETRIES):
                try:
                    pd_client.dt_state(node_name, deployable_type, state)
                    break
                except timeout:
                    wait_time = i*i # Exponentially increasing wait
                    log.warning("PD not available yet. Waiting %ss" % wait_time)
                    time.sleep(i*i)
