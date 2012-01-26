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
from exceptions import DeploymentDescriptionError

log = logging.getLogger(__name__)
ADVERTISE_RETRIES = 10

class EPUHarness(object):
    """EPUHarness. Sets up Process Dispatchers and EEAgents for testing.
    """
    #TODO: add framework for pyon messaging

    def __init__(self, exchange=None, pidantic_dir=None):

        configs = ["epuharness"]
        config_files = get_config_paths(configs)
        self.CFG = bootstrap.configure(config_files)

        self.pidantic_dir = pidantic_dir or self.CFG.epuharness.pidantic_dir
        self.exchange = exchange or self.CFG.server.amqp.get('exchange', None) or str(uuid.uuid4())
        self.CFG.server.amqp.exchange = self.exchange
        self.dashi = bootstrap.dashi_connect(self.CFG.dashi.topic, self.CFG)
        self.process_dispatchers = []

        try:
            os.makedirs(self.pidantic_dir)
        except OSError:
            pass

        self.factory = SupDPidanticFactory(directory=self.pidantic_dir,
                name="epu-harness")

    def stop(self, force=False):
        """Stop services that were previously started by epuharness

        @param force: When False raises an exception when there is something
                      that can't be killed.
        """
        instances = self.factory.reload_instances()
        if instances:
            log.info("Stopping %s" % ", ".join(instances.keys()))
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
            self._start_process_dispatcher(pd_name, pd.get('engines', {}), 
                logfile=pd.get('logfile'))

        nodes = deployment.get('nodes', {})
        for node_name, node in nodes.iteritems():

            if not node.has_key('process-dispatcher'):
                msg = "No process-dispatcher specified for node '%s'" % (
                    node_name)
                raise DeploymentDescriptionError(msg)

            self.announce_node(node_name, node.get('dt', ''),
                    node['process-dispatcher'])

            for eeagent_name, eeagent in node.get('eeagents', {}).iteritems():
                dispatcher = eeagent.get('process-dispatcher') or \
                    node.get('process-dispatcher', '')
                self._start_eeagent(eeagent_name, dispatcher, eeagent.get('logfile'))

        
    def _start_process_dispatcher(self, name, engines, logfile=None,
            exe_name="epu-processdispatcher-service"):
        """Starts a process dispatcher with SupervisorD

        @param name: Name of process dispatcher to start
        @param engines: a dictionary of eeagent configs. Same format as the 
                Process Dispatcher config file
        @param exe_name: the name of the process dispatcher executable
        """

        log.info("Starting Process Dispatcher '%s'" % name)

        config_file = self._build_process_dispatcher_config(self.exchange,
                name, engines, logfile=logfile)

        cmd = "%s %s" % (exe_name, config_file)
        log.debug("Running command '%s'" % cmd)
        pid = self.factory.get_pidantic(command=cmd, process_name=name,
                directory=self.pidantic_dir)
        pid.start()


    def _build_process_dispatcher_config(self, exchange, name, engines,
            logfile=None):
        """Builds a yaml config file to feed to the process dispatcher

        @param exchange: the AMQP exchange the service should be on
        @param name: name of the process dispatcher, used as the topic to be
                addressed on AMQP
        @param engines: a dictionary of eeagent configs. Same format as the     
                Process Dispatcher config file
        @param logfile: the log file for the Process Dispatcher
        """
        if not logfile:
            logfile = "/dev/null"

        config = {
          'server': {
            'amqp': {
              'exchange': exchange,
            }
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
                'filename': logfile,
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

    def _start_eeagent(self, name, process_dispatcher, logfile=None,
            exe_name="eeagent"):
        """Starts an eeagent with SupervisorD

        @param name: Name of process dispatcher to start
        @param process_dispatcher: The name of the parent Process Dispatcher to
                connect to
        @param logfile: the log file for the eeagent
        @param exe_name: the name of the eeagent executable
        """
        log.info("Starting EEAgent '%s'" % name)

        config_file = self._build_eeagent_config(self.exchange, name,
                process_dispatcher, logfile=logfile)
        cmd = "%s %s" % (exe_name, config_file)
        pid = self.factory.get_pidantic(command=cmd, process_name=name,
                directory=self.pidantic_dir)
        pid.start()

    def _build_eeagent_config(self, exchange, name, process_dispatcher, logfile=None):
        """Builds a yaml config file to feed to the eeagent

        @param exchange: the AMQP exchange the service should be on
        @param name: name of the eeagent, used as the topic to be addressed
                on AMQP
        @param process_dispatcher: the name of the parent Process Dispatcher to 
                connect to          
        @param logfile: the log file for the eeagent
        """
        if not logfile:
            logfile="/dev/null"

        config = {
          'server': {
            'amqp': {
              'exchange': exchange,
            }
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
                'filename': logfile,
              }
            }
          }
        }

        config_yaml = yaml.dump(config)

        (os_handle, config_filename) = tempfile.mkstemp(suffix='.yml')
        os.write(os_handle, config_yaml)
        os.close(os_handle)

        return config_filename


    def announce_node(self, node_name, deployable_type, process_dispatcher,
            state=None):
        """Announce a node to each process dispatcher.

        @param node_name: the name of the node to advertise
        @param deployable_type: the deployable type of the node
        @param process_dispatcher: the pd to announce to
        @param state: the state to advertise to the pd
        """
        if not state:
          state = InstanceState.RUNNING

        pd_client = ProcessDispatcherClient(self.dashi, process_dispatcher)
        log.info("Announcing %s of type %s is '%s' to %s" % (node_name,
            deployable_type, state, process_dispatcher))
            
        for i in range(1, ADVERTISE_RETRIES):
            try:
                pd_client.dt_state(node_name, deployable_type, state)
                break
            except timeout:
                wait_time = i*i # Exponentially increasing wait
                log.warning("PD '%s' not available yet. Waiting %ss" % (process_dispatcher, wait_time))
                time.sleep(2**i)
