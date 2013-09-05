import os
import tempfile
from socket import timeout
import logging

from epu.dashiproc.processdispatcher import ProcessDispatcherClient
from epu.dashiproc.dtrs import DTRSClient
from epu.dashiproc.provisioner import ProvisionerClient
from epu.dashiproc.epumanagement import EPUManagementClient
from eeagent.client import EEAgentClient

from epuharness.deployment import parse_deployment
from epuharness.harness import EPUHarness

log = logging.getLogger(__name__)


class TestFixture(object):
    """A mixin to provide some helper methods to test classes
    """
    epuharness = None
    libcloud_drivers = None
    dashi = None

    def setup_harness(self, *args, **kwargs):

        self.epuharness = EPUHarness(*args, **kwargs)
        if os.path.exists(self.epuharness.pidantic_dir):
            raise Exception("EPUHarness running. Can't run this test")

        self.dashi = self.epuharness.dashi

    def teardown_harness(self, remove_dir=True):
        if self.epuharness:
            try:
                self.epuharness.stop(remove_dir=remove_dir)
            except Exception:
                log.exception("Error shutting down epuharness!")

        if self.libcloud_drivers:
            for site_name, driver in self.libcloud_drivers.iteritems():
                try:
                    driver.shutdown()
                except Exception:
                    log.exception("Error shutting down libcloud driver for site %s", site_name)

                try:
                    if driver.sqlite_db:
                        os.remove(driver.sqlite_db)
                except Exception:
                    log.exception("Error removing fake libcloud db: %s", driver.sqlite_db)

    cleanup_harness = teardown_harness

    def get_clients(self, deployment_str, dashi):
        """returns a dictionary of epu clients, indexed by their topic name
        """

        deployment = parse_deployment(yaml_str=deployment_str)

        clients = {}

        for provisioner_name in deployment.get('provisioners', {}).iterkeys():
            client = ProvisionerClient(dashi, topic=provisioner_name)
            clients[provisioner_name] = client

        for epum_name in deployment.get('epums', {}).iterkeys():
            client = EPUManagementClient(dashi, epum_name)
            clients[epum_name] = client

        for node in deployment.get('nodes', {}).itervalues():
            for eeagent_name in node.get('eeagents', {}).iterkeys():
                client = EEAgentClient(dashi=dashi, ee_name=eeagent_name,
                        handle_heartbeat=False)
                clients[eeagent_name] = client

        for pd_name in deployment.get('process-dispatchers', {}).iterkeys():
            client = ProcessDispatcherClient(dashi, pd_name)
            clients[pd_name] = client

        for dt_name in deployment.get('dt_registries', {}).iterkeys():
            client = DTRSClient(dashi, topic=dt_name)
            clients[dt_name] = client

        return clients

    def block_until_ready(self, deployment_str, dashi):
        """Blocks until all of the services in a deployment are contacted
        """

        deployment = parse_deployment(yaml_str=deployment_str)

        for provisioner_name in deployment.get('provisioners', {}).iterkeys():
            provisioner = ProvisionerClient(dashi, topic=provisioner_name)
            self._block_on_call(provisioner.describe_nodes)

        for epum_name in deployment.get('epums', {}).iterkeys():
            epum = EPUManagementClient(dashi, epum_name)
            self._block_on_call(epum.list_domains)

        for node in deployment.get('nodes', {}).itervalues():
            for eeagent_name in node.get('eeagents', {}).iterkeys():
                eeagent = EEAgentClient(dashi=dashi, ee_name=eeagent_name,
                        handle_heartbeat=False)
                self._block_on_call(eeagent.dump, kwargs={'rpc': True})

        for pd_name in deployment.get('process-dispatchers', {}).iterkeys():
            pd = ProcessDispatcherClient(dashi, pd_name)
            self._block_on_call(pd.describe_processes)

        for dt_name in deployment.get('dt_registries', {}).iterkeys():
            dtrs = DTRSClient(dashi, topic=dt_name)
            self._block_on_call(dtrs.list_sites)

    def _block_on_call(self, fn_to_block_on, attempts=None, kwargs={}):
        if not attempts:
            attempts = 10

        print "Block on %s" % fn_to_block_on.__name__
        for i in range(0, attempts):
            try:
                fn_to_block_on(**kwargs)
                break
            except timeout:
                continue
            except:
                #call worked, but got some mystery error
                raise
        else:
            try:
                msg = "Wasn't able to call %s.%s" % (
                    fn_to_block_on.im_class.__name__,
                    fn_to_block_on.__name__)
            except AttributeError:  # Not a member of a class
                msg = "Wasn't able to call %s.%s" % fn_to_block_on.__name__
            assert False, msg

    def make_fake_libcloud_site(self, site_name="ec2-fake", needs_elastic_ip=None):
        """makes a fake libcloud site and driver.

        Returns tuple of the site definition, and a libcloud driver instance
        """
        if self.libcloud_drivers is None:
            self.libcloud_drivers = {}

        if needs_elastic_ip is None:
            needs_elastic_ip = False

        if site_name in self.libcloud_drivers:
            driver = self.libcloud_drivers[site_name]

        else:
            fh, fake_libcloud_db = tempfile.mkstemp(prefix="fakelibcloud_db")
            os.close(fh)

            from epu.mocklibcloud import MockEC2NodeDriver
            driver = MockEC2NodeDriver(sqlite_db=fake_libcloud_db)
            self.libcloud_drivers[site_name] = driver

        fake_site = {
            'type': 'mock-ec2',
            'sqlite_db': driver.sqlite_db,
            'needs_elastic_ip': needs_elastic_ip
        }

        return fake_site, driver
