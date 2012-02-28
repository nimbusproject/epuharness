from nose.plugins.skip import SkipTest
from epuharness.harness import EPUHarness

class TestEPUHarness(object):

    def setup(self):

        self.epuharness = EPUHarness()

    def test_start_pd(self):

        pd_name = "testpd"
        engines = {}

        assert not self.epuharness.factory.reload_instances()

        self.epuharness._start_process_dispatcher(pd_name, engines, exe_name="echo")
        assert len(self.epuharness.factory.reload_instances()) == 1

    def test_start_eeagent(self):

        ee_name = "testeeagent"
        pd_name = "testpd"
        launch_type = "fork"

        assert not self.epuharness.factory.reload_instances()

        self.epuharness._start_eeagent(ee_name, pd_name, launch_type, exe_name="echo")
        assert len(self.epuharness.factory.reload_instances()) == 1

    def test_announce_node(self):

        raise SkipTest("TODO")

        got_announce = False
        def dt_state(self):
            pass
        dashi = self.epuharness.dashi
        raise Exception("TODO")

    def teardown(self):
        instances = self.epuharness.factory.reload_instances()
        for instance in instances.values():
            instance.cleanup()
        self.epuharness.factory.terminate()
