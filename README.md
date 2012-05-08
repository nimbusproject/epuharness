EPU Harness
===========

A tool to deploy EPU locally for development.

Refer to https://confluence.oceanobservatories.org/display/CIDev/Lightweight+CEI+Launch

Usage
-----

By default, epu-harness will start one Process Dispatcher and one eeagent, and
create a configuration for each that had the eeagent announce itself to the PD.

If you would like a different deployment, you can create your own deployment
configuration. The default configuration is as follows:

    process-dispatchers:
      pd_0:
        logfile: /tmp/pd_0.log
        engines:
          default:
            deployable_type: eeagent
            slots: 4
            base_need: 1
    nodes:
      nodeone:
        dt: eeagent
        process-dispatcher: pd_0
        eeagents:
          eeagent_nodeone:
            logfile: /tmp/eeagent_nodeone.log

If you want two nodes, for example, your configuration file would look like:

    process-dispatchers:
      pd_0:
        logfile: /tmp/pd_0.log
        engines:
          default:
            deployable_type: eeagent
            slots: 4
            base_need: 1
    nodes:
      nodeone:
        dt: eeagent
        process-dispatcher: pd_0
        eeagents:
          eeagent_nodeone:
            logfile: /tmp/eeagent_nodeone.log
      nodetwo:
        dt: eeagent
        process-dispatcher: pd_0
        eeagents:
          eeagent_nodetwo:
            logfile: /tmp/eeagent_nodetwo.log


To use the profile, save it to a yml file, and launch it like so:

    $ epu-harness start twonodes.yml

When you're ready to stop the service, you can do so like so:

    $ epu-harness stop

Installation
------------

For deployment:

    pip install -r requirements.txt

For development of epu-harness itself:

    python setup.py develop
