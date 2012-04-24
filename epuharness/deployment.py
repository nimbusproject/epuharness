import yaml

from exceptions import *

DEFAULT_DEPLOYMENT = """---
process-dispatchers:
  pd_0:
    config:
      processdispatcher:
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
        launch_type: supd
        logfile: /tmp/eeagent_nodeone.log
provisioners:
  provisioner_0:
    config:
      provisioner:
        default_user: default
        dtrs: epu.localdtrs.LocalDTRS
epums:
  epum_0:
    config:
      epumanagement:
        default_user: default
        provisioner_topic: provisioner_0
      logging:
        handlers:
          file:
            filename: /tmp/epum_0.log
"""


def parse_deployment(yaml_path=None, yaml_str=None):
    if yaml_path and yaml_str:
        ProgrammingError("Cannot handle both a yaml file and a yaml string")
    elif not yaml_path and not yaml_str:
        msg = "Please provide a path to a yaml file or a yaml string to parse"
        ProgrammingError(msg)

    parsed_yaml = None
    if yaml_path:
        with open(yaml_path) as yaml_file:
            parsed_yaml = yaml.load(yaml_file)
    else:
        parsed_yaml = yaml.load(yaml_str)

    return parsed_yaml
