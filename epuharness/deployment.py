import yaml

from exceptions import *

DEFAULT_DEPLOYMENT = """---
process-dispatchers:
  pd_0:
    engines:
      default:
        deployable_type: eeagent
        slots: 4
        base_need: 1
nodes:
  nodeone:
    dt: eeagent
    eeagents:
      - name: eeagent_nodeone
        process-dispatcher: pd_0
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
