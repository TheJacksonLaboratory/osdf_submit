import os
import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(name=os.path.basename(__file__))

def save_if_valid(data_dict):
    """Usage: save_if_valid( metadata_dict, NodeType(e.g.Project))"""
    nodename = data_dict.__name__
    valid = data_dict.is_valid()
    errors = data_dict.validate()
    if valid and (len(errors)==0):
        success = data_dict.save()
        if success:
            data_id = data_dict.id
            log.info("Succesfully saved {}. ID: {}".format(nodename,data_id))
            # TODO: print json data_dict to exernal log file
            json_log_filename = "{}_{}".format(nodename,data_id)
            node_json_to_file(data_dict, json_log_filename)
            return data_id
        else:
            log.info("Save failed")
            return None
    else:
        log.info("Invalid...")
        validation_errors = data_dict.validate()
        log.info(validation_errors)
        return None

from pprint import pprint
def node_json_to_file(data_dict, filename):
    # pprint(data_dict.to_json(indent=4))
    pass

def delete_node(data_dict, nodetype):
    # TODO: test delete_node() function
    """Usage: delete_node( metadata_dict, NodeType(e.g.Project))"""
    nodename = nodetype.__name__
    if data_dict.is_valid():
        log.info("Valid!")
        success = data_dict.delete()

        if success:
            data_id = data_dict.id
            log.info("Deleted {} with ID {}".format(nodename,data_id))
            log.info(data_dict.to_json(indent=4))
            return data_id
        else:
            log.info("Deletion of {} with ID {} failed.".format(
                    nodetype,data_id))
            return None
    else:
        log.info("Invalid...")
        validation_errors = data_dict.validate()
        pprint(validation_errors)
        return None


import os
def load_string_from_file(filename):
    with open(os.path.join(os.curdir, filename)) as f:
        return f.read().strip()


import yaml
def load_config_from_file(yaml_file):
    config = []
    log.info('Loading config from {}'.format(yaml_file))
    for config_set in yaml.load_all(open(yaml_file)):
        config.append(config_set)
    return config

