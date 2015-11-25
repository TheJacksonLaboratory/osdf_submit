from pprint import pprint
def save_if_valid(data_dict, nodetype):
    """Usage: save_if_valid( metadata_dict, NodeType(e.g.Project))"""
    nodename = nodetype.__name__
    if data_dict.is_valid():
        print("Valid!")
        success = data_dict.save()

        if success:
            data_id = data_dict.id
            print("Succesfully saved {}. ID: {}".format(nodename,data_id))
            print(data_dict.to_json(indent=4))

            deletion_success = data_dict.delete()
            if deletion_success:
                print("Deleted {} with ID {}".format(nodename,data_id))
            else:
                print("Deletion of {} with ID {} failed.".format(
                    nodetype,data_id))
        else:
            print("Save failed")
    else:
        print("Invalid...")
        validation_errors = data_dict.validate()
        pprint(validation_errors)


import os
def load_string_from_file(filename):
    with open(os.path.join(os.curdir, filename)) as f:
        return f.read().strip()


import yaml
def load_config_from_file(yaml_file):
    config = []
    for config_set in yaml.load_all(open(yaml_file)):
        config.append(config_set)
    return config

