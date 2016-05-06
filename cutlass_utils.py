import os
import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(name=os.path.basename(__file__))

def save_if_valid_test2(data_dict):
    """Usage: save_if_valid( metadata_dict, NodeType(e.g.Project))"""
    nodename = 'name' #data_dict.__name__
    valid = True
    valid = data_dict.is_valid()
    # errors = data_dict.validate()
    # import pdb; pdb.set_trace()
    # if valid and (len(errors)==0):
    if valid:
        success = data_dict.save()
        if success:
            data_id = data_dict._id
            log.info("Succesfully saved {}. ID: {}".format(nodename,data_id))
            # TODO: print json data_dict to exernal log file
            # json_log_filename = "{}_{}".format(nodename,data_id)
            # node_json_to_file(data_dict, json_log_filename)
            return data_id
        else:
            log.info("Save failed")
            raise Exception
        return None
    else:
        log.info("Invalid...")
        validation_errors = data_dict.validate()
        log.info(validation_errors)
        raise Exception
    return None

def save_if_valid(data_dict, nodename=''):
    """Usage: save_if_valid( metadata_dict, NodeType(e.g.Project))"""
    # valid = data_dict.is_valid()
    errors = data_dict.validate()
    # if valid and
    if (len(errors)==0):
        success = data_dict.save()
        if success:
            data_id = data_dict._id
            log.info("Succesfully saved {}. ID: {}".format(nodename,data_id))
            return data_id
        else:
            log.info("Save failed. Errors: {}".format(errors))
            return None
    else:
        log.info("Data invalid! Errors: {}".format(errors))
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
def load_yaml_data(yaml_file):
    config = []
    log.info('Loading config from {}'.format(yaml_file))
    for config_set in yaml.load_all(open(yaml_file)):
        config.append(config_set)
    return config

import csv
def load_csv_rows_dict(csv_file):
    rows = []
    log.info('Loading rows from {}'.format(csv_file))
    with open(csv_file) as csvfh:
        reader = csv.DictReader(csvfh)
        try:
            for row in reader:
                rows.append(row)
        except csv.Error as e:
            sys.exit('CSV file %s, line %d: %s'.format(\
                    csv_file, reader.line_num, e))
        finally:
            return rows


import csv
import re
def match_records(records, match_re, match_field='internal_id'):
    rows = []
    log.info('matching records for {}'.format(csv_file))
    for row in records:
        if re.match(match_re, row[match_field]):
            rows.append(row)
    return rows


id_fields = ['node_type', 'internal_id', 'osdf_node_id',
             'parent_node_type', 'parent_id', 'parent_node_id']
def read_ids(id_file_name, node_type, internal_id):
    """ read node ids from csv file
        sample: subject,69-01-01,610a4911a5c,study,prediabetes,610a4911a5c
    """
    node_ids = []
    try:
        rows = load_csv_rows_dict(id_file_name)
        matches = match_records(rows, internal_id, node_type)
        for id in matches:
            pass #TODO: match!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    except Exception, e:
        raise e
    finally:
        return node_ids

import csv
import re
def write_out_csv(csv_file,fieldnames=id_fields,values=[]):
    """ write all values in csv format to outfile """
    try:
        with open(csv_file, 'a') as csvout:
            writer = csv.DictWriter(csvout,fieldnames)
            try:
                for rows in values:
                    writer.write(row)
            except Exception, e:
                sys.exit('CSV file %s, line %d: %s'.format(\
                        csv_file, writer.line_num, e))
    except IOError, e:
        raise e

class osdf_gensc_required_dicts():
    from cutlass.mimarks import MIMARKS
    mimarks = dict(MIMARKS._fields)

    from cutlass.mixs import MIXS
    mixs = dict(MIXS._fields)

    from cutlass.mims import MIMS
    mims = dict(MIMS._fields)


def submit_node(node_type, node_creation, node_info, child_text, parent_id):
    """ Load node into OSDF using info from file.
        Can be called from specific node-type functions with predetermined args
    """
    try:
        node = node_creation

        log.debug("Required fields: {}".format(node.required_fields()))
        # set node required fields empty as start point
        for f in node.required_fields():
            node.f = ''
        log.debug("Required fields all set empty.")
        #TODO: set empty required MIMS, MIMARKS, MIXS fields

        # load all other fields from data file
        node.update(node_info)
        # for f in node_info:
            # node.f = node_info[f]

        log.debug("All fields set from data file.")
        log.debug("--> node info:", node)

        # set linkage as passed in 'child_text'
        node.linkage = { child_text: [ parent_id ] }
        log.debug("Node 'linkage' set. ({})".format(child_txt))

        success = node.save()
        log.info('Node: {}, parent: {}, ID: {}'.format(
            node_type, parent_id, node._id))
        return node._id
    except Exception as e:
        log.warn(e)
        raise e
