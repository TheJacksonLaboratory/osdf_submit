import os
import sys
import csv
import re
import logging

# Log It!
def log_it(filename=os.path.basename(__file__)):
    """log_it setup"""
    logging.basicConfig(level=logging.DEBUG,
        format="%(asctime)s %(levelname)5s %(funcName)20s: %(message)s")
    return logging.getLogger(filename)
log = log_it()

import cutlass

import settings

id_fields = settings.node_id_tracking.id_fields
# sample: subject,69-01,610a491c,study,prediabetes,610a4911a5c


# generator of rows in csv file
def load_data(csv_file):
    log.info('Loading rows from {}'.format(csv_file))
    with open(csv_file) as csvfh:
        reader = csv.DictReader(csvfh)
        log.debug('csv dictreader opened')
        try:
            for row in reader:
                # log.debug(row)
                yield row
        except csv.Error as e:
            sys.exit('CSV file %s, line %d: %s'.format(\
                    csv_file, reader.line_num, e))


def write_out_csv(csv_file,fieldnames=id_fields,values=[]):
    """ write all values in csv format to outfile """
    log.debug('writing out...')
    if values[0] is not None:
        try:
            with open(csv_file, 'a') as csvout:
                writer = csv.DictWriter(csvout,fieldnames)
                try:
                    # log.debug(values)
                    for row in values:
                        if isinstance(row,dict):
                            log.debug(row)
                            writer.writerow(row)
                except Exception, e:
                    sys.exit('CSV file %s, %s'.format(
                            csv_file, str(e)))
        except IOError, e:
            raise e


id_keys = settings.node_id_tracking.id_fields
from collections import OrderedDict
def values_to_node_dict(values=[],keynames=id_keys):
    """pass list of lists of values and list of keys of desired dict
       This converts to list of dicts
    """
    log.info('In values_to_node_dict')
    final_list = []

    key_dict = OrderedDict()
    for key in keynames:
        key_dict[key] = ''

    for vals in values:
        l = vals
        d = key_dict.copy()
        k = d.keys()
        for x in range(len(d)):
            lx = l[x] if len(l) > x and l[x] is not None else ''
            d[k[x]] = lx
        # log.debug(d)
        final_list.append(d)

    return final_list

# values_to_node_dict([['foo','bar','1'],['spam','eggs','2','spoof','filled']])


def get_parent_node_id(id_file_name, node_type, parent_id):
    """ read node ids from csv tracking file
        return first "parent" node matching node_type
    """
    log.debug('--> args: '+ id_file_name +','+ node_type +','+ parent_id)
    try:
        for row in yield_csv_rows(id_file_name):
            if re.match(node_type,row['node_type']):
                # node_ids.append(row.parent_id)
                if re.match(parent_id,row['internal_id']):
                    log.debug('--> matching node row: '+ str(row))
                    log.debug('parent type: {}, osdf_node_id: {}'.format(
                        node_type,str(row['osdf_node_id'])))
                    return row['osdf_node_id']
    except Exception, e:
        raise e


def format_query(strng, patt='-', field='rand_subj_id', mode='&&'):
    """format OQL query by replacing control character e.g. '-'
       Split 'strng' on 'patt'; append 'field' text; concat using mode
    """
    strgs = re.split(patt,strng)
    if len(strgs) > 1:
        strngs = ['"{s}"[{field}] ".format(s,field) for s in strngs]
        #TODO: does the JSON query require spaces between terms?? e.g.  ' && '
        strng = '{mode}'.join(strngs)
    return strng


def list_tags(node_tags, *tags):
    """generate list of all tags to be used in add_tag method"""
    end_tags = []
    [end_tags.append(t) for t in node_tags]
    [end_tags.append(t) for t in tags]
    return end_tags


class osdf_gensc_required_dicts():
    mimarks = dict(cutlass.mimarks.MIMARKS._fields)
    mixs = dict(cutlass.mixs.MIXS._fields)
    mims = dict(cutlass.mims.MIMS._fields)





# All functions below no longer used!
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
        log.debug("node info:", node)

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



if __name__ == '__main__':
    pass
