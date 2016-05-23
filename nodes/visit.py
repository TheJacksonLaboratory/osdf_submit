#!/usr/bin/env python
""" load Visit into OSDF using info from data file """

import os

from cutlass.Visit import Visit

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, \
        list_tags, format_query, \
        values_to_node_dict, write_out_csv, \
        log_it

filename=os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type = 'visit'
parent_type = 'subject'
grand_parent_type = 'study'
great_parent_type = 'project'
node_tracking_file = settings.node_id_tracking.path

class node_values:
    visit_id = ''
    visit_number = ''
    interval = ''
    clinic_id = ''
    tags = ['subject_id: ','sub_study: ','study: ']


def load(internal_id,parent_id):
    """search for existing node to update, else create new"""
    try:
        query = '"'+internal_id+'"[visit_id]'
        query += '&&"'+parent_id+'"[tags]'
        s = Visit.search(query)
        for n in s:
            if parent_id in n.tags:
                return Visit.load_visit(n)
    except Exception, e:
        n = Visit()
        return n


def validate_record(parent_id, node, record):
    """update record fields
       validate node
       if valid, save, if not, return false
    """
    log.info("in validate/save: "+node_type)
    node.visit_id = record['visit_id']
    node.visit_number = int(record['visit_number'])
    node.interval = int(record['collection_date'])
    # node.tags = [] # erases all pre-existing tags!!
    node.add_tag('test') # for debug!!
    node.add_tag('subject_id: '+record['subject_id'])
    node.add_tag('study: '+record['study'])
    node.add_tag('sub_study: '+record['sub_study'])
    log.debug('parent_id: '+str(parent_id))
    node.links = {'by':[parent_id]}
    if not node.is_valid():
        invalidities = node.validate()
        err_str = "Invalid!\n{}".format("\n".join(invalidities))
        log.error(err_str)
        raise Exception(err_str)
    elif node.save():
        return node
    else:
        return False


def submit(data_file, id_tracking_file=node_tracking_file):
    log.info('Starting submission of visits.')
    nodes = []
    for record in load_data(data_file):
        try:
            # log.debug('trying...')
            log.debug('record: '+str(record))

            parent_id = get_parent_node_id(
                    id_tracking_file, parent_type, record['subject_id'])

            n = load(record['visit_id'],record['subject_id'])
            if not n.visit_number:
                log.debug('loaded newbie...')
                saved = validate_record(parent_id, n, record)
                if saved:
                    header = settings.node_id_tracking.id_fields
                    vals = values_to_node_dict(
                            [[node_type,saved.visit_id,saved.id,
                              parent_type,record['subject_id'],parent_id]]
                            )
                    nodes.append(vals)
                    write_out_csv(id_tracking_file,values=vals)
        except Exception, e:
            log.error(e)
            raise e
    return nodes


if __name__ == '__main__':
    pass
