#!/usr/bin/env python
""" load Sample into OSDF using info from data file """

import os

from cutlass.Sample import Sample

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, \
        list_tags, format_query, \
        values_to_node_dict, write_out_csv, \
        log_it

filename=os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type = 'sample'
parent_type = 'visit'
grand_parent_type = 'subject'
great_parent_type = 'study'
node_tracking_file = settings.node_id_tracking.path

class node_values:
    name = ''  # sample_id
    body_site = ''
    fma_body_site = ''
    mixs = {}
    tags = []


class mixs_fields:
    biome = ''
    body_product = ''
    collection_date = ''
    env_package = ''
    feature = ''
    geo_loc_name = ''
    lat_lon = ''
    material = ''
    project_name = ''
    rel_to_oxygen = ''
    samp_collect_device = ''
    samp_mat_process = ''
    samp_size = ''
    source_mat_id = []


def check_mixs(*mixs):
    """validate incoming mixs dict
        check it against MIXS required fields and value types
    """
    #TODO: make check_mixs happen!`  [split on commas? text as raw text?]
    return mixs


def load(internal_id,parent_id,grand_parent_id):
    """search for existing node to update, else create new"""
    try:
        query = format_query(internal_id, field='name')
        query += '&&"'+format_query(parent_id, field='tags')
        query += '&&"'+format_query(grand_parent_id, field='tags')
        s = Sample.search(query)
        for n in s:
            if parent_id in n.tags:
                return Sample.load_visit(n)
        # no match, return empty node:
        n = Sample()
        return n
    except Exception, e:
        raise e


def validate_record(parent_id, node, record):
    """update record fields
       validate node
       if valid, save, if not, return false
    """
    log.info("in validate/save: "+node_type)

    node.name = record['sample_id']
    node.body_site = record['body_site']
    node.fma_body_site = record['fma_body_site']
    node.mixs = check_mixs(record['mixs'])
    node.tags = list_tags(node.tags,
                'test', # for debug!!
                'interval: '+record['collection_date'],
                'jaxid: "'+record['jaxid']+'"',
                'visit id: '+record['visit_id'],
                'subject id: '+record['subject_id'],
                'study: '+record['study'],
                'sub_study: '+record['sub_study']
                'visit type: '+record['visit_type'],
                'material received: '+record['material_received'],
                )
    node.links = {'collected_during':parent_id}
    node._attribs = record['attributes']

    log.debug('parent_id: '+str(parent_id))
    node.links = {'collected_during':[parent_id]}
    if not node.is_valid():
        invalidities = node.validate()
        err_str = "Invalid {}!\n{}".format(node_type, "\n".join(invalidities))
        log.error(err_str)
        raise Exception(err_str)
    elif node.save():
        return node
    else:
        return False


def submit(data_file, id_tracking_file=node_tracking_file):
    log.info('Starting submission of Samples.')
    nodes = []
    for record in load_data(data_file):
        try:
            # log.debug('trying...')
            log.debug('data record: '+str(record))

            parent_id = get_parent_node_id(
                    id_tracking_file, parent_type, record['visit_id'])
            grant_parent_id = get_parent_node_id(
                    id_tracking_file, grant_parent_type, record['subject_id'])

            n = load(record['sample_id'],record['visit_id'],record['subject_id'])
            if not n.visit_number:
                log.debug('loaded node newbie...')
                saved = validate_record(parent_id, n, record)
                if saved:
                    header = settings.node_id_tracking.id_fields
                    vals = values_to_node_dict(
                            [[node_type,saved.visit_id,saved.id,
                              parent_type,record['visit_id'],parent_id]]
                            )
                    nodes.append(vals)
                    write_out_csv(id_tracking_file,values=vals)
        except Exception, e:
            log.error(e)
            raise e
    return nodes


if __name__ == '__main__':
    pass
