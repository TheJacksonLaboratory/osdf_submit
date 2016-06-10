#!/usr/bin/env python
""" load Sample into OSDF using info from data file """

import os
import re

from cutlass.Sample import Sample

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, \
        list_tags, format_query, \
        values_to_node_dict, write_out_csv, \
        log_it, dump_args

filename=os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = 'sample'
parent_type        = 'visit'
grand_parent_type  = 'subject'
great_parent_type  = 'study'
node_tracking_file = settings.node_id_tracking.path

class node_values:
    name = ''  # sample_name
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

# @dump_args
def generate_mixs(row):
    try:
        mixs = {
            'biome': 'terrestrial biome [ENVO:00000446]',
            'body_product': row['body_site'],
            'collection_date': ('2112-12-21'), #not allowed by IRB!
            'env_package': 'human-gut' \
                    if re.match('stool',row['body_site']) \
                    else 'human-associated',
            'feature': 'N/A',
            'geo_loc_name': 'Palo Alto, CA, USA',
            'lat_lon': 'N 37'+DEGREE+' 26\' 30.78" W 122'+DEGREE+' 8\' 34.87"',
            'material': 'feces(ENVO:00002003)' \
                    if re.match('stool',row['body_site']) \
                    else 'oronasal secretion(ENVO:02000035)',
            'project_name': 'iHMP',
            'rel_to_oxygen': 'N/A',
            'samp_collect_device': 'self-sample' \
                    if re.match('stool',row['body_site']) \
                    else 'self-swab',
            'samp_mat_process': 'N/A',
            'samp_size': 'N/A',
            'source_mat_id': [],
            }
        return mixs
    except Exception as e:
        log.warn('Conversion to MIXS format failed?! (SampleName: {}).\n'
            '    Exception message:{}'.format(row['sample_name'], e.message))

@dump_args
def load(internal_id,parent_id,grand_parent_id):
    """search for existing node to update, else create new"""
    try:
        # query = format_query(internal_id, field='name')
        # query += '&&"'+format_query(parent_id, field='tags')
        # query += '&&"'+format_query(grand_parent_id, field='tags')
        # query = "1636_123_123_123[tags]"  # search with '-' still not functional! (ticket CUTLASS-003)
        id_bits = re.split('-',internal_id)[1:]
        query = ' '.join(id_bits)
        s = Sample.search(query)
        for n in s:
            if parent_id in n.tags:
                return Sample.load_sample(n)
        # no match, return empty node:
        n = Sample()
        return n
    except Exception, e:
        raise e

@dump_args
def validate_record(parent_id, node, record):
    """update record fields
       validate node
       if valid, save, if not, return false
    """
    node.name = record['sample_name']
    node.body_site = record['body_site']
    node.fma_body_site = record['fma_body_site']
    node.mixs = generate_mixs(record)
    node.tags = list_tags(node.tags,
                'test', # for debug!!
                'interval: '+record['collection_date'],
                'jaxid: "'+record['jaxid_recd_dna']+'"' \
                        if record['jaxid_recd_dna']\
                        else 'jaxid: '+record['jaxid_recd_tissue']\
                                    if record['jaxid_recd_tissue']\
                                    else 'jaxid: pending',
                # 'visit id: '+record['visit_id'],
                'visit id: '+record['sample_name'],
                'subject id: '+record['rand_subject_id'],
                'study: '+record['study'],
                'sub_study: '+record['sub_study'],
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

# @dump_args
def submit(data_file, id_tracking_file=node_tracking_file):
    log.info('Starting submission of Samples.')
    nodes = []
    for record in load_data(data_file):
        log.debug('...next record...')
        try:
            log.debug('data record: '+str(record))
            parent_id = get_parent_node_id(
                    id_tracking_file, parent_type, record['sample_name'])
                                                  #record['visit_id'])
            n = load(record['sample_name'],
                 record['sample_name'], #convenient identity of name/id...
                 # record['visit_id'],
                 record['rand_subject_id'])
            # n = load(record['sample_name'],parent_id,grand_parent_id)
            if not n.name:
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
                else:
                    write_out_csv(id_tracking_file+'_unsaved_records.csv',
                        fieldnames=record.keys(),values=[record,])
        except Exception, e:
            log.error(e)
            raise e
    return nodes


if __name__ == '__main__':
    pass
