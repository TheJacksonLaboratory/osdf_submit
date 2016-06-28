#!/usr/bin/env python
""" load Sample into OSDF using info from data file """

import os
import re

from cutlass.Sample import Sample

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, \
        list_tags, format_query, \
        values_to_node_dict, write_out_csv, get_field_header, \
        log_it, dump_args

filename=os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = 'Sample'
parent_type        = 'Visit'
grand_parent_type  = 'Subject'
great_parent_type  = 'Study'
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


def generate_mixs(row):
    DEGREE_SIGN = u"\N{DEGREE SIGN}"
    # DEGREE = DEGREE_SIGN.encode("iso-8859-1")
    DEGREE = DEGREE_SIGN.encode("UTF-8")
    # DEGREE = DEGREE_SIGN.encode("cp850")
    # DEGREE = DEGREE_SIGN.encode("macroman")
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
            'lat_lon': '37.441883, -122.143019',
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


def load(internal_id, search_field):
    """search for existing node to update, else create new"""

    # node-specific variables:
    NodeTypeName = Sample
    NodeLoadFunc = NodeTypeName.load_sample

    try:
        query = format_query(internal_id, '[-\.]', field=search_field)
        results = NodeTypeName.search(query)
        for node in results:
            if internal_id in getattr(node, search_field):
                return NodeLoadFunc(node)
        # no match, return new, empty node:
        node = NodeTypeName()
        return node
    except Exception, e:
        raise e


def validate_record(parent_id, node, record, data_file_name=node_type):
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
                'jaxid (dna): '+record['jaxid_recd_dna'] \
                             if record['jaxid_recd_dna'] \
                             else 'jaxid (dna): none',
                'jaxid (tissue): '+record['jaxid_recd_tissue'] \
                                if record['jaxid_recd_tissue'] \
                                else 'jaxid (tissue): none',
                # 'visit id: '+record['visit_id'],
                'visit id: '+record['sample_name'], #hack: identical variables!
                'subject id: '+record['rand_subject_id'],
                'study: '+record['study'],
                'sub_study: '+record['sub_study'],
                'visit type: '+record['visit_type']
                # 'material received: '+record['material_received'],
                )
    # node._attribs = record['attributes']

    log.debug('parent_id: '+str(parent_id))
    node.links = {'collected_during':[parent_id]}

    csv_fieldnames = get_field_header(data_file_name)
    if not node.is_valid():
        write_out_csv(data_file_name+'_invalid_records.csv',
            fieldnames=csv_fieldnames,values=[record,])
        invalidities = node.validate()
        err_str = "Invalid {}!\n\t{}".format(node_type, str(invalidities))
        log.error(err_str)
        # raise Exception(err_str)
    elif node.save():
        write_out_csv(data_file_name+'_submitted.csv',
                      fieldnames=record.keys(),values=[record,])
        return node
    else:
        write_out_csv(data_file_name+'_unsaved_records.csv',
                      fieldnames=csv_fieldnames,values=[record,])
        return False


def write_csv_headers(base_filename='data_file', fieldname_list=[]):
    """init other csv files (invalid, unsaved, etc) with fieldname headers"""
    # TODO:  ?? use DictWriter.writeheader() instead ??
    err_file_appends = ['_unsaved_records.csv',
                        '_invalid_records.csv',
                        '_submitted.csv']
    [ write_out_csv(
        base_filename+suff,
        fieldnames=fieldname_list,
        values=[fieldname_list,])
        for suff in err_file_appends
        if not os.path.exists(base_filename+suff) ]


def submit(data_file, id_tracking_file=node_tracking_file):
    log.info('Starting submission of %ss.', node_type)
    nodes = []
    for record in load_data(data_file):
        # write_csv_headers(data_file, record.keys()) # done by hand
        log.debug('...next record...')
        try:
            log.debug('data record: '+str(record))

            # Node-Specific Variables:
            load_search_field = 'name'
            internal_id = record['sample_name']
            parent_internal_id = record['sample_name'] # sample id = visit id
            grand_parent_internal_id = record['rand_subject_id']

            parent_id = get_parent_node_id(
                id_tracking_file, parent_type, parent_internal_id)

            node = load(internal_id, load_search_field)
            # Loading NEW sample records, due to non-searchability via OQL to ES
            # node = Sample()
            if not getattr(node, load_search_field):
                log.debug('loaded node newbie...')
            saved = validate_record(parent_id, node, record,
                                    data_file_name=data_file)
            if saved:
                header = settings.node_id_tracking.id_fields
                saved_name = getattr(saved, load_search_field)
                vals = values_to_node_dict(
                    [[node_type.lower(),saved_name,saved.id,
                      parent_type.lower(),parent_internal_id,parent_id]],
                    header
                    )
                nodes.append(vals)
                write_out_csv(id_tracking_file,values=vals)
        except Exception, e:
            log.exception(e)
            raise e
    return nodes


if __name__ == '__main__':
    pass
