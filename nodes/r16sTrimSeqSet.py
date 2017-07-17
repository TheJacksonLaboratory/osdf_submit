#!/usr/bin/env python
""" load 16S DNA prep into OSDF using info from data file """

import os
import re

from cutlass.SixteenSTrimmedSeqSet import SixteenSTrimmedSeqSet

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, list_tags, format_query, \
        write_csv_headers, values_to_node_dict, write_out_csv, \
        load_node, get_field_header, dump_args, log_it, \
        get_cur_datetime

filename=os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = '16s_trimmed_seq_set'
parent_type        = '16s_raw_seq_set'
grand_parent_type  = '16s_dna_prep'
great_parent_type  = 'sample'
great_great1_type  = 'visit'
great_great2_type  = 'subject'
great_great3_type  = 'study'

node_tracking_file = settings.node_id_tracking.path


class node_values:
    checksums     = ''
    comment       = ''
    format        = ''
    format_doc    = ''
    local_file    = ''
    size          = ''
    study         = ''
    tags          = []


def load(internal_id, search_field='local_file'):
    """search for existing node to update, else create new"""

    # node-specific variables:
    NodeTypeName = 'SixteenSTrimmedSeqSet'
    NodeLoadFunc = 'load_sixteenSTrimmedSeqSet'

    return load_node(internal_id, search_field, NodeTypeName, NodeLoadFunc)


def validate_record(parent_id, node, record, data_file_name=node_type):
    """update record fields
       validate node
       if valid, save, if not, return false
    """
    csv_fieldnames = get_field_header(data_file_name)
    write_csv_headers(data_file_name,fieldnames=csv_fieldnames)

    local_file_name = os.path.basename(record['local_file_clean'])
    node.study         = 'prediabetes'
    node.comment       = local_file_name
    node.format        = record['format'] # only 'fasta', 'fastq' allowed!
    node.format_doc    = 'https://en.wikipedia.org/wiki/' +\
                         record['format'].upper() + '_format'
    if record['consented'] == 'YES':
        node.local_file = record['local_file_clean']
        node.checksums  = {'md5':record['clean_md5'], 'sha256':record['clean_sha256']}
        node.size       = int(record['clean_size'])
    else:
        node.private_files = True
        node.checksums     = {'md5': '00000000000000000000000000000000'}
        node.size          = 1
    node.tags = list_tags(
                          'study: prediabetes',
                          'subject id: '+record['rand_subject_id'],
                          'sample name: '+record['sample_name_id'],
                          'body site: '+record['body_site'],
                          'prep_id:' + record['prep_id'],
                          'raw_file_id: '+ record['raw_file_id'],
                         )

    log.debug('parent_id: '+str(parent_id))
    node.links = {'computed_from':[parent_id]}

    if not node.is_valid():
        invalidities = str(node.validate())
        err_str = "Invalid node {}!\t\t{}".format(node_type, invalidities)
        log.error(err_str)
        # vals = [record]
        # vals.append(invalidities)
        write_out_csv(data_file_name+'_invalid_records.csv',
                fieldnames=csv_fieldnames,
                values=[record,])
        return False
    elif node.save():
        log.info('node saved: '+str(node.comment))
        write_out_csv(data_file_name+'_submitted.csv',
                fieldnames=csv_fieldnames, values=[record,])
        return node
    else:
        log.info('node NOT saved: '+str(node.comment))
        write_out_csv(data_file_name+'_unsaved_records.csv',
                fieldnames=csv_fieldnames, values=[record,])
        return False


def submit(data_file, id_tracking_file=node_tracking_file):
    log.info('Starting submission of %ss.', node_type)
    nodes = []
    csv_fieldnames = get_field_header(data_file)
    write_csv_headers(data_file,fieldnames=csv_fieldnames)
    for record in load_data(data_file):
        log.info('\n...next record...')
        try:
            log.debug('data record: '+str(record))

            # node-specific variables:
            if record['local_file_clean'] != '':
                load_search_field = 'local_file'
                internal_id = os.path.basename(record['local_file_clean'])
                parent_internal_id = record['raw_file_id']

                parent_id = get_parent_node_id(
                    id_tracking_file, parent_type, parent_internal_id)
                log.debug('matched parent_id: %s', parent_id)

            if parent_id:
                node_is_new = False # set to True if newbie
                node = load(internal_id, load_search_field)
                if not getattr(node, load_search_field):
                    log.debug('loaded node newbie...')
                    node_is_new = True

                saved = validate_record(parent_id, node, record,
                        data_file_name=data_file)
                if saved:
                    header = settings.node_id_tracking.id_fields
                    if record['consented'] == 'YES':
                        saved_name = os.path.basename(getattr(saved, load_search_field))
                    else:
                       saved_name = '-'.join([getattr(saved, 'comment'), 'private_file'])
                    vals = values_to_node_dict(
                        [[node_type.lower(), saved_name, saved.id,
                          parent_type.lower(), parent_internal_id, parent_id,
                          get_cur_datetime()]],
                        header
                        )
                    nodes.append(vals)
                    if node_is_new:
                        write_out_csv(id_tracking_file,
                              fieldnames=get_field_header(id_tracking_file),
                              values=vals)
            else:
                log.error('No parent_id found for %s', parent_internal_id)

        except Exception, e:
            log.exception(e)
            raise e
    return nodes


if __name__ == '__main__':
    pass
