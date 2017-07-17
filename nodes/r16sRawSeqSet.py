#!/usr/bin/env python
""" load 16S DNA prep into OSDF using info from data file """

import os
import re

from cutlass.SixteenSRawSeqSet import SixteenSRawSeqSet

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, list_tags, format_query, \
        write_csv_headers, values_to_node_dict, write_out_csv, \
        load_node, get_field_header, dump_args, log_it, \
        get_cur_datetime

filename = os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = '16s_raw_seq_set'
parent_type        = '16s_dna_prep'
grand_parent_type  = 'Sample'
great_parent_type  = 'Visit'
great_great1_type  = 'Subject'
great_great2_type  = 'Study'

node_tracking_file = settings.node_id_tracking.path

TAG_pri_D5 = ['AATGATACGGCGACCACCGAGATCTACAC',
              'ACACTCTTTCCCTACACGACGCTCTTCCGATCT']
PCR_fwd_D5 = 'AGAGTTTGATCCTGGCTCAG'
TAG_pri_D7 = ['GATCGGAAGAGCACACGTCTGAACTCCAGTCAC',
              'ATCTCGTATGCCGTCTTCTGCTTG']
PCR_rev_D7 = 'ATTACCGCGGCTGCTGG'

TAG_pri_A5 = [ ]
PCR_fwd_A5 = 'AGAGTTTGATCCTGGCTCAG'
TAG_pri_A7 = [ ]
PCR_rev_A7 = 'ATTACCGCGGCTGCTGG'


class node_values:
    study         = ''
    comment       = ''
    sequence_type = ''
    seq_model     = ''
    format        = ''
    format_doc    = ''
    exp_length    = ''
    local_file    = ''
    checksums     = ''
    size          = ''
    study         = ''
    urls          = []
    tags          = []


def load(internal_id, search_field):
    """search for existing node to update, else create new"""

    # node-specific variables:
    NodeTypeName = 'SixteenSRawSeqSet'
    NodeLoadFunc = 'load_16s_raw_seq_set'

    return load_node(internal_id, search_field, NodeTypeName, NodeLoadFunc)


def validate_record(parent_id, node, record, data_file_name=node_type):
    """update record fields
       validate node
       if valid, save, if not, return false
    """
    log.info("in validate/save: "+node_type)
    csv_fieldnames = get_field_header(data_file_name)
    write_csv_headers(data_file_name,fieldnames=csv_fieldnames)

    local_file_name = os.path.basename(record['local_file_raw'])
    node.study         = 'prediabetes'
    node.comment       = local_file_name
    node.sequence_type = 'nucleotide'
    node.seq_model     = 'MiSeq'  # record['seq_model']
    node.format        = 'fastq'
    node.format_doc    = 'https://en.wikipedia.org/wiki/FASTQ_format'
    node.exp_length    = 0 #record['exp_length']
    if record['consented'] == 'YES':
        node.local_file = record['local_file_raw']
        node.checksums  = {'md5':record['md5'], 'sha256':record['sha256']}
        node.size       = int(record['size'])
    else:
        node.private_files = True
        node.checksums     = {'md5': '00000000000000000000000000000000'}
        node.size          = 1
    node.tags = list_tags(
                          'jaxid (sample): '+record['jaxid_sample'],
                          'sample name: '+record['sample_name_id'],
                          'body site: '+record['body_site'],
                          'subject id: '+record['rand_subject_id'],
                          'study: prediabetes',
                          'prep_id:' + record['prep_id'],
                          'file name: '+ local_file_name,
                         )

    parent_link = {'sequenced_from':[parent_id]}
    log.debug('parent_id: '+str(parent_link))
    node.links = parent_link

    if not node.is_valid():
        invalidities = str(node.validate())
        err_str = "Invalid node {}!\t\t{}".format(node_type, invalidities)
        log.error(err_str)
        write_out_csv(data_file_name+'_invalid_records.csv',
                      fieldnames=csv_fieldnames.append('invalidities'),
                      values=[record.append(invalidities),])
        return False
    elif node.save():
        write_out_csv(data_file_name+'_submitted.csv',
                      fieldnames=csv_fieldnames, values=[record,])
        return node
    else:
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
            if record['local_file_raw'] != '':
                load_search_field = 'local_file'
                internal_id = os.path.basename(load_search_field)
                parent_internal_id = record['prep_id']
                # grand_parent_internal_id = record['visit_id']

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
            # raise e
    return nodes


if __name__ == '__main__':
    pass
