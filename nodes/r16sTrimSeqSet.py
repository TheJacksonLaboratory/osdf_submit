#!/usr/bin/env python
""" load 16S DNA prep into OSDF using info from data file """

import os
import re

from cutlass.SixteenSRawSeqSet import SixteenSRawSeqSet

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, \
        list_tags, format_query, \
        values_to_node_dict, write_out_csv, \
        log_it, dump_args

filename=os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = 'r16sTrimSeqSet'
parent_type        = 'r16sRawSeqSet'
grand_parent_type  = 'r16sDnaPrep'
great_parent_type  = 'sample'
great_great1_type  = 'visit'
great_great2_type  = 'subject'
great_great3_type  = 'study'

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


@dump_args
def load(internal_id,parent_id,grand_parent_id):
    """search for existing node to update, else create new"""
    try:
        query = format_query(internal_id, field='prep_id')
        s = SixteenSRawSeqSet.search(query)
        for n in s:
            if parent_id in n.prep_id:
                return SixteenSRawSeqSet.load_sixteenSDnaPrep(n)
        # no match, return empty node:
        n = SixteenSRawSeqSet()
        return n
    except Exception, e:
        raise e


@dump_args
def validate_record(parent_id, node, record, data_filename=node_type):
    """update record fields
       validate node
       if valid, save, if not, return false
    """

    node.study         = 'prediabetes'
    node.comment       = record['prep_id']
    node.format        = 'fasta'
    node.format_doc    = 'https://en.wikipedia.org/wiki/FASTA_format'
    node.exp_length    = record['exp_length']
    node.local_file    = record['local_file']
    node.checksums     = record['checksums']
    node.size          = record['size']
    node.study         = record['study']
    node.urls          = []
    node.tags = list_tags(node.tags,
                'test', # for debug!!
                'jaxid (dna): '+record['jaxid_recd_dna'] \
                             if record['jaxid_recd_dna'] \
                             else 'jaxid (dna): none',
                'jaxid (tissue): '+record['jaxid_recd_tissue'] \
                                if record['jaxid_recd_tissue'] \
                                else 'jaxid (tissue): none',
                'jaxid (library): '+record['jaxid_library'] \
                                if record['jaxid_library'] \
                                else 'jaxid (library): none',
                'sample name: '+record['visit_id'],
                'body site: '+record['body_site'],
                'visit id: '+record['visit_id'],
                'subject id: '+record['rand_subject_id'],
                'study: prediabetes',
                'file prefix: '+ record['prep_id'],
                )

    log.debug('parent_id: '+str(parent_id))
    node.links = {'prepared_from':[parent_id]}
    if not node.is_valid():
        write_out_csv(data_filename+'_invalid_records.csv',
            fieldnames=record.keys(),values=[record,])
        invalidities = node.validate()
        err_str = "Invalid {}!\n{}".format(node_type, "\n".join(invalidities))
        log.error(err_str)
        # raise Exception(err_str)
    elif node.save():
        return node
    else:
        return False


# @dump_args
def submit(data_file, id_tracking_file=node_tracking_file):
    log.info('Starting submission of SixteenSTrimmedSeqSet.')
    nodes = []
    for record in load_data(data_file):
        log.debug('...next record...')
        try:
            log.debug('data record: '+str(record))
            sample_name = record['visit_id']
            parent_id = get_parent_node_id(
                    id_tracking_file, parent_type, sample_name)
            grand_parent_id = get_parent_node_id(
                    id_tracking_file, grand_parent_type, record['visit_id'])
            n = load(record['prep_id'],parent_id,grand_parent_id)
            if not n.prep_id:
                log.debug('loaded node newbie...')
                saved = validate_record(parent_id, n, record,
                                        data_filename=data_file)
                if saved:
                    header = settings.node_id_tracking.id_fields
                    vals = values_to_node_dict(
                            [[node_type,saved.prep_id,saved.id,
                              parent_type,sample_name,parent_id]] #hack: identical variables!
                            )
                    nodes.append(vals)
                    write_out_csv(id_tracking_file,values=vals)
                    write_out_csv(data_file+'_submitted.csv',
                        fieldnames=record.keys(),values=[record,])
                # else:
                    # write_out_csv(data_file+'_unsaved_records.csv',
                        # fieldnames=record.keys(),values=[record,])
        except Exception, e:
            log.exception(e)
            raise e
    return nodes


if __name__ == '__main__':
    pass
