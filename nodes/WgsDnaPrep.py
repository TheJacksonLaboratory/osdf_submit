#!/usr/bin/env python
""" load mWGS DNA prep into OSDF using info from data file """

import os
import re

from cutlass.WgsDnaPrep import WgsDnaPrep

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, list_tags, format_query, \
        write_csv_headers, values_to_node_dict, write_out_csv, \
        load_node, get_field_header, dump_args, log_it, \
        get_cur_datetime


filename = os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = 'WgsDnaPrep'
parent_type        = 'Sample'
grand_parent_type  = 'Visit'
great_parent_type  = 'Subject'
great_great1_type  = 'Study'

node_tracking_file = settings.node_id_tracking.path

TAG_pri_D5 = ['AATGATACGGCGACCACCGAGATCTACAC',
              'ACACTCTTTCCCTACACGACGCTCTTCCGATCT']
TAG_pri_D7 = ['GATCGGAAGAGCACACGTCTGAACTCCAGTCAC',
              'ATCTCGTATGCCGTCTTCTGCTTG']

TAG_A = ['AGATCGGAAGAGCACACGTCTGAAC',
         'AGATCGGAAGAGCGTCGTGTAGGGA']

TAG_N = ['CAAGCAGAAGACGGCATACGAGAT',
         'GTCTCGTGGGCTCGG']
TAG_S = ['AATGATACGGCGACCACCGAGATCTACAC',
         'TCGTCGGCAGCGTC']


class node_values:
    comment = ''
    lib_layout = ''
    lib_selection = ''
    mims = {}
    ncbi_taxon_id = ''
    prep_id = ''
    sequencing_center = ''
    sequencing_contact = ''
    storage_duration = ''
    tags = []


class mims_fields:
    adapters = ''
    annot_source = ''
    assembly = ''
    assembly_name = ''
    biome = ''
    collection_date = ''
    encoded_traits = ''
    env_package = ''
    experimental_factor = ''
    extrachrom_elements = ''
    feature = ''
    findex = ''
    finishing_strategy = ''
    geo_loc_name = ''
    investigation_type = ''
    lat_lon = ''
    lib_const_meth = ''
    lib_reads_seqd = ''
    lib_screen = ''
    lib_size = 0
    lib_vector = ''
    material = ''
    nucl_acid_amp = ''
    nucl_acid_ext = '' #format: uri
    project_name = ''
    rel_to_oxygen = ''
    rindex = ''
    samp_collect_device = ''
    samp_mat_process = ''
    samp_size = ''
    seq_meth = ''
    sop = []
    source_mat_id = []
    submitted_to_insdc = False
    url = []


def concat_pcr(index_type):
    if re.match('D5', index_type) or re.match('D7', index_type):
        return ','.join([PCR_fwd_D5, PCR_rev_D7])
    elif re.match('A0', index_type):
        return ','.join([PCR_fwd_A5, PCR_rev_A7])
    else:
        return 'N/A'


def concat_tag(index_type,index_seq):
    if re.match('D5', index_type):
        tag_pre = TAG_pri_D5[0]
        tag_post = TAG_pri_D5[1]
    elif re.match('D7', index_type):
        tag_pre = TAG_pri_D7[0]
        tag_post = TAG_pri_D7[1]
    elif re.match('A', index_type):
        tag_pre = TAG_A[0]
        tag_post = TAG_A[1]
    elif re.match('N', index_type):
        tag_pre = TAG_N[0]
        tag_post = TAG_N[1]
    elif re.match('S', index_type):
        tag_pre = TAG_S[0]
        tag_post = TAG_S[1]
    else: # for single-read runs
        tag_pre = ''
        tag_post = ''

    return tag_pre + index_seq + tag_post


def determine_findex(index1_id, index1_seq, index2_id, index2_seq):
    """calc the findex based on index id value"""
    findex = index1_seq
    if re.match('D5', index2_id):
        findex = index2_seq
    return findex


def determine_rindex(index1_id, index1_seq, index2_id, index2_seq):
    """calc the findex based on index id value"""
    rindex = index2_seq
    if re.match('D7', index1_id):
        rindex = index1_seq
    return rindex


def generate_mims(row):
    try:
        # body_site specifics:
        gsc_package = \
                'human-gut' if row['body_site'] == 'stool' else \
                'human-associated' if row['body_site'] == 'nasal' \
                else ''
        mims = {
            # seq specifics:
            'adapters': ','.join([concat_tag(index_code, index_seq)
                                 for index_code, index_seq in
                                 [(row['index1_id'], row['index1_seq']),
                                  (row['index2_id'], row['index2_seq']) ]
                                 ]),
            'findex': determine_findex(row['index1_id'], row['index1_seq'],
                                       row['index2_id'], row['index2_seq']),
            'rindex': determine_rindex(row['index1_id'], row['index1_seq'],
                                       row['index2_id'], row['index2_seq']),
            # generics:
            'annot_source': 'N/A',
            'assembly': 'N/A',
            'assembly_name': 'N/A',
            'biome': 'terrestrial biome [ENVO:00000446]',
            'collection_date': ('2112-12-21'), #not allowed by IRB!
            'encoded_traits': 'N/A',
            'extrachrom_elements': 'N/A',
            'feature': 'N/A',
            'finishing_strategy': 'draft genome',
            'geo_loc_name': 'Palo Alto, CA, USA',
            'investigation_type': 'metagenome',
            'lat_lon': '37.441883, -122.143019',
            'lib_const_meth': 'N/A',
            'lib_reads_seqd': 'N/A',
            'lib_size': 700,
            'lib_vector': 'N/A',
            'lib_screen': 'N/A',
            'nucl_acid_amp': 'N/A',
            'nucl_acid_ext': 'Nucleic Acid Extraction [OBI:0666667]',
            'project_name': 'iHMP',
            'rel_to_oxygen': 'N/A',
            'samp_size': 'N/A',
            'sop': [],
            #TODO: put in place technique to specify RNA vs DNA prep
            # 'source_mat_id': ['ribonucleic acid CHEBI:33697',
            #                   'RNA extract OBI:0000845',
            #                   'nucleic acid extract OBI:0001010'],
            'source_mat_id': [ 'deoxyribonucleic acid CHEBI:16991',
                               'DNA extract OBI:0001051' ],
            'seq_meth': 'nextgen',
            'submitted_to_insdc': False,
            'url': [],
            'experimental_factor': gsc_package,
            'env_package': gsc_package,
            'material': \
                'feces(ENVO:00002003)' if row['body_site'] == 'stool' else \
                'oronasal secretion(ENVO:02000035)' if row['body_site'] == 'nasal' \
                else '',
            'samp_collect_device': \
                'self-sample' if row['body_site'] == 'stool' else \
                'self-swab' if row['body_site'] == 'nasal' \
                else '',
            'samp_mat_process': 'N/A',
        }
        return mims
    except Exception as e:
        log.error('!Conversion to MIMS format failed?! '
                  '    (DNAPrep: %s).\n'
                  '    Exception message:%s'
                  , row['prep_id'], e.message)


def load(internal_id, search_field):
    """search for existing node to update, else create new"""

    # node-specific variables:
    NodeTypeName = 'WgsDnaPrep'
    NodeLoadFunc = 'load_wgsDnaPrep'

    return load_node(internal_id, search_field, NodeTypeName, NodeLoadFunc)


def validate_record(parent_id, node, record, data_file_name=node_type):
    """update record fields
       validate node
       if valid, save, if not, return false
    """
    log.info("in validate/save: "+node_type)
    csv_fieldnames = get_field_header(data_file_name)
    write_csv_headers(data_file_name, fieldnames=csv_fieldnames)

    node.comment = record['prep_id']
    node.frag_size = 550 # goal size
    node.lib_layout = 'Paired End libraries, with nominal insert size of 450-550bp with a standard deviation of 50-60 bp'
    node.lib_selection = ''
    node.mims = generate_mims(record)
    node.ncbi_taxon_id = '408170' if 'stool' == record['body_site'] \
                         else '1131769' if 'nasal' == record['body_site'] \
                         else ''
            # ST: http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=408170
            # NS: http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=1131769
    node.prep_id = record['prep_id']
    node.sequencing_center = 'Jackson Laboratory for Genomic Medicine'
    node.sequencing_contact = 'George Weinstock'
    node.storage_duration = 1
    node.tags = list_tags(
            'dna_prep_id: '+ record['prep_id'],
            'jaxid (sample): ' +record['jaxid_sample'],
            'sample type: ' +record['body_site'],
            'subject id: ' +record['rand_subject_id'],
            'study: ' +'prediabetes',
           )
    parent_link = {'prepared_from':[parent_id]}
    log.debug('parent_id: '+str(parent_link))
    node.links = parent_link

    csv_fieldnames = get_field_header(data_file_name)
    if not node.is_valid():
        write_out_csv(data_file_name+'_invalid_records.csv',
                      fieldnames=csv_fieldnames, values=[record,])
        invalidities = node.validate()
        err_str = "Invalid {}!\n\t{}".format(node_type, str(invalidities))
        log.error(err_str)
        # raise Exception(err_str)
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
    write_csv_headers(data_file, fieldnames=csv_fieldnames)
    for record in load_data(data_file):
        log.info('...next record...')
        try:
            log.debug('data record: '+str(record))

            # node-specific variables:
            load_search_field = 'prep_id'
            internal_id = record['prep_id']
            parent_internal_id = record['sample_name_id']
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
                    saved_name = getattr(saved, load_search_field)
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
