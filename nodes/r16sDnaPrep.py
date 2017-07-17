#!/usr/bin/env python
""" load 16S DNA prep into OSDF using info from data file """

import os
import re

from cutlass.SixteenSDnaPrep import SixteenSDnaPrep

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, list_tags, format_query, \
        write_csv_headers, values_to_node_dict, write_out_csv, \
        load_node, get_field_header, dump_args, log_it, \
        get_cur_datetime

filename = os.path.basename(__file__)
log = log_it(filename)

# the Higher-Ups
node_type          = '16s_dna_prep'
parent_type        = 'Sample'
grand_parent_type  = 'Visit'
great_parent_type  = 'Subject'
great_great1_type  = 'Study'

node_tracking_file = settings.node_id_tracking.path

# TruSeq
TAG_pri_D5 = ['AATGATACGGCGACCACCGAGATCTACAC',
              'ACACTCTTTCCCTACACGACGCTCTTCCGATCT']
PCR_fwd_D5 = 'AGAGTTTGATCCTGGCTCAG'
TAG_pri_D7 = ['GATCGGAAGAGCACACGTCTGAACTCCAGTCAC',
              'ATCTCGTATGCCGTCTTCTGCTTG']
PCR_rev_D7 = 'ATTACCGCGGCTGCTGG'

# ScriptSeq
PCR_fwd_A = []
PCR_rev_A = []
TAG_pri_A = ['AGAGTTTGATCCTGGCTCAG','ATTACCGCGGCTGCTGG']


class node_values:
    comment = ''
    lib_layout = ''
    lib_selection = ''
    mimarks = {}
    ncbi_taxon_id = ''
    prep_id = ''
    sequencing_center = ''
    sequencing_contact = ''
    storage_duration = ''
    tags = []


class mimarks_fields:
    adapters = ''
    biome = ''
    collection_date = ''
    experimental_factor = ''
    feature = ''
    findex = ''
    geo_loc_name = ''
    investigation_type = ''
    isol_growth_condt = ''
    lat_lon = ''
    lib_const_meth = ''
    lib_reads_seqd = ''
    lib_size = 0
    lib_vector = ''
    material = ''
    nucl_acid_amp = ''
    nucl_acid_ext = ''
    pcr_primers = ''
    pcr_cond = ''
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
    target_gene = ''
    target_subfragment = ''
    url = []


def concat_pcr(index_type):
    if re.match('D5', index_type) or re.match('D7', index_type):
        return ','.join([PCR_fwd_D5, PCR_rev_D7])
    # elif re.match('A%', index_type):
    #     return ','.join([PCR_fwd_A5, PCR_rev_A7])
    else:
        return 'N/A'


def concat_tag(index_type,index_seq):
    if re.match('D5', index_type):
        tag_pre = TAG_pri_D5[0]
        tag_post = TAG_pri_D5[1]
    elif re.match('D7', index_type):
        tag_pre = TAG_pri_D7[0]
        tag_post = TAG_pri_D7[1]
    elif re.match('A5', index_type):
        tag_pre = TAG_pri_A[0]
        tag_post = TAG_pri_A[1]
    else:
        tag_pre = TAG_pri_D7[0]
        tag_post = TAG_pri_D7[1]

    return tag_pre + index_seq + tag_post


def generate_mimarks(row):
    DEGREE_SIGN = u"\N{DEGREE SIGN}"
    DEGREE = DEGREE_SIGN.encode("UTF-8")
    try:
        mimarks = {
            'adapters': ','.join([concat_tag(index_code, index_seq)
                                 for index_code, index_seq in
                                 [(row['index1_id'], row['index1_seq']),
                                  (row['index2_id'], row['index2_seq']) ]
                                 ]),
            'biome': 'terrestrial biome [ENVO:00000446]',
            'collection_date': ('2112-12-21'), #not allowed by IRB!
            'feature': 'N/A',
            'findex': row['index2_seq'] \
                    if re.match('D5', row['index2_id']) else '',
            'rindex': row['index2_seq'] \
                    if re.match('D7', row['index1_id']) else '',
            'geo_loc_name': 'Palo Alto, CA, USA',
            'investigation_type': 'metagenome',
            'isol_growth_condt': 'N/A',
            'lat_lon': '37.441883, -122.143019',
            'lib_const_meth': 'paired end 16S 454 Amp protocol',
            'lib_reads_seqd': 'N/A',
            'lib_size': 700,
            'lib_vector': 'N/A',
            'nucl_acid_amp': 'N/A',
            'nucl_acid_ext': 'Nucleic Acid Extraction [OBI:0666667]',
            'pcr_cond': 'initial denaturation: 95C_2min; '
                '[denaturation: 95C_20sec; anealing: 56C_30sec; '
                'extension: 72C_5min]-30 cycles: hold: 4C',
            'pcr_primers': concat_pcr(row['index1_id']),
            'project_name': 'iHMP',
            'rel_to_oxygen': 'N/A',
            'samp_size': 'N/A',
            'sop': [],
            'source_mat_id': [ 'deoxyribonucleic acid CHEBI:16991',
                               'DNA extract OBI:0001051' ],
            'seq_meth': 'nextgen',
            'submitted_to_insdc': False,
            'target_gene': '16S rRNA',
            'target_subfragment': 'V1-V3',
            'url': [],
            'experimental_factor': 'human-gut' \
                if 'stool' == row['body_site'].lower() \
                else 'human-associated',
            'material': 'feces(ENVO:00002003)' \
                if 'stool' == row['body_site'].lower() \
                else 'oronasal secretion(ENVO:02000035)',
            'samp_collect_device': 'self-sample' \
                if 'stool' == row['body_site'].lower() \
                else 'self-swab',
            'samp_mat_process': 'N/A',
        }
        return mimarks
    except Exception as e:
        log.error('Conversion to MIMARKS format failed?! '
                  '    (DNAPrep: %s).\n'
                  '    Exception message:%s'
                  , row['prep_id'], e.message)


def load(internal_id, search_field):
    """search for existing node to update, else create new"""

    # node-specific variables:
    NodeTypeName = 'SixteenSDnaPrep'
    NodeLoadFunc = 'load_sixteenSDnaPrep'

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
    node.frag_size = 301 # goal size
    node.lib_layout = 'paired 301bp'
    node.lib_selection = ''
    node.mimarks = generate_mimarks(record)
    node.ncbi_taxon_id = '408170' \
            if 'stool' == record['body_site'].lower() \
            else '1131769' # nasal
            # ST: http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=408170
            # NS: http://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=1131769
    node.prep_id = record['prep_id']
    node.sequencing_center = 'Jackson Laboratory for Genomic Medicine'
    node.sequencing_contact = 'George Weinstock'
    node.storage_duration = 2112
    node.tags = list_tags(
                          'jaxid (sample): '+record['jaxid_sample'],
                          'subject id: '+record['rand_subject_id'],
                          'study: prediabetes',
                          'file prefix: '+ record['prep_id'],
                         )
    parent_link = {'prepared_from':[parent_id]}
    log.debug('parent_id: '+str(parent_link))
    node.links = parent_link

    csv_fieldnames = get_field_header(data_file_name)
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
                        write_out_csv(
                            id_tracking_file,
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
