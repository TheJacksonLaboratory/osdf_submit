#!/usr/bin/env python

from os.path import basename
import os, logging
import json

from cutlass import iHMPSession
# from cutlass import Project, Study, Subject, Visit, Sample
# from cutlass import \
    # SixteenSDnaPrep, SixteenSRawSeqSet, SixteenSTrimmedSeqSet, \
    # WgsDnaPrep, WgsRawSeqSet \

from cutlass_utils import load_string_from_file
from cutlass_utils import load_yaml_data as load_data
from cutlass_utils import submit_node, load_csv_rows_dict,\
        write_out_csv, read_ids, match_records
import cutlass_utils
import settings

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(name=basename(__file__))


# config file names
NodeDataFiles = {
    'Project':               '../data_files/project_info.yaml',
    'Study':                 '../data_files/study_info.yaml',
    'Subject':               '../data_files/subjects_info.yaml',
    'Visit':                 '../data_files/visits_info.yaml',
    'Sample':                '../data_files/samples_info.yaml',
    'SixteenSDnaPrep':       '../data_files/16S_dna_prep.yaml',
    'SixteenSRawSeqSet':     '../data_files/16s_raw_seq_set.yaml',
    'SixteenSTrimmedSeqSet': '../data_files/16s_trim_set.yaml',
    'WgsDnaPrep':            '../data_files/wgs_dna_prep.yaml',
    'WgsRawSeqSet':          '../data_files/wgs_raw_seq_set.yaml',
    }

info_file_16S_raw_seq_set = '../data_files/16S_raw_seq_set.yaml'

node_tracking_file = settings.node_id_file.path

# load username, password from files
dcc_user = settings.auth.dcc_user
dcc_pass = settings.auth.dcc_pass
session = iHMPSession(dcc_user, dcc_pass)
log.info('Loaded session: {}'.format(session.get_session()))

# TODO: inform Vincent of cutlass.examples outdated usage of 'links' instead of 'linkage'
    # node.links = { 'part_of' : [ parent_id ], "subset_of" : [] }
    # node.linkage = { 'part_of' : [ parent_id ], "subset_of" : [] }


def submit_study(node_info, parent_id):
    """ load Study into OSDF using info from file
        Note: this only uses the 'part_of' linkage for the project!
    """
    # TODO: allow use of linkage 'subset_of' for sub-studies (submit_sub_study)
    pass
    return submit_node('Study',
            session.create_study(), node_info,
            'part_of', parent_id)

def submit_subject(node_info, parent_id):
    """ load Subject into OSDF using info from file """
    return submit_node('Subject',
            session.create_subject(), node_info,
            'participates_in', parent_id)

def submit_visit(node_info, parent_id):
    """ load Visit into OSDF using info from file """
    return submit_node('Visit',
            session.create_visit(), node_info,
            'by', parent_id)

def submit_Sample(node_info, parent_id):
    """ load Sample into OSDF using info from file """
    return submit_node('Sample',
            session.create_sample(), node_info,
            'collected_during', parent_id)

def submit_SixteenSDnaPrep(node_info, parent_id):
    """ load 16S DNA Prep into OSDF using info from file """
    return submit_node('16S DNA Prep',
            session.create_16s_dna_prep(), node_info,
            'prepared_from', parent_id)

def submit_SixteenSRawSeqSet(node_info, parent_id):
    """ load 16S Raw Sequence Set into OSDF using info from file """
    return submit_node('16S Raw Seq Set',
            session.create_16s_raw_seq_set(), node_info,
            'sequenced_from', parent_id)

def submit_SixteenSTrimmedSeqSet(node_info, parent_id):
    """ load 16S Trimmed Sequence Set into OSDF using info from file """
    return submit_node('16S Trimmed Seq Set',
            session.create_16s_trimmed_seq_set(), node_info,
            'computed_from', parent_id)

def submit_WgsDnaPrep(node_info, parent_id):
    """ load WGS DNA Prep into OSDF using info from file """
    return submit_node('WGS DNA Prep',
            session.create_wgs_dna_prep(), node_info,
            'prepared_from', parent_id)

def submit_WgsRawSeqSet(node_info, parent_id):
    """ load WGS DNA Raw Sequence Set into OSDF using info from file """
    return submit_node('WGS Raw Seq Set',
            session.create_wgs_raw_seq_set(), node_info,
            'sequenced_from', parent_id)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Main Actions ~~~~~


""" Project node """
# Predefined_id_from_osdf_for_overall_iHMP_project
project_node_id = '3fffbefb34d749c629dc9d147b18e893?'

""" Study node """
# config = load_data(NodeDataFiles['Study'])
from nodes import study
study = study.get_study()
study_node_id = study.

""" Subject node """
from nodes import subject
node_records = load_csv_rows_dict(node_tracking_file)
for node_info in load_data(NodeDataFiles['Subject']):
    study_node_id = match_records(node_records,
                                  study.node_values.name,
                                  'internal_id'
                                  )[0]
    subject_node_id = submit_subject(node_info, study_node_id)
    #pseudocode:
    # write_out_csv 'study_id, subject_id, subject_node_id'
    # later retrieval of multiple lower levels
    values=['subject',subject.node_values.rand_subject_id,subject_node_id
            'study',study.node_values.name,study_node_id
            ]
    write_out_csv(node_tracking_file,values)


""" Visit node """
node_records = load_csv_rows_dict(node_tracking_file)
for node_info in load_yaml_data(NodeDataFiles['Visit']):
    subject_list = match_records(node_records, node_info.tags['internal_id'])
    for row in subject_list:
        # subject_id, subject_node_id = csv_split_fields??
        visit_node_id = submit_visit(node_info, )
        #pseudocode:
        # write_out_csv 'study_id, subject_id, visit_id, visit_node_id'

""" Sample node """
node_records = load_csv_rows_dict(node_tracking_file)
for node_info in load_data(NodeDataFiles['Sample']):
    visit_list = match_records(node_records, node_info.tags['internal_id'])
    for row in visit_list:
        # visit_id, visit_node_id =
        sample_node_id = submit_Sample(node_info, visit_node_id)

mix_dict = cutlass_utils.osdf_gensc_required_dicts.mixs
print(mix_dict.biome)

# """ 16S DNA Prep node """
# for node_info in load_data(NodeDataFiles['SixteenSDnaPrep']):
    # sixteensdnaprep_node_id = \
            # submit_SixteenSDnaPrep(node_info, sample_node_id)

# """ 16S Raw Sequence Set node """
# for node_info in load_data(NodeDataFiles['SixteenSRawSeqSet']):
    # sixteensrawseqset_node_id = \
        # submit_SixteenSRawSeqSet(node_info, sixteensdnaprep_node_id)

# """ 16S Trimmed Sequence Set node """
# for node_info in load_data(NodeDataFiles['SixteenSTrimmedSeqSet']):
    # sixteenstrimmed_node_id = submit_SixteenSTrimmedSeqSet(
            # node_info, sixteensdnaprep_node_id)

# """ load WGS DNA Prep node """
# for node_info in load_data(NodeDataFiles['WgsDnaPrep']):
    # wgsdnaprep_node_id = submit_WgsDnaPrep(node_info, sample_node_id)

# """ load WGS Raw Sequence Set node """
# for node_info in load_data(NodeDataFiles['WgsRawSeqSet']):
    # wgsrawseqset_node_id = \
        # submit_WgsDnaPrep(node_info, wgsdnaprep_node_id)

