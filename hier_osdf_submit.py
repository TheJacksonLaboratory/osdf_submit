#!/usr/bin/env python

from os.path import basename
import logging
import json

from cutlass import iHMPSession
# from cutlass import Project, Study, Subject, Visit, Sample
# from cutlass import \
    # SixteenSDnaPrep, SixteenSRawSeqSet, SixteenSTrimmedSeqSet, \
    # WgsDnaPrep, WgsRawSeqSet \

from cutlass_utils import load_string_from_file
from cutlass_utils import load_config_from_files as load_config

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(name=basename(__file__))


# config file names
NodeInfo = {
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

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')
session = iHMPSession(username, password)
log.info('Loaded session: {}'.format(session.get_session()))

data_node_ids = {
        'Project': '',
        'Study': '',
        'SubStudy': '',
        'Subject': [],
        'Visit': [],
        'Sample': [],
        }

# TODO: inform Vincent of cutlass.examples outdated usage of 'links' instead of 'linkage'
    # node.links = { 'part_of' : [ parent_id ], "subset_of" : [] }
    # node.linkage = { 'part_of' : [ parent_id ], "subset_of" : [] }

def submit_node(node_type, node_creation, node_info, child_text, parent_id):
    """ Load node into OSDF using info from file.
        Can be called from specific node-type functions with predetermined args
    """
    try:
        node = node_creation

        log.debug("Required fields: {}".format(node.required_fields()))
        # set node required fields empty as start point
        for f in node.required_fields():
            node.f = ''
        log.debug("Required fields all set empty.")
        #TODO: set empty required MIMS, MIMARKS, MIXS fields

        # load all other fields from data file
        for f in node_info:
            node.f = node_info[f]
        log.debug("All fields set from data file.")

        # set linkage as passed in 'child_text'
        node.linkage = { child_text: [ parent_id ] }
        log.debug("Node 'linkage' set.")

        success = node.save()
        log.info('{} ID: {}'.format(node_type, node._id))
        return node._id
    except Exception as e:
        log.debug(e)
        raise e


def submit_project(node_info):
    """ load Project into OSDF using info from file """
    pass

def submit_study(node_info, parent_id):
    """ load Study into OSDF using info from file
        Note: this only uses the 'part_of' linkage for the project!
    """
    # TODO: allow use of linkage 'subset_of' for sub-studies (submit_sub_study)
    return submit_node('Study',
            session.create_study(), node_info,
            'part_of', parent_id)

def submit_subject(node_info, parent_id):
    """ load Subject into OSDF using info from file """
    return submit_node('Subject',
            session.create_subject(), node_info,
            'participates_in', parent_id)

def submit_visit(node_info, parent_id):
    """ load Subject into OSDF using info from file """
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
    """ submit WGS DNA Prep info into node """
    return submit_node('WGS DNA Prep',
            session.create_wgs_dna_prep(), node_info,
            'prepared_from', parent_id)

def submit_WgsRawSeqSet(node_info, parent_id):
    return submit_node('WGS Raw Seq Set',
            session.create_wgs_raw_seq_set(), node_info,
            'sequenced_from', parent_id)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Main Actions ~~~~~

""" Project node """
# config = load_config_from_file(NodeInfo['Project'])
# project_id = submit_project(config)

# Predefined_id_from_osdf_for_overall_iHMP_project !?still valid?!
project_id = '610a4911a5ca67de12cdc1e4b40018e1?'

""" Study node """
config = load_config(NodeInfo['Study'])
study_id = submit_study(config[0], project_id)

""" Subject node """
subject_id_csv = open('ihmp_prediab_subject_ids.json', 'w')
for node_info in load_config(NodeInfo['Subject']):
    subject_id = submit_subject(node_info, study_id)
    #pseudocode:
    # write_out_csv 'subject_person_id, subject_json_id'
    # later retrieval of multiple lower levels

    """ Visit node """
    config = load_config_from_file(NodeInfo['Visit']):
    for node_info in config:
        visit_id = submit_visit(node_info, subject_id)

        """ Sample node """
        for node_info in load_config(NodeInfo['Sample']):
            sample_id = submit_Sample(node_info, visit_id)

            """ 16S DNA Prep node """
            for node_info in load_config(NodeInfo['SixteenSDnaPrep']):
                sixteensdnaprep_id = \
                        submit_SixteenSDnaPrep(node_info, sample_id)

                """ 16S Raw Sequence Set node """
                for node_info in load_config(NodeInfo['SixteenSRawSeqSet']):
                    sixteensrawseqset_id = \
                        submit_SixteenSRawSeqSet(node_info, sixteensdnaprep_id)

                """ 16S Trimmed Sequence Set node """
                for node_info in load_config(NodeInfo['SixteenSTrimmedSeqSet'])
                    sixteenstrimmed_id = submit_SixteenSTrimmedSeqSet(
                            node_info, sixteensdnaprep_id)

            """ load WGS DNA Prep node """
            for node_info in load_config(NodeInfo['WgsDnaPrep']):
                wgsdnaprep_id = submit_WgsDnaPrep(node_info, sample_id)

                """ load WGS Raw Sequence Set node """
                for node_info in load_config(NodeInfo['WgsRawSeqSet']):
                    wgsrawseqset_id = \
                            submit_WgsDnaPrep(node_info, wgsdnaprep_id)

