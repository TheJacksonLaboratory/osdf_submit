#!/usr/bin/env python

import os
import logging
import json

from cutlass import iHMPSession, mixs, mims, mimarks
from cutlass import Project, Study, Subject, Visit, Sample
from cutlass import \
    SixteenSDnaPrep, SixteenSRawSeqSet, SixteenSTrimmedSeqSet, \
    WgsDnaPrep, WgsRawSeqSet \

from cutlass_utils import save_if_valid, \
    load_string_from_file, \
    load_config_from_file

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(name=os.path.basename(__file__))


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
        'Subject': [],
        'Visit': [],
        'Sample': [],
        }
#TODO: use 'tags' or '_attribs' field to refer to direct parent e.g. subj_id or sample_id

def submit_project(node_info):
    """ load Project info from node info file """
    node_type = 'Project'
    node = session.create_project()
    log.debug("Required fields: {}".format(node.required_fields()))
    node.load(to_json(node_info))
    node_id = save_if_valid(node)
    log.info('{} ID: {}'.format(node_type, node_id))
    return node_id

def submit_study(node_info, parent_id):
    """ load Study info from node info file """
    node_type = 'Study'
    # project_id = parent_id
    node = session.create_study()
    log.debug("Required fields: {}".format(node.required_fields()))
    node.links = { 'part_of' : [ parent_id ], "subset_of" : [] }
    # node.load(info_json(node_info))
    node.load(info_json(node_info))
    node_id = save_if_valid(node)
    return node_id

def submit_subject(node_info, parent_id):
    """ load Subject info from node info file """
    node_type = 'Subject'
    # study_id = parent_id
    node = session.create_subject()
    log.debug("Required fields: {}".format(node.required_fields()))
    node.links = { participates_in : [ parent_id ] }
    node.load(info_json(node_info))
    node_id = save_if_valid(node)
    return node_id

def submit_visit(node_info, parent_id):
    """ load Visit info from node info file """
    node_type = 'Visit'
    # subject_id = parent_id
    node = session.create_visit()
    log.debug("Required fields: {}".format(node.required_fields()))
    node.links = { by : [ parent_id ] }
    node.load(info_json(node_info))
    node_id = save_if_valid(node)
    return node_id

def submit_sample(node_info, parent_id):
    """ load Sample info from node info file """
    node_type = 'Sample'
    # visit_id = parent_id
    node = session.create_sample()
    log.debug("Required fields: {}".format(node.required_fields()))
    node.links = { collected_during : [ parent_id ] }
    node.load(info_json(node_info))
    node_id = save_if_valid(node)
    return node_id

def submit_SixteenSDnaPrep(node_info, parent_id):
    """ load 16S DNA Prep info from node info file """
    node_type = '16S DNA Prep'
    # sample_id = parent_id
    node = session.create_16s_dna_prep()
    log.debug("Required fields: {}".format(node.required_fields()))
    node.links = { prepared_from: [ parent_id ] }
    node.load(info_json(node_info))
    node_id = save_if_valid(node)
    node_list.append(node_id)
    return node_list

def submit_SixteenSRawSeqSet(node_info, parent_id):
    """ load 16S Raw Sequence Set info from node info file """
    node_type = '16S Raw Seq Set'
    # sixteensdnaprep_id = parent_id
    node = session.create_16s_raw_seq_set()
    log.debug("Required fields: {}".format(
        node.required_fields()))
    node.links = { sequenced_from: [ parent_id ] }
    node.load(info_json(node_info))
    node_id = save_if_valid(node)
    node_list.append(node_id)
    return node_list

def submit_SixteenSTrimmedSeqSet(node_info, parent_id):
    """ load 16S Trimmed Sequence Set info from node info file """
    node_type = '16S Trimmed Seq Set'
    # sixteensrawseqset_id = parent_id
    node = session.create_16s_trimmed_seq_set()
    log.debug("Required fields: {}".format(
        node.required_fields()))
    node.links = { sequenced_from: [ parent_id ] }
    node.load(info_json(node_info))
    node_id = save_if_valid(node)
    return node_id

def submit_WgsDnaPrep(node_info, parent_id):
    """ submit WGS DNA Prep info into node """
    node_type = 'WGS DNA Prep'
    node = session.create_wgs_dna_prep()
    log.debug("Required fields: {}".format(node.required_fields()))
    node.links = { prepared_from: parent_id }
    node.load(info_json(node_info))
    node_id = save_if_valid(node)
    return node_id

def submit_WgsRawSeqSet(node_info, parent_id):
    node_type = 'WGS Raw Seq Set'
    node = session.create_wgs_raw_seq_set()
    log.debug("Required fields: {}".format(node.required_fields()))
    node.links = { sequenced_from: [ parent_id ] }
    node_id = save_if_valid(node)
    node.load(info_json(node_info))
    return node_id


def info_json(info):
    """ convert dict to json string for osdf nodes """
    return json.dumps(info)


# TODO: refactor from serial for single to hierarchical for each set/subset

""" load Project info from node info file """
# config = load_config_from_file(NodeInfo['Project'])
# project_id = submit_project(config)

# Predefined_id_from_osdf_for_overall_iHMP_project !?valid?!
project_id = '610a4911a5ca67de12cdc1e4b40018e1?'

""" load Study info from node info file """
config = load_config_from_file(NodeInfo['Study'])
try:
    study_id = submit_study(config, project_id)
except Exception as e:
    import pdb; pdb.set_trace()

""" load Subject info from node info file """
config = load_config_from_file(NodeInfo['Subject'])
for node_info in config:
    subject_id = submit_subject(node_info, study_id)

    """ load Visit info from node info file """
    config = load_config_from_file(NodeInfo['Visit'])
    for node_info in config:
        visit_id = submit_visit(node_info, subject_id)

        """ load Sample info from node info file """
        config = load_config_from_file(NodeInfo['Sample'])
        for node_info in config:
            sample_id = submit_sample(node_info, visit_id)

            """ load 16S DNA Prep info from node info file """
            config = load_config_from_file(NodeInfo['SixteenSDnaPrep'])
            for node_info in config:
                sixteensdnaprep_id = \
                    submit_SixteenSDnaPrep(node_info, sample_id)

            #     """ load 16S Raw Sequence Set info from node info file """
            #     config = load_config_from_file(NodeInfo['SixteenSRawSeqSet'])
            #     for node_info in config:
            #         sixteensrawseqset_id = \
            #             submit_SixteenSRawSeqSet(node_info,
            #                     sixteensdnaprep_id)

            #     """ load 16S Trimmed Sequence Set info from node info file """
            #     config = load_config_from_file(
            #             NodeInfo['SixteenSTrimmedSeqSet'])
            #     for node_info in config:
            #         sixteenstrimmed_id = submit_SixteenSTrimmedSeqSet(
            #                 node_info, sixteensdnaprep_id)

            # """ load WGS DNA Prep info from node info file """
            # config = load_config_from_file(NodeInfo['WgsDnaPrep'])
            # for node_info in config:
            #     wgsdnaprep_id = submit_WgsDnaPrep(node_info, sample_id)

            #     """ load WGS Raw Sequence Set info from node info file """
            #     config = load_config_from_file(NodeInfo['WgsRawSeqSet'])
            #     for node_info in config:
            #         wgsrawseqset_id = \
            #                 submit_WgsDnaPrep(node_info, wgsdnaprep_id)

