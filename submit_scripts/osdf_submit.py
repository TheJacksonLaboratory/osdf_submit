#!/usr/bin/env python

# from enum import Enum
import logging

from cutlass import iHMPSession, mixs, mims, mimarks
from cutlass import Project, Study, Subject, Visit, Sample
from cutlass import \
    SixteenSDnaPrep, SixteenSRawSeqSet, SixteenSTrimmedSeqSet, \
    WgsDnaPrep, WgsRawSeqSet \

from cutlass_utils import save_if_valid, \
    load_string_from_file, \
    load_config_from_file

logging.basicConfig(level=logging.DEBUG)

# info file names
NodeInfo = {
    'Project': '../data_files/project_info.yaml',
    'Study': '../data_files/study_info.yaml',
    'Subject':  '../data_files/subjects_info.yaml',
    'Visit':  '../data_files/.yaml',
    'Sample':  '../data_files/.yaml',
    'SixteenSDnaPrep': '../data_files/16S_dna_prep.yaml',
    'SixteenSRawSeqSet':  '../data_files/.yaml',
    'SixteenSTrimmedSeqSet': '../data_files/.yaml',
    'WgsDnaPrep': '../data_files/.yaml',
    'WgsRawSeqSet': '../data_files/.yaml',
    }

info_file_16S_raw_seq_set = '../data_files/16S_raw_seq_set.yaml'

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')
session = iHMPSession(username, password)




""" load Project info from node info file """
config = load_config_from_file(NodeInfo['Project'])
for node_info in config:
    node = session.create_project()
    print("Required fields: {}".format(Project.required_fields()))
    node.name = node_info['name']
    node.description = node_info['description']
    node.center = node_info['center']
    node.contact = node_info['contact']
    node.srp_id = node_info['srp_id']
    node.tags = node_info['tags']
    node.mixs = node_info['mixs']
    save_if_valid(node, Project)
    project_id = node.id

""" load Study info from node info file """
config = load_config_from_file(NodeInfo['Study'])
for node_info in config:
    node = session.create_study()
    print("Required fields: {}".format(Study.required_fields()))
    node.name = node_info['name']
    node.description = node_info['description']
    node.center = node_info['center']
    node.contact = node_info['contact']
    node.srp_id = node_info['srp_id']
    node.subtype = node_info['subtype']
    node.tags = node_info['tags']
    node.links = { part_of : [ project_id ] }
    study_id = save_if_valid(node)

""" load Subject info from node info file """
config = load_config_from_file(NodeInfo['Subject'])
for node_info in config:
    node = session.create_subject()
    print("Required fields: {}".format(Subject.required_fields()))
    node.rand_subject_id = node_info['subject_id']
    node.gender = node_info['gender']
    node.race = node_info['race']
    node.tags = node_info['tags']
    node._attribs = node_info['attributes']
    node.links = { participates_in : [ study_id ] }
    subject_id = save_if_valid(node)

    """ load Visit info from node info file """
    config = load_config_from_file(NodeInfo['Visit'])
    for node_info in config:
        node = session.create_visit()
        print("Required fields: {}".format(Visit.required_fields()))
        node.visit_id = node_info['visit_id']
        node.visit_number = node_info['visit_number']
        node.date = str(node_info['date']) # cutlass expects 'str' format, not 'datetime'
        node.interval = node_info['interval']
        node.clinic_id = node_info['clinic_id']
        node.tags = node_info['tags']
        node._attribs = node_info['attributes']
        node.links = { by : [ subject_id ] }
        visit_id = save_if_valid(node)

        """ load Sample info from node info file """
        config = load_config_from_file(NodeInfo['Sample'])
        for node_info in config:
            node = session.create_sample()
            print("Required fields: {}".format(Sample.required_fields()))
            node.visit_id = node_info['visit_id']
            node.visit_number = node_info['visit_number']
            node.date = str(node_info['date']) # cutlass expects 'str' format, not 'datetime'
            node.interval = node_info['interval']
            node.clinic_id = node_info['clinic_id']
            node.fma_body_site = node_info['fma_body_site']
            node.tags = node_info['tags']
            node.mixs = node_info['mixs']
            node._attribs = node_info['attributes']
            node.links = { ollected_during : [ visit_id ] }
            sample_id = save_if_valid(node)



""" load 16S DNA Prep info from node info file """
config = load_config_from_file(NodeInfo['SixteenSDnaPrep'])
for node_info in config:
    node = session.create_16s_dna_prep()
    print("Required fields: {}".format(SixteenSDnaPrep.required_fields()))
    node.comment = node_info['comment']
    node.frag_size = node_info['frag_size']
    node.lib_layout = node_info['lib_layout']
    node.lib_selection = node_info['lib_selection']
    node.mimarks = node_info['mimarks']
    node.storage_duration = node_info['storage_duration']
    node.sequencing_center = node_info['sequencing_center']
    node.sequencing_contact = node_info['sequencing_contact']
    node.prep_id = node_info['prep_id']
    node.ncbi_taxon_id = node_info['ncbi_taxon_id']
    node.tags = node_info['tags']
    node.links = node_info['links']
    sixteensdnaprep_id = save_if_valid(node)

""" load 16S Raw Sequence Set info from node info file """
config = load_config_from_file(NodeInfo['SixteenSRawSeqSet'])
for node_info in config:
    node = session.create_16s_raw_seq_set()
    print("Required fields: {}".format(SixteenSRawSeqSet.required_fields()))
    node.comment = node_info['comment']
    node.checksums = node_info['checksums']
    node.exp_length = node_info['exp_length']
    node.format = node_info['format']
    node.format_doc = node_info['format_doc']
    node.seq_model = node_info['seq_model']
    node.sequence_type = node_info['sequence_type']
    node.size = node_info['size']
    node.study = node_info['study']
    node.subtype = node_info['subtype']
    node.local_file = node_info['local_file']
    node.tags = node_info['tags']
    node.links = node_info['links']
    sixteensrawseqset_id = save_if_valid(node)

# config = load_config_from_file(NodeInfo['SixteenSTrimmedSeqSet'])
# for node_info in config:
    # node = session.create_16s_trimmed_seq_set()
    # print("Required fields: {}".format(SixteenSTrimmedSeqSet.required_fields()))
    # save_if_valid(node)

""" load WGS DNA Prep info from node info file """
config = load_config_from_file(NodeInfo['WgsDnaPrep'])
for node_info in config:
    node = session.create_wgs_dna_prep()
    print("Required fields: {}".format(WgsDnaPrep.required_fields()))
    node.comment = node_info['comment']
    node.frag_size = node_info['frag_size']
    node.lib_layout = node_info['lib_layout']
    node.lib_selection = node_info['lib_selection']
    node.mims = node_info['mims']
    node.storage_duration = node_info['storage_duration']
    node.sequencing_center = node_info['sequencing_center']
    node.sequencing_contact = node_info['sequencing_contact']
    node.prep_id = node_info['prep_id']
    node.ncbi_taxon_id = node_info['ncbi_taxon_id']
    node.tags = node_info['tags']
    node.links = node_info['links']
    wgsdnaprep_id = save_if_valid(node)

# """ load WGS Raw Sequence Set info from node info file """
# config = load_config_from_file(NodeInfo['WgsRawSeqSet'])
# for node_info in config:
    # node = session.create_wgs_raw_seq_set()
    # print("Required fields: {}".format(WgsRawSeqSet.required_fields()))
    # wgsrawseqset_id = save_if_valid(node)

