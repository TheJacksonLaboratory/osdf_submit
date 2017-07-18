#!/usr/bin/env python

import os
import sys
import json
import logging

from cutlass import iHMPSession

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, list_tags, format_query, \
        write_csv_headers, values_to_node_dict, \
        get_field_header, dump_args, log_it

log = log_it('jax_osdf_submit')
log.info('Starting metadata submission to OSDF server.')

# load username, password from files
dcc_user = settings.auth.dcc_user
dcc_pass = settings.auth.dcc_pass
session = iHMPSession(dcc_user, dcc_pass, ssl=False)
log.info('Loaded session: {}'.format(session.get_session()))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Main Actions ~~~~~

def main():
    """make it happen!"""

    """ Project node """
    # Predefined_id_from_osdf_for_overall_iHMP_project
    project_name = 'iHMP'
    project_node_id = '3fffbefb34d749c629dc9d147b18e893'

    """ Study node """
    #from nodes import study
    #study = study.get_study(project_node_id)
    #study_node_id = study.id if study else ''
    #study_name = study.name if study else ''
    #log.debug('Study.id: "{}"'.format(str(study_node_id)))
    #if not study_node_id:
    #    sys.exit('Saving study record failed!')

    study_name = 'prediabetes'
    study_node_id = '194149ed5273e3f94fc60a9ba58f7c24'

    #node_tracking_file = settings.node_id_tracking.path
    #node_records = load_data(node_tracking_file)
    #study_row = [['study',study_name,study_node_id,
    #              'project','iHMP',project_node_id]]
    #values = values_to_node_dict(study_row)

    """ Subject node """
    # from nodes import subject
    # subject_nodes = subject.submit(study_name, study_node_id,
    #        settings.NodeDataFiles['Subject'])

    """ Visit node """
    # from nodes import visit
    # visit_nodes = visit.submit(
    #        settings.NodeDataFiles['Visit'])
    # """ Sample node """
    # from nodes import sample
    # sample_nodes = sample.submit(
    #        settings.NodeDataFiles['Sample'])

    # """ 16S DNA Prep node """
    # from nodes import r16sDnaPrep
    # r16_nodes = r16sDnaPrep.submit(
    #        settings.NodeDataFiles['r16sDnaPrep'])
    # from nodes import r16sRawSeqSet
    # raw_nodes = r16sRawSeqSet.submit(
    #        settings.NodeDataFiles['r16sRawSeqs'])
    from nodes import r16sTrimSeqSet
    trim_nodes = r16sTrimSeqSet.submit(
           settings.NodeDataFiles['r16sTrimSeqs'])

    """ WGS DNA Prep node """
    # from nodes import WgsDnaPrep
    # wgs_dna_nodes = WgsDnaPrep.submit(
    #       settings.NodeDataFiles['WgsDnaPrep'])
    """ WGS Raw Sequence Set node """
    # from nodes import WgsRawSeqSet
    # wgs_raw_nodes = WgsRawSeqSet.submit(
    #        settings.NodeDataFiles['WgsRawSeqs'])

    """ RNA Prep node """
    # from nodes import WgsDnaPrep
    # wgs_nodes = WgsDnaPrep.submit(
    #        settings.NodeDataFiles['RnaPrep'])
    """ RNASeq Raw Sequence Set node """
    # from nodes import MicrobTranscriptomicsRaw
    # rnaraw_nodes = MicrobTranscriptomicsRaw.submit(
    #        settings.NodeDataFiles['MicrobRnaRaw'])


if __name__ == '__main__':
    sys.exit(main())
