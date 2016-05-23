#!/usr/bin/env python

import os
import json

from cutlass import iHMPSession

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, \
        list_tags, format_query, \
        values_to_node_dict, write_out_csv, \
        log_it

# Log It!
import os, logging # needed for LogIt function!
def log_it(filename=os.path.basename(__file__)):
    """log_it setup"""
    logging.basicConfig(level=logging.DEBUG,
        format="%(asctime)s %(levelname)5s %(filename)15s: %(message)s")
    return logging.getLogger(filename)
log = log_it()
log.info('Starting metadata submission to OSDF server.')

# load username, password from files
dcc_user = settings.auth.dcc_user
dcc_pass = settings.auth.dcc_pass
session = iHMPSession(dcc_user, dcc_pass)
log.info('Loaded session: {}'.format(session.get_session()))




#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Main Actions ~~~~~

def main():
    """make it happen!"""

    """ Project node """
    # Predefined_id_from_osdf_for_overall_iHMP_project
    project_name = 'iHMP'
    project_node_id = '3fffbefb34d749c629dc9d147b18e893'

    """ Study node """
    # from nodes import study
    # study = study.get_study(project_node_id)
    # study_node_id = study.id if study else ''
    # study_name = study.name if study else ''
    # log.debug('Study.id: "{}"'.format(str(study_node_id)))
    # if not study_node_id:
    #     sys.exit('Saving study record failed!')

    study_name = 'prediabetes'
    study_node_id = '194149ed5273e3f94fc60a9ba58f7c24'

    # node_tracking_file = settings.node_id_tracking.path
    # node_records = load_data(node_tracking_file)
    # study_row = [['study',study_name,study_node_id,
    #               'project','iHMP',project_node_id]]
    # values = values_to_node_dict(study_row)
    # write_out_csv(node_tracking_file,values=values)

    """ Subject node """
    from nodes import subject
    subject_nodes = subject.submit(study_name, study_node_id,
            settings.NodeDataFiles['Subject'])
    # """89 subjects submitted...""""


    """ Visit node """
    from nodes import visit
    visit_nodes = visit.submit(settings.NodeDataFiles['Visit'])

    """ Sample node """
    from nodes import sample
    sample_nodes = sample.submit(settings.NodeDataFiles['Sample'])

    """ 16S DNA Prep node """
    # from nodes.r16s import dna_prep
    # dna_prep_nodes = dna_prep.submit(settings.NodeDataFiles['r16sSDnaPrep'])
    """ 16S Raw Sequence Set node """
    # from nodes.r16s import raw_seq
    # raw_seq_nodes = raw_seq.submit(settings.NodeDataFiles['r16sSRawSeqs'])
    """ 16S Trimmed Sequence Set node """
    # from nodes.r16s import trimmed_seq
    # trim_nodes = trim_seq.submit(settings.NodeDataFiles['r16sSTrimSeqs'])
    """ WGS DNA Prep node """
    # from nodes.wga import dna_prep
    # sample_nodes = dna_prep.submit(settings.NodeDataFiles['WgsDnaPrep'])
    """ WGS Raw Sequence Set node """
    # from nodes.wgs import raw_seq
    # raw_seq_nodes = raw_seq.submit(settings.NodeDataFiles['WgsRawSeqs'])


    """ load 16S DNA Prep into OSDF using info from data file """
    #'prepared_from'
    """ load 16S Raw Sequence Set into OSDF using info from data file """
    #'sequenced_from'
    """ load 16S Trimmed Sequence Set into OSDF using info from data file """
    #'computed_from'

    """ load WGS DNA Prep into OSDF using info from data file """
    #'prepared_from'
    """ load WGS DNA Raw Sequence Set into OSDF using info from data file """
    #'sequenced_from'


if __name__ == '__main__':
    import sys
    sys.exit(main())
