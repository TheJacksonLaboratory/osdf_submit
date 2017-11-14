#!/usr/bin/env python

import sys

from cutlass import iHMPSession

from settings import auth
from settings import NodeUpdateFiles
from cutlass_utils import log_it
from cutlass_search import update_nodes, delete_nodes, update_nodes_general

log = log_it('jax_osdf_update')
log.info('Starting metadata submission to OSDF server.')

# load username, password from files
dcc_user = auth.dcc_user
dcc_pass = auth.dcc_pass
session = iHMPSession(dcc_user, dcc_pass, ssl=False)
log.info('Loaded session: {}'.format(session.get_session()))
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Main Actions ~~~~~


def main():
    """make it happen!"""

    # study_name = 'prediabetes'
    # study_node_id = '194149ed5273e3f94fc60a9ba58f7c24'

    """ Subject node """
    # update_nodes(session, NodeUpdateFiles['Subject'], 'subject')
    """ Visit node """
    # update_nodes(session, NodeUpdateFiles['Visit'], 'visit')
    """ Sample node """
    # update_nodes(session, NodeUpdateFiles['Sample'], 'sample')

    """ 16S nodes """
    # update_nodes(session, NodeUpdateFiles['r16sDnaPrep'], '16s_dna_prep')
    # update_nodes(session, NodeUpdateFiles['r16sRawSeqs'], '16s_raw_seq_set')
    # update_nodes(session, NodeUpdateFiles['r16sTrimSeqs'], '16s_trimmed_seq_set')

    """ WGS DNA Prep node """
    # update_nodes(session, NodeUpdateFiles['WgsDnaPrep'], 'wgs_dna_prep')
    """ WGS Raw Sequence Set node """
    # update_nodes(session, NodeUpdateFiles['WgsRawSeqs'], 'wgs_raw_seq_set')

    """ RNA Prep node """
    # update_nodes(session, NodeUpdateFiles['RnaPrep'], 'wgs_dna_prep')
    """ RNASeq Raw Sequence Set node """
    # update_nodes(session, NodeUpdateFiles['MicrobRnaRaw'], 'microb_transcriptomics_raw_seq_set')


    """ Arbitrary """
    # update_nodes(session, 'osdf_node_records/20170309_update_r16sPreps_4.csv', 'multiple')
    # update_nodes(session, 'osdf_node_records/20170309_update_r16sPreps_4.csv', 'multiple')
    update_nodes_general(session, NodeUpdateFiles['general'])

    """ Deletions """
    from osdf_delete_ids import node_ids
    # delete_nodes(session, node_ids)

if __name__ == '__main__':
    sys.exit(main())
