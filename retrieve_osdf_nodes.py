#!/usr/bin/env python

import sys

from cutlass import iHMPSession

from settings import auth
from settings import NodeRetrievalFiles as NodeDataFiles
from cutlass_utils import log_it
from cutlass_search import retrieve_nodes

log = log_it('jax_osdf_submit')
log.info('Starting metadata submission to OSDF server.')

# load username, password from files
dcc_user = auth.dcc_user
dcc_pass = auth.dcc_pass
session = iHMPSession(dcc_user, dcc_pass)
log.info('Loaded session: {}'.format(session.get_session()))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Main Actions ~~~~~

def main():
    """make it happen!"""

    # study_name = 'prediabetes'
    # study_node_id = '194149ed5273e3f94fc60a9ba58f7c24'

    """ Subject node """
    retrieve_nodes(session, NodeDataFiles['Subject'], 'subject')

    """ Vimportisit node """
    retrieve_nodes(session, NodeDataFiles['Visit'], 'visit')
    """ Sample node """
    retrieve_nodes(session, NodeDataFiles['Sample'], 'sample')

    """ 16S nodes """
    retrieve_nodes(session, NodeDataFiles['r16sDnaPrep'], '16s_dna_prep')
    retrieve_nodes(session, NodeDataFiles['r16sRawSeqs'], '16s_raw_seq_set')
    retrieve_nodes(session, NodeDataFiles['r16sTrimSeqs'], '16s_trimmed_seq_set')

    """ WGS DNA Prep node """
    retrieve_nodes(session, NodeDataFiles['WgsDnaPrep'], 'wgs_dna_prep')
    """ WGS Raw Sequence Set node """
    retrieve_nodes(session, NodeDataFiles['WgsRawSeqs'], 'wgs_raw_seq_set')

    """ RNA Prep node """
    # grep wgs_dna_prep set above for '_R_':
    # retrieve_nodes(session, NodeDataFiles['RnaPrep'], 'wgs_dna_prep')
    """ RNASeq Raw Sequence Set node """
    retrieve_nodes(session, NodeDataFiles['MicrobRnaRaw'], 'microb_transcriptomics_raw_seq_set')


if __name__ == '__main__':
    sys.exit(main())
