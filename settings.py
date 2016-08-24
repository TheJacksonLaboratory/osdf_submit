""" Settings, Variables
    for Stanford/JAXGM iHMP OSDF submissions
"""
import os
import yaml


def load_string_from_file(filename):
    with open(os.path.join(os.curdir, filename)) as f:
        return f.read().strip()

class auth():
    dcc_user = load_string_from_file('auth/username.txt')
    dcc_pass = load_string_from_file('auth/password.txt')

class consented_users():
    try:
        list_of_consented_participants = load_string_from_file('auth/CONSENTED_MAP.sec')
    except Exception, e:
        list_of_consented_participants = ""
    

class data_node_ids():
        project = ''
        study = ''
        substudy = ''
        subject = []
        visit = []
        sample = []

class node_id_tracking:
    path = './data_files/prediabetes_node_id_tracking.csv'
    id_fields = ['node_type', 'internal_id', 'osdf_node_id',
                 'parent_node_type', 'parent_id', 'parent_node_id']

# data file names
NodeDataFiles = {
        'Project':      './data_files/project_info.yaml',
        'Study':        './data_files/study_info.yaml',
        'Subject':      './data_files/20160608-HMP2_metadata-subjects.csv',
        # 'Visit':        './data_files/20160609-HMP2_metadata-visits_jaxgm.csv',
        # 'Sample':       './data_files/20160610-HMP2_metadata-samples-final.csv',
        # 'Visit':        './data_files/20160623-HMP2_metadata-visits-ZOZOW1T.csv',
        # 'Sample':       './data_files/20160623-HMP2_metadata-samples-ZOZOW1T.csv',
        #
        'Visit':        './data_files/20160708_HMP2_metadata-visits_jaxgm.csv',
        #
        'Sample':       './data_files/20160711_HMP2_metadata-r16sSamples_13J_seqd.csv',
        'r16sDnaPrep':  './data_files/20160708_HMP2_metadata-r16sDnaPrep_1st6.csv',
        'r16sRawSeqs':  './data_files/20160708_HMP2_metadata-r16sRaw_1st6.csv',
        'r16sTrimSeqs': './data_files/20160708_HMP2_metadata-r16sTrim_1st6.csv',
        #
        'Sample':       './data_files/HMP2_metadata-MasterSampleSheet_0801_new_jaxids-visits_20160804.csv',
        #
        'WgsDnaPrep':   './data_files/dnaPrep_mwgs_0801_20160808_merged_ZOZOW1T.csv',
        'WgsRawSeqs':   './data_files/20160824_mwgs_raw_ready.csv',
        #
        'metabolome':   './data_files/metabolome.csv',
        'hostTranscriptomicsRawSeqSet':   './data_files/host_transcriptome_raw_seq_set.csv',
        'hostSeqPrep' : './data_files/host_seq_prep.csv',
        'proteome' : './data_files/proteome.csv',

        }


class node_hierarchy:
  """node_hierarchy lists nodes in hierarchical tree
     => project.study.subject.visit.sample.[16S,wgs,rnaseq,host,genome,...]
        16S[dnaprep,raw,trim]
        wgs[dnaprep,raw,assembled]
        rnaseq[dnaprep,raw,...]
        host[dnaprep,raw,...]
  """
  node_tree = [
    'project', [
      'study', [
        'subject', [
          'visit', [
            'sample',[
              [ 'r16sDnaPrep',[
                    'r16sRawSeqSet',[
                       'r16sTrimSeqSet',] ] ],
              [ 'wgsDnaPrep', [
                    'wgsRawSeqSet',[
                      'wgsAssembledSeqSet', ] ] ],
              ['hostAssay', 'etc',],
              ['metabolome', 'etc',],
              ['hostTranscriptomicsRawSeqSet', 'etc',],
              ['hostSeqPrep','etc',],
              ['proteome','etc',],
            ]
          ]
        ]
      ]
    ]
  ]


if __name__ == '__main__':
    from pprint import pprint
    print('nodes')
    n = node_hierarchy()
    pprint(n.node_tree)

    pass
