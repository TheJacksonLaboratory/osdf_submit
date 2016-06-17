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

class data_node_ids():
        project = ''
        study = ''
        substudy = ''
        subject = []
        visit = []
        sample = []

class node_id_tracking:
    path = 'prediabetes_node_id_tracking.csv'
    id_fields = ['node_type', 'internal_id', 'osdf_node_id',
                 'parent_node_type', 'parent_id', 'parent_node_id']

# data file names
NodeDataFiles = {
        'Project':        './data_files/project_info.yaml',
        'Study':          './data_files/study_info.yaml',
        'Subject':        './data_files/20160608-HMP2_metadata-subjects.csv',
        'Visit':          './data_files/20160609-HMP2_metadata-visits_jaxgm.csv',
        'Sample':         './data_files/20160610-HMP2_metadata-samples-final.csv',
        #
        'r16sDnaPrep':    './data_files/20160615-HMP2_metadata-16S_dna_prep.csv',
        'r16sRawSeqs':    './data_files/20160617-HMP2_metadata-r16sRawSeqs.csv',
        'r16sRawSeqs':    './data_files/20160617-HMP2_metadata-r16sRawSeqs2.csv',
        'r16sTrimSeqs':   './data_files/20160615-HMP2_metadata-16sTrimmedSeqs.csv',
        #
        'WgsDnaPrep':     './data_files/wgs_dna_prep.2016.....csv',
        'WgsRawSeqs':     './data_files/wgs_raw_seq_set.2016.....csv',
        }


class node_hierarchy:
  """node_hierarchy lists nodes in hierarchical tree"""
  node_tree = [
    'project', [
      'subject', [
        'visit', [
          'sample',
            [ 'r16sDnaPrep',
              'r16sRawSeqSet',[
                 'r16sTrimSeqSet', ] ],
            [ 'wgsDnaPrep', [
              'wgsRawSeqSet',[
                'wgsAssembledSeqSet', ] ],
            ['hostAssay', 'etc',],
          ]
        ]
      ]
    ]
  ]


if __name__ == '__main__':
    # from pprint import pprint
    # print('nodes')
    # n = node_hierarchy()
    # pprint(n.node_tree)

    pass
