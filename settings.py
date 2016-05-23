""" Settings, Variables
    for Stanford/JAXGM iHMP OSDF submissions
"""

class auth():
    dcc_user = 'bleopold'
    dcc_pass = 'undrogin15'

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
        'Project':          './data_files/project_info.yaml',
        'Study':            './data_files/study_info.yaml',
        # 'Subject':          './data_files/subjects_20160510.csv',
        'Subject':          './data_files/subjects_20160520.csv',
        # 'Visit':            './data_files/visits_20160511.csv',
        # 'Visit':            './data_files/visits_MISSING_subjects_20160512.csv',
        'Visit':            './data_files/visits_20160520.csv',
        # 'Sample':           './data_files/samples_20160520.csv',
        'Sample':           './data_files/samples_20160521.csv',
        #
        'r16sSDnaPrep':     './data_files/16S_dna_prep.2016.....csv',
        'r16sSRawSeqs':     './data_files/16s_raw_seq_set.2016.....csv',
        'r16sSTrimSeqs':    './data_files/16s_trim_set.2016.....csv',
        'WgsDnaPrep':       './data_files/wgs_dna_prep.2016.....csv',
        'WgsRawSeqs':       './data_files/wgs_raw_seq_set.2016.....csv',
        }

