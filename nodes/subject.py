#!/usr/bin/env python

import os, logging
logging.basicConfig(level=logging.DEBUG,
        format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(name=os.path.basename(__file__))

# from cutlass_utils import load_config_from_file
from cutlass.Subject import Subject

from cutlass_utils import \
    load_csv_rows_dict, \
    load_yaml_data as load_data, \



class node_values:
    rand_subject_id = ''
    race = ''
    gender = ''
    tags = ''

# Race decoding
race_code_map = {
        'B': 'black_or_african_american',  # needs osdf json update!!!
        'C': 'caucasian',
        'A': 'asian',
        'H': 'hispanic_or_latino',
        'O': 'ethnic_other',
         '': 'ethnic_other'
        }

def get_race(race_code):
    if not race_code:
        race_code = 'O'
    if race_code_map.get(race_code) not in Subject.valid_races:
        raise ValueError("Race value is not valid! Choose from {}".format(
            str(Subject.valid_races)))
    return race_code_map.get(race_code)

# validate and create
def check_record(record):
    s = Subject()
    s.race = get_race(record.race)
    s.rand_subject_id = record.subject_id
    s.gender = record.gender
    s.links = 
    s.tags = {}
    s.tags['age'] = record.age
    return s

# load data from file, convert to list of records
def from_file(filename,parent_node):
    lines = []
    with open(filename, 'r') as fh:
        lines.append(check_record(fh.readline))
    return lines



node_records = load_csv_rows_dict(node_tracking_file)
for node_info in load_data(NodeDataFiles['Subject']):
    study_node_id = match_records(node_records,
                                  study.node_values.name,
                                  'internal_id'
                                  )[0]
    subject_node_id = submit_subject(node_info, study_node_id)
    #pseudocode:
    # write_out_csv 'study_id, subject_id, subject_node_id'
    # later retrieval of multiple lower levels
    values=['subject',subject.node_values.rand_subject_id,subject_node_id
            'study',study.node_values.name,study_node_id
            ]
    write_out_csv(node_tracking_file,values)

