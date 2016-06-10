#!/usr/bin/env python
""" load Subject into OSDF using info from data file """

import os
import re

from cutlass.Subject import Subject

import settings
from cutlass_utils import \
        load_data, get_parent_node_id, \
        list_tags, format_query, \
        values_to_node_dict, write_out_csv, \
        log_it, dump_args

filename=os.path.basename(__file__)
log = log_it(filename)

node_type = 'subject'
parent_type = 'study'
node_tracking_file = settings.node_id_tracking.path

class node_values:
    rand_subject_id = ''
    race = ''
    gender = ''
    tags = ['age']

# Race decoding
race_code_map = {
        'B': 'african_american',
        #'B': 'black_or_african_american',  # needs osdf json update!!!
        'C': 'caucasian',
        'A': 'asian',
        'H': 'hispanic_or_latino',
        'O': 'ethnic_other',
         '': 'ethnic_other'
        }

@dump_args
def get_race(race_code=None):
    if not race_code:
        race_code = 'O'
    if race_code_map.get(race_code) not in Subject.valid_races:
        raise ValueError("Race value is not valid! Choose from {}".format(
            str(Subject.valid_races)))
    return race_code_map.get(race_code)

# Race decoding
gender_code_map = {
        'M': 'male',
        'F': 'female',
        'U': 'unknown',
         '': 'unknown'
        }

@dump_args
def get_gender(gender_code=None):
    """return gender as listed, or unknown, or map to code dict"""
    if not gender_code:
        gender_code = 'U'
    if gender_code in Subject.valid_genders:
        return gender_code
    elif gender_code_map.get(gender_code) not in Subject.valid_genders:
        raise ValueError("Gender value is not valid! Choose from {}".format(
            str(Subject.valid_gender)))
    return gender_code_map.get(gender_code)


@dump_args
def load(internal_id):
    """search for existing node to update, else create new"""
    try:
        # query = format_query(internal_id)
        # query = '("1636"[rand_subject_id] && "69"[rand_subject_id]) && "001"[rand_subject_id]'
            #, patt='-', field='rand_subject_id', mode='&&')
        # query = "1636_123_123_123[tags]"  # search with '-' still not functional! (ticket CUTLASS-003)
        query = '"{}"[rand_subject_id]'.format(internal_id)
        log.debug("query:"+query)
        s = Subject.search(query)
        for n in s:
            if n.rand_subject_id == internal_id:
                return Subject.load_subject(info)
        # no match, return empty node:
        n = Subject()
        log.debug('loaded node newbie...')
        return n
    except Exception, e:
        raise e


@dump_args
def validate_record(parent_id, node, record):
    """update record fields
       validate node
       if valid, save, if not, return false
    """
    log.debug("in validate/save: "+node_type)
    node.rand_subject_id = record['rand_subject_id']
    node.gender = get_gender(record['gender'])
    node.race = get_race(record['race_code'])
    node.tags = list_tags(node.tags,
                'test', # for debug!!
                'age: '+record['age'] if record['age'] else 'unk',
                'rand_subject_id: '+record['rand_subject_id'],
                'study: prediabetes',
                )
    node.links = {'participates_in':[parent_id]}
    if not node.is_valid():
        invalidities = node.validate()
        err_str = "Invalid!\n{}".format("\n".join(invalidities))
        log.error(err_str)
        raise Exception(err_str)
    elif node.save():
        return node
    else:
        return False


def submit(parent_name, parent_id, data_file,
        id_tracking_file=node_tracking_file):
    log.info('Starting submission of subjects.')
    nodes = []
    for record in load_data(data_file):
        try:
            log.debug('...trying next record...')
            # log.debug(record)
            n = load(record['rand_subject_id'])
            # n = Subject()
            if not n.rand_subject_id:
                saved = validate_record(parent_id, n, record)
                if saved:
                    header = settings.node_id_tracking.id_fields
                    vals = values_to_node_dict(
                            [[node_type,saved.rand_subject_id,saved.id,
                              parent_type,parent_name,parent_id]]
                            )
                    write_out_csv(id_tracking_file,
                            values=vals)
                    nodes.append(vals)
        except Exception, e:
            log.error(e)
            raise e
    return nodes


if __name__ == '__main__':
    pass
