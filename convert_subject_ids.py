#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import os,sys,re,csv

# generator of rows in csv file
def yield_rows(csv_file):
    print('Loading rows from {}'.format(csv_file))
    with open(csv_file) as csvfh:
        reader = csv.DictReader(csvfh)
        print('csv dictreader opened')
        try:
            for row in reader:
                # print(row)
                yield row
        except csv.Error as e:
            sys.exit('CSV file %s, line %d: %s'.format(\
                    csv_file, reader.line_num, e))


def write_out_csv(csv_file,fieldnames=[],values=[]):
    """ write all values in csv format to outfile """
    print('writing out csv...')
    if values[0] is not None:
        # print('value exists')
        try:
            # print('trying')
            with open(csv_file, 'a') as csvout:
                # print('opened')
                writer = csv.DictWriter(csvout,fieldnames)
                # print('writer')
                try:
                    # print('trying inner')
                    # print('write_out_csv values: '+str(values))
                    for row in values:
                        # print('values row: '+row)
                        if isinstance(row,dict):
                            # print(row)
                            writer.writerow(row)
                except Exception, e:
                    sys.exit(e)
                    # sys.exit('Error?!  CSV file %s, %s'.format(
                            # csv_file, str(e)))
        except IOError, e:
            raise e


def values_to_node_dict(values=[],keynames=[]):
    from collections import OrderedDict
    """pass list of lists of values and list of keys of desired dict
       This converts to list of dicts
    """
    print('In values_to_node_dict')
    final_list = []

    key_dict = OrderedDict()
    for key in keynames:
        key_dict[key] = ''

    for vals in values:
        l = vals
        d = key_dict.copy()
        k = d.keys()
        for x in range(len(d)):
            lx = l[x] if len(l) > x and l[x] is not None else ''
            d[k[x]] = lx
        # print(d)
        final_list.append(d)

    return final_list
# usage: values_to_node_dict([['foo','bar','1'],['spam','eggs','2','spoof']])


def convert_subject_samples():
    """convert ids in sample list"""

    old_fields = ['old_subject_id','old_sample_name']
    ref_fields = ['subject_id_orig','rand_subject_id','consented']
    new_fields = ['old_subject_id','rand_subject_id',
                  'old_sample_name','sample_name',
                  'consented']

    old_file = 'data_files/old_subj_sampl.csv'
    ref_file = 'data_files/new_subj_ids.csv'
    new_file = 'data_files/new_subj_samp_consents.csv'

    for old in yield_rows(old_file):
        old_subject = old['old_subject_id']
        old_sample  = old['old_sample_name']
        print(old_subject+', '+old_sample+'. ', end='\n')
        matched = []
        unmatched = []
        refunmatched = []
        for ref in yield_rows(ref_file):
            ref_subject = ref['subject_id_orig']
            ref_rand_id = ref['rand_subject_id']
            consented   = ref['consented'] if ref['consented'] else 'NO'
            # print(ref_subject+', '+ref_rand_id+', '+consented, end='.  ')
            print('.', end='')
            if ref_subject == old_subject:
                new_sample_name = re.sub(old_subject,ref_rand_id,old_sample)
                matched.append([ref_subject, ref_rand_id,
                                old_sample, new_sample_name, consented])
                # print(len(matched))
            else:
                # unmatched.append([old_subject, old_sample])
                unmatched.append([old_subject, 'missing',
                                old_sample, '', ''])

        print() # add newline to above conintuous '.' output
        print('how many matched? '+str(len(matched)))
        if len(matched) > 0:
            write_out_csv(new_file,new_fields,values_to_node_dict(matched,new_fields))
        if len(matched) == 0:
            unmatched.append([ref_subject, ref_rand_id, consented])
            write_out_csv(new_file+'.unmatched',new_fields,
                    values_to_node_dict(unmatched))

def convert_visit_subject_tags():
    """convert ids in visit list"""

    old_file   = 'data_files/subj_visits.csv'
    old_fields = ['subject_id']

    ref_file   = 'data_files/new_subj_ids.csv'
    ref_fields = ['subject_id_orig','rand_subject_id','consented']

    new_file   = 'data_files/subj_visit_tags_new.csv'
    new_fields = ['old_subject_id','rand_subject_id']

    for old in yield_rows(old_file):
        old_subject = old['subject_id']
        matched = []
        unmatched = []
        for ref in yield_rows(ref_file):
            ref_subject = ref['subject_id_orig']
            ref_rand_id = ref['rand_subject_id']
            # print(ref_subject+', '+ref_rand_id+', '+consented, end='.  ')
            print('.', end='')
            if ref_subject == old_subject:
                matched.append([ref_subject, ref_rand_id])
            else:
                # unmatched.append([old_subject, old_sample])
                unmatched.append([old_subject, 'missing'])

        print() # add newline to above conintuous '.' output
        print('how many matched? '+str(len(matched)))
        if len(matched) > 0:
            write_out_csv(new_file,new_fields,values_to_node_dict(matched,new_fields))
        if len(matched) == 0:
            # unmatched.append([ref_subject, ref_rand_id, consented])
            write_out_csv(new_file+'.unmatched',new_fields,
                    values_to_node_dict(unmatched))

def add_consent_to_visits():
    """add 'consented' to visit list"""

    vis_file   = 'data_files/20160608-HMP2_metadata-visits_jaxgm.csv'
    vis_fields = [ 'visit_id','rand_subject_id',
            'consented','visit_number','interval',
            'old_subject_id','old_visit_id','study_date',
            'microbiome_note' ]

    ref_file   = 'data_files/subject_id_convert.csv'
    ref_fields = ['subject_id_orig','rand_subject_id','consented']

    new_file   = 'data_files/visit_consent_check.csv'
    new_fields = vis_fields

    # WOAH this does wierd things i don't want to bother looking at further!
    # write_out_csv(new_file,new_fields,
            # values_to_node_dict(new_fields,new_fields))

    for old in yield_rows(vis_file):
        vis_subject = old['rand_subject_id']
        matched = []
        unmatched = []
        for ref in yield_rows(ref_file):
            ref_rand_id = ref['rand_subject_id']
            ref_consent = ref['consented']
            # print(ref_rand_id+', '+ref_rand_id+', '+consented, end='.  ')
            print('.', end='')
            if ref_rand_id == vis_subject:
                matched.append([
                        old['visit_id'],
                        old['rand_subject_id'],
                        ref['consented'],
                        old['visit_number'],
                        old['interval'],
                        old['old_subject_id'],
                        old['old_visit_id'],
                        old['study_date'],
                        old['microbiome_note']
                    ])

            else:
                # unmatched.append([vis_subject, vis_sample])
                unmatched.append([vis_subject, 'missing'])

        print() # add newline to above conintuous '.' output
        print('how many matched? '+str(len(matched)))
        if len(matched) > 0:
            write_out_csv(new_file,new_fields,
                    values_to_node_dict(matched,new_fields))
        if len(matched) == 0:
            # unmatched.append([ref_rand_id, ref_rand_id, consented])
            write_out_csv(new_file+'.unmatched',new_fields,
                    values_to_node_dict(unmatched))


def main():
    """ main: run a specific file id conversion """
    # convert_subject_samples()
    # convert_visit_subject_tags()
    add_consent_to_visits()


if __name__ == '__main__':
    sys.exit(main())
