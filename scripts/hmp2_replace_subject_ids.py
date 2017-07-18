#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: hmp2_replace_subject_ids.py
Author: Benjamin Leopold
Date: 2016-06-28T11:18:17
Description: Uses 2-column mapping csv file for orig->new IDs, replaces
             occurrences of old ids in a csv file passed as param 1.
"""

import sys
import os
import re
import csv

import logging
import time

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Constants ~~~~~
COOLNESS = True

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~
# Log It!
def log_it(logname=os.path.basename(__file__)):
    """log_it setup"""
    curtime = time.strftime("%Y%m%d-%H%M")
    logfile = '{}_{}.log'.format(curtime, logname)

    loglevel = logging.DEBUG
    # logFormat="%(asctime)s %(levelname)5s: %(funcName)15s: %(message)s"
    logFormat="%(asctime)s %(levelname)5s: %(message)s"

    logging.basicConfig(format=logFormat)
    logger = logging.getLogger(logname)
    logger.setLevel(loglevel)

    formatter = logging.Formatter(logFormat)

    fh = logging.FileHandler(logfile, mode='a')
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
log = log_it('new_subject_id_change')


def yield_csv_data(csv_file):
    """yield row dicts from csv_file using DictReader
    """
    # log.info('Loading rows from {}'.format(csv_file))
    with open(csv_file,'U') as csvfh:
        reader = csv.DictReader(csvfh,dialect='excel')
        # log.debug('csv dictreader opened')
        try:
            for row in reader:
                # log.debug(row)
                yield row
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                          csv_file, reader.line_num, e)


def write_out_csv(csv_file,fieldnames,values=[]):
    """write all values in csv format to outfile.
    Values is list of dicts w/ keys matching fieldnames.
    To write header to file, omit `values`
    """
    log.info('Writing csv to {}'.format(csv_file))
    try:
        with open(csv_file, 'a') as csvout:
            writer = csv.DictWriter(csvout, fieldnames)
            if values:
                try:
                    for row in values:
                        if isinstance(row, dict):
                            log.debug(row)
                            writer.writerow(row)
                except Exception, e:
                    log.exception('Writing CSV file %s, %s', csv_file, str(e))
                    raise e
            else:
                log.info('Writing header of fieldnames to {}'.format(csv_file))
                writer.writeheader()
    except IOError, e:
        raise e


def get_field_header(csv_file):
    """returns first row of csv file as list of fieldnames"""
    log.info('Loading fields from {}'.format(csv_file))
    with open(csv_file,'U') as csvfh:
        try:
            reader = csv.DictReader(csvfh)
            return reader.fieldnames
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                    csv_file, reader.line_num, e)


def build_id_ref_map(ref_csv):
    """construct reference mapping dict of ids from csv file"""
    ref_map = {}
    for row in yield_csv_data(ref_csv):
        ref_map[row['orig_subject_id']] = row['new_subject_id']
    return ref_map


def replace_ids(ref_map_file,csv_mod_file,match_field='old_sample_name'):
    """replace all matching id strings in csv_mod_file, using ref_map_file
       dict for reference."""
    csv_field_names = get_field_header(csv_mod_file)
    new_field_name = 'Random ' + match_field

    # insert new column just after matched column
    csv_field_names.insert(csv_field_names.index(match_field)+1,new_field_name)
    # insert extra string into filename
    new_csv_file = re.sub('.csv$','_newIDS.csv',csv_mod_file)
    # init empty list for new_csv_file
    new_rows = []
    # write field_headers to new csv file
    write_out_csv(new_csv_file, csv_field_names)

    # create dict from ref_map_file
    ref_dict = build_id_ref_map(ref_map_file)

    for row in yield_csv_data(csv_mod_file):
        """search row[match_field] for match with key to orig id
           and insert new column with matching new id or empty string
        """
        match_id = row[match_field]

        for old_id,new_id in ref_dict.items():
            old_patt = re.subn('-','[-_]',old_id)[0]
            # log.info('--> old_patt %s, match_id %s', old_patt, match_id)
            if re.search(old_patt,match_id):
                # log.info('--> Match %s == %s', old_patt, match_id)
                row[new_field_name] = re.subn(old_patt,new_id,match_id,count=1)[0]

        new_rows.append(row)

    write_out_csv(new_csv_file, csv_field_names, new_rows)


if __name__ == '__main__':
    """ This script will read in a reference mapping file with new subject ids,
        then will search in a specified csv file for Original ids, add in a new
        column to hold the New ids.  All to be inspected visually afterwards.

        Must be run from within the directory containing the id mapping csv!
    """
    # default_match_field = 'sheet_sample_name'
    default_match_field = 'Final Sample Name'

    if len(sys.argv)<2:
        log.error("Usage: %s csv_file_name_to_modify.csv\n"
              "NOTE: this presumes field_name_to_match == %s",
              os.path.basename(sys.argv[0]),
              default_match_field)
        sys.exit(1)
    else:
        csv_file = sys.argv[1] # presume first arg is file to search/modify
        map_file = 'hmp2_stanford_subject_id_replacements.csv'

    replace_ids(map_file,csv_file,match_field=default_match_field)
