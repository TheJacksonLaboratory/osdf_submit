#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Check status of all nodes submitted to the OSDF server,
determine parent-child hierarchy, detect any missing
or mis-linkage to incorrect node.
"""

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~
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
    logFormat="%(asctime)s %(levelname)5s: %(funcName)15s: %(message)s"

    logging.basicConfig(format=logFormat)
    logger = logging.getLogger(logname)
    logger.setLevel(loglevel)

    formatter = logging.Formatter(logFormat)

    fh = logging.FileHandler(logfile, mode='a')
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
log = log_it()


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~

def get_field_header(csv_file):
    """returns first row of csv file as list of fieldnames"""
    log.info('Loading fields from %s', csv_file)
    with open(csv_file) as csvfh:
        try:
            reader = csv.DictReader(csvfh)
            return reader.fieldnames
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                    csv_file, reader.line_num, e)

def csv_type_sniff(csv_file):
    """find the line/ending type using csv.sniffer"""
    try:
        with open(csv_file, 'rb') as f:
            dialect = csv.Sniffer().sniff(f.read(1024))
            return dialect
    except Exception as e:
        raise e

def load_data(csv_file):
    """yield row dicts from csv_file using DictReader
    """
    log.info('Loading rows from %s', csv_file)
    with open(csv_file, 'U') as csvfh:
        reader = csv.DictReader(csvfh)
        # log.debug('csv dictreader opened')
        try:
            for row in reader:
                # log.debug(row)
                yield row
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                          csv_file, reader.line_num, e)

def write_out_csv(csv_file, fieldnames, values=[]):
    """write all values in csv format to outfile.
    Values is list of dicts w/ keys matching fieldnames.
    To write header to file, omit `values`
    """
    log.info('Writing csv to %s', csv_file)
    try:
        with open(csv_file, 'a') as csvout:
            writer = csv.DictWriter(csvout, fieldnames)
            if values:
                try:
                    for row in values:
                        if isinstance(row, dict):
                            # log.debug(row)
                            writer.writerow(row)
                except Exception as e:
                    log.exception('Writing CSV file %s, %s', csv_file, str(e))
                    raise e
            else:
                log.info('Writing header of fieldnames to %s', csv_file)
                writer.writeheader()
    except IOError as e:
        raise e

def write_csv_headers(filenames,fieldnames):
    """init csv files with fieldname headers"""
    [write_out_csv(name, fieldnames=fieldnames)
     for name in filenames
     if not os.path.exists(name)]

def import_whole_csv(filename):
    """loads data from concatenated sample sheet of all HMP2 samples """
    csv_data = []
    # csv_dialect = csv_type_sniff(filename)
    # csv_data = load_data(filename)
    for row in load_data(filename):
        csv_data.append(row)
    return csv_data


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ In the Mainline ~~~~~
def run_tests(test_type='all'):
    # csv_dialect = csv_type_sniff(filename)
    """run tests of all usage scenarios"""
    csv_file = 'tmp_file.csv'
    fieldname_list = ['foo', 'bar', 'spam']
    value_dict = dict(foo=1, bar=2, spam=123)
    assert write_out_csv(csv_file, fieldname_list), \
           log.error('write_out_csv headers test failed!')

    assert write_out_csv(csv_file, fieldname_list, \
                         values=[value_dict, value_dict]), \
           log.error('write_out_csv test failed!')

    assert load_data(csv_file), \
           log.error('load_data test failed!')

    pass

def main(args):
    """manage imports, cross-matches"""

    # import dicts from all files
    # sample_sheets = import_whole_csv(args.sample_sheet)
    master_sample = import_whole_csv(args.master_samples)
    # changed_names = import_whole_csv(args.master_changed)
    # jaxid_db_list = import_whole_csv(args.jaxid_list)
    visit_numbers = import_whole_csv(args.visit_numbers)
    log.info('Done loading files')

    new_master_file = args.master_samples[0:-4] + '_new.csv'
    # log.debug('new_master_file: %s', new_master_file)

    ### pre-coalesce, ensure all sample jaxid's in master list for comparison
    # log.info('mod\'ing master list pre-coalescing')

    # write_csv_headers([new_master_file],fieldnames=args.fieldnames.master_list)
    # matched_total = add_jaxid_to_master_list(jaxid_db_list,
    #                                            master_sample,
    #                                            new_master_file)

    # add_visit_id_to_master_list(visit_numbers, master_sample,
    #                             args.master_samples, new_master_file)

    add_visit_id_to_master_list_old_name(visit_numbers, master_sample,
                                         args.master_samples, new_master_file)

    ### cross-matching
    # TODO: generate logic for cross-matching, start in pseudo code



if __name__ == '__main__':
    # TODO: run_tests()

    filepath = '/Users/bleopold/osdf/ipop_osdf/coalesce/'
    class args(object):
        sample_sheet = filepath + 'sample_sheets_0713-concat.csv'
        jaxid_list = filepath + 'JAXId-DB-List-2016-07-05-HMP2.csv'
        master_samples = filepath + 'HMP2_metadata-MasterSampleSheet_0713_new_new.csv'
        master_changed = filepath + 'samples_0713_subset_changed.csv'
        visit_numbers = filepath + 'HMPs_wenyu-visit-list.csv'

        class fieldnames:
            jaxid_db = ['id','project_code','creation_date','collab_id', 'collab_id_old',
                        'sample','nucleic_acid','seq_type','jaxid','parent_id',
                        'id_type','entered_into_lims','external_data','notes']

            osdf_node_list = ['node_type','id','internal_id','linkage','meta',
                              'ns','ver','acl','date_retrieved']
            final = []


    sys.exit(main(args))

