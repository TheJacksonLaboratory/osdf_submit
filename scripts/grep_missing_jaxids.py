#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Script to grep all non-submitted jaxids from pervious csv's
"""

"""
File: grep_missing_jaxids.py
Author: Benjamin Leopold
Date: 2017-02-13 21:00:20-0500
"""

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~~
import sys
import os
import re
import csv
import logging
import time
import shutil

"""Subprocess module wrappers"""
from subprocess import call, check_output
from subprocess import STDOUT #, STD_ERROR_HANDLE as STDERR
from subprocess import CalledProcessError as SubprocCallError

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
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # root = logging.getLogger(format=logFormat)
    # root.setLevel(loglevel)
    # root.addHandler(fh)

    return logger

log = log_it('dcc_submission_file_name_change')

def get_output(cmd_string, stderr=STDOUT, shell=True,
        universal_newlines=True, **kwargs):
    """wrapper for subprocess.call; takes single or list as arg"""
    return check_output(cmd_string, stderr=stderr, shell=shell,
                        universal_newlines=universal_newlines, **kwargs)

def yield_csv_data(csv_file):
    """yield row dicts from csv_file using DictReader
    """
    log.info('Loading rows from {}'.format(csv_file))
    with open(csv_file,'U') as csvfh:
        reader = csv.DictReader(csvfh)
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Make it Happen' ~~~~~

if __name__ == '__main__':
    """Mod main functions run called below as you need them (files need
       many steps, at different times...)
    """

    class args:
        # ...[ defaults ]...
        base_path = '/data/weinstocklab/projects/HMP2/submissions/dcc_osdf/submit_osdf/data_files/'
        data_path = '/data/weinstocklab/projects/HMP2/submissions/data/'
        renamed_path = 'renamed'

    # ...[ 16S ]...
    jaxid_missing_file = 'data_files/20170213_notsubmitted_jaxids-16S.csv'
    grep_file = 'data_files/20170202-samples_16S.csv'
    grep_file = 'data_files/20170202-samples_16S_merged.csv'
    # grep_file = 'data_files/20170202-samples_16S_merged_2.csv'
    # grep_file = 'data_files/20170202-samples_16S_merged_checksummed_nomatch.csv'

    # ...[ mwgs ]...
    # jaxid_missing_file = 'data_files/20170213_notsubmitted_jaxids-mwgs_preps.csv'
    # grep_file = 'data_files/20170206-samples_mWGS_merged.csv'

    # ...[ rnaseq ]...
    # jaxid_missing_file = 'data_files/20170213_notsubmitted_jaxids-rnaseq.csv'
    # grep_file = 'data_files/20170202-samples_rnaseq_nopreps.csv'
    # grep_file = 'data_files/20170202-samples_rnaseq_merged.csv'
    # grep_file = 'logs/'

    for idrow in yield_csv_data(jaxid_missing_file):
        jaxid = idrow['jaxid']
        collab_id = idrow['collab_id']
        grep_cmd = ' '.join(['echo -n', jaxid+",;",
                             'grep', '"'+jaxid+'"',
                             grep_file, '; exit 0'])
        # log.warning(get_output(grep_cmd))
        print(get_output(grep_cmd))
