#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
update all file names according to dict {before: after} of basenames
"""

__date__ = '2017-01-05'
__author__ = 'bleopold'
__license__ = 'GPL v3'

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~
import os
import sys
import re
import csv
import logging
import time

"""Subprocess module wrappers"""
from subprocess import call, check_output
from subprocess import STDOUT
from subprocess import CalledProcessError as SubprocCallError

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Constants ~~~~
COOLNESS = True

class settings:
    data_path = '/data/weinstocklab/projects/HMP2/submissions/data/'
    renamed_path = 'renamed/16S'

settings.mapping_file = os.path.join(settings.data_path, settings.renamed_path, '20170105_raw_name_updates_351.csv')
# settings.mapping_file = os.path.join(settings.data_path, settings.renamed_path, '20170105_clean_name_updates_351.csv')

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
log = log_it('r16s_file_name_change')

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

def rename_files(settings):
    """loop through mapping_file, subloop through files in curdir+row-subdir,
       rename matching filenames, convert compression formats, log
       checksums and file name/size to checksum_file
    """

    renamed_path = settings.renamed_path
    data_path = settings.data_path
    map_file = settings.mapping_file

    class summary:
        renamed = 0
        converted = 0
        errored = 0
        errfiles = []
        pending = 0
        pending_files = []

    for row in yield_csv_data(map_file):
        if row['original_file_base'] and row['dcc_file_base']: #row isn't empty
            # path = row['dir']
            srce = row['original_file_base']
            dest = row['dcc_file_base']
            try:
                filepath = os.path.join(data_path, renamed_path, row['dir'])
                # log.debug('__ filepath='+filepath)
                for file in os.listdir(filepath):
                    # log.debug('__ file=%s, %s', file, row['dir'])
                    if re.match(srce, file):
                        repl_sub = re.sub(srce, dest, file)
                        repl_path = os.path.join(data_path, renamed_path, row['dir'])
                        repl_file = os.path.join(repl_path, repl_sub)
                        file = os.path.join(filepath, file)
                        log.info('Moving "%s" to "%s"', file, repl_file)
                        try:
                            os.renames(file, repl_file)
                            try:
                                #TODO: check is_symlink before .st_size? (only if on same host)
                                if int(os.stat(repl_file).st_size) > 0:
                                    pass
                                    # os.chmod(repl_file, 0444) # not needed for links, only for new files
                                else:
                                    log.error('Error in "%s"!!! file: %s, except: %s',
                                              'file.stat', file, e)

                            except Exception, e:
                                log.error('Error in "%s"!!! file: %s, except: %s',
                                          'rename_files.stat+chmod', file, e)
                                # raise e
                        except Exception, e:
                            log.error('Error in "%s"!!! file: %s, except: %s',
                                      'rename_files.os-renames', file, e)
                            summary.errored += 1
                            summary.errfiles.append(srce)
                            # raise e

            except Exception as e:
                log.error('Error in "%s"!!! row: %s, except: %s', 'rename_files', row, e)
                summary.errored += 1
                summary.errfiles.append(srce)
                # continue to next, instead of `raise e`

    # summarize:
    log.error('-> Errors found when processing: %4s', str(summary.errored))
    if summary.errored:
        log.error('  -> For specifics on these files that errored, '
                  'please see the logfile.')
        log.error('  -> Files: %s', str(summary.errfiles))

def get_output(cmd_string, stderr=STDOUT, shell=True,
        universal_newlines=True, **kwargs):
    """wrapper for subprocess.call; takes single or list as arg"""
    return check_output(cmd_string, stderr=stderr, shell=shell,
                        universal_newlines=universal_newlines, **kwargs)

def generate_raw_tar(dest_path="./", file_prefix=""):
    """generate tar archives, return file name"""
    try:
        src_path = os.path.dirname(file_prefix)
        prefix = os.path.basename(file_prefix)
        os.chdir(src_path)
        tar_file = os.path.join(dest_path, file_prefix + '.raw.fastq.tar')
        tar_cmd = ' '.join(['tar', 'chf', tar_file, prefix+'*'])
        tar_stat = get_output(tar_cmd)

        log.info('Archive file created: %s', tar_file)
        return tar_file

    except Exception, e:
        log.error('Uh-Oh (generate_raw_tar)... %s', e)
        raise e

def archive_raw_fastq_files(settings):
    """ only run this func for raw fastq PE files!!
    """

    renamed_path = settings.renamed_path
    data_path = settings.data_path
    map_file = settings.mapping_file

    archived = 0
    errored = 0
    errfiles = []
    for row in yield_csv_data(map_file):
        if (re.search('raw', row['dir']) and
                row['dcc_file_base'] != ''):
            dest = row['dcc_file_base']
            dir = row['dir']
            try:
                file_path = os.path.join(data_path, renamed_path, dir)
                # log.debug('(%s)... file_path: %s', 'archive', file_path)
                pref = os.path.join(file_path, dest)
                tar_file = generate_raw_tar(file_path, file_prefix=pref)
                # archived += 1 #debug "+= 1" syntax errors

            except Exception, e:
                log.error('Error in "%s"!!!   %s', 'archive_raw_fastq_files', e)
                errfiles.append(dest)
    # summarize:
    # if errored:
    if True:
        log.error('-> For specifics on these files that errored, please see the logfile.')
        log.error('-> Files: %s', str(errfiles))


if __name__ == '__main__':
    """Mod main functions run called below as you need them (files need
       many steps, at different times...)
    """
    rename_files(settings)
    archive_raw_fastq_files(settings)
