#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Script to change and record all filenames from our original names at JAX to
   the newly-Stanford-privated names without subject-specific info; to
   publicize and upload to the DCC's OSDF, SRA, etc

   All filenames (original, final) are read from a CSV

   End filename structure:
        PROJ_Joriginal_Jlibrary_Tn_Bn_0000_SeqType_TissueSource_CollabID_RunID
"""

"""
File: dcc_submission_file_name_change.py
Author: Benjamin Leopold
Date: 2016-06-15T09:35:41

date 2016-12-07 16:10:13
Revised: slimmed down to only deal with cleaned data files.  Removed all extraneous function defs.
"""

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~~
import sys
import os
import re
import csv
import logging
import time

"""Subprocess module wrappers"""
from subprocess import call, check_output
from subprocess import STDOUT
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

def checksums(filename):
    """run md5 and sha256 checksums, return strings of two digests"""
    log.info('Running checksums on: '+filename)

    md5_cmd = ' '.join(['md5sum', filename])
    md5_out = get_output(md5_cmd, stderr=None)
    md5_str = re.split(' ', md5_out)[0]
    log.debug('checksum md5: '+md5_str)

    sha256_cmd = ' '.join(['sha256sum', filename])
    sha256_out = get_output(sha256_cmd, stderr=None)
    sha256_str = re.split(' ', sha256_out)[0]
    log.debug('checksum sha: '+sha256_str)

    return (md5_str, sha256_str)

def write_checksum_list(file_name, checksum_file):
    """generate list of file_name, md5sum and sha256sum
       then write file_name, file_size, md5sum, sha256sum values to csv outfile
    """
    log.info( '-> Beginning function %s', 'write_checksum_list')
    fields = ['local_file','size','format','md5','sha256']
    if not (os.path.exists(checksum_file) or os.path.getsize(checksum_file) >= 0):
        write_out_csv(checksum_file, fields) #write headers
    try:
        # log.debug( '-> Beginning try with: %s', file_name)
        if os.path.exists(file_name) and os.path.getsize(file_name) >= 0:
            # log.debug('checksums for file being created: %s', file_name)
            (md5_str, sha_str) = checksums(file_name)
            log.info('checksums for file created: %s', file_name)
            file_size = os.stat(file_name).st_size
            file_path = os.path.abspath(file_name)
            # format = re.split('.',file_name)[-1]
            # log.debug(format)

            file_formats = ['fastq', 'fasta', 'csv', 'what_else']
            format = 'other'
            for type in file_formats:
                if re.search(type, file_name):
                    format = type

            values = [{'local_file': file_path,
                       'size':       file_size,
                       'format':     format,
                       'md5':        md5_str,
                       'sha256':     sha_str
                      }]
            write_out_csv(checksum_file, fields, values)
        else:
            log.error('No such file?  %s', file_name)

    except Exception, e:
        log.error('Uh-Oh with writing list %s... %s', file_name, e)

def dir_checksum(args):
    """convert compression, checksum final file, writeout fields"""
    log.info( '-> Beginning function %s', 'dir_checksum')
    data_path = os.path.join(args.data_path, args.renamed_path)
    checksum_file = args.checksum_list_file

    checksummed = 0
    errored = 0
    errfiles = []
    try:
        subdirs = ['raw', 'clean']
        for subdir in subdirs:
            subpath = os.path.join(data_path, subdir)
            log.debug('subpath:', subpath)
            try:
                for file in os.listdir(subpath):
                    # log.debug('file in dir: %s', file)
                    file_path = os.path.join(subpath, file)
                    log.debug('file path: %s', file_path)
                    write_checksum_list(file_path, checksum_file)
                    checksummed += 1
            except Exception as e:
                log.error('Error in "%s"!!!   %s', 'file_in_subpath', e)
                errored += 1
                errfiles.append(file)
                raise e
    except Exception, e:
        log.error('Error in "%s"!!!   %s', 'convert_and_checksum', e)
        # continue to next, instead of `raise e`

    # summarize:
    log.info( '-> Files compression checksummed: %4s', str(checksummed))
    log.error('-> Errors found when processing:  %4s', str(errored))
    if errored:
        log.error('  -> For specifics on these files that errored, '
                  'please see the logfile.')
        log.error('Files: %s', str(errfiles))

def rename_files(args):
    """loop through mapping_file, subloop through files in curdir+row-subdir,
       rename matching filenames, convert compression formats, log
       checksums and file name/size to checksum_file
    """
    #TODO: squeeze into single loop through map_file, search curdir for match

    map_file = args.mapping_file
    checksum_file = args.checksum_list_file
    renamed_path = args.renamed_path
    data_path = args.data_path

    class summary:
        renamed = 0
        converted = 0
        errored = 0
        errfiles = []
        pending = 0
        pending_files = []

    for row in yield_csv_data(map_file):
        if row['original_file_base'] and row['dcc_file_base']: #row isn't empty
            # dir = row['dir']
            srce = row['second_file_base'] \
                if row['second_file_base'] != '' \
                else row['original_file_base']
            dest = row['dcc_file_base']
            # log.debug('__srce:'+srce+', dest:'+dest)
            try:
                filepath = os.path.join(data_path, row['dir'])
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
    log.info('-> Files successfully renamed:   %4s', str(summary.renamed))
    # log.info('-> Files compression converted:  %4s', str(summary.converted))
    # log.info('-> Files pending renaming:       %4s', str(summary.pending))
    # if summary.pending:
    #     log.error('  -> Pending Files: %s', str(summary.pending_files))

    log.error('-> Errors found when processing: %4s', str(summary.errored))
    if summary.errored:
        log.error('  -> For specifics on these files that errored, '
                  'please see the logfile.')
        log.error('  -> Files: %s', str(summary.errfiles))

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Make it Happen' ~~~~~

if __name__ == '__main__':
    """Mod main functions run called below as you need them (files need
       many steps, at different times...)
    """

    class args:
        # ...[ defaults ]...
        base_path = '/Users/bleopold/osdf/ipop_osdf/submit_osdf/data_files/'
        # data_path = '/Volumes/helix_weinstock/projects/HMP2/submissions/data/'
        data_path = '/data/weinstocklab/projects/HMP2/submissions/data/'
        renamed_path = 'renamed'


    # ...[ 16S ]...
    args.mapping_file = args.data_path + '20161108-data_files_16S.fof.dcc_rename_clean.csv'
    args.checksum_list_file = args.data_path + '20161206-checksums_16S.csv'
    rename_files(args)
    # dir_checksum(args)

    # ...[ mwgs ]...
    args.mapping_file = args.data_path + 'data_files_mwgs.fof_ready.csv'
    args.checksum_list_file = args.data_path + '20161206-checksums_mwgs.csv'
    # dir_checksum(args)

    # ...[ rnaseq ]...
    args.mapping_file = args.data_path + 'data_files_rnaseq.fof_ready.csv'
    args.checksum_list_file = args.data_path + '20161206-checksums_rnaseq.csv'
    # dir_checksum(args)

