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

def generate_tar(file_prefix=""):
    """generate tar archives, return file name
    """
    # TODO: debug this, as it always says: 'command returned non-zero exit status 1'
    try:
        tar_file = file_prefix + '.raw.fastq.tar'
        tar_cmd = ['tar', 'cf', tar_file, file_prefix+'*']
        tar_stat = get_output(tar_cmd)
        
        log.info('Archive file created: '+tar_file)
        return tar_file

    except Exception, e:
        log.error('Uh-Oh (generate_tar)... %s', e)
        raise e

def generate_tarbz(file_prefix="", outfile='list.log.csv'):
    """generate tar.bz2 archives, run md5sum and sha256sum
       then write file_name, file_size, md5sum, sha256sum values to csv outfile
    """
    fields = ['local_file','size','md5','sha256']
    try:
        if os.path.exists(outfile) and os.path.getsize(outfile) >= 0:
            tar_file = file_prefix + '.tar.bz2'
            tar_cmd = ['tar', 'cjf', tar_file, file_prefix+'*fastq*']
            tar_stat = get_output(tar_cmd)

            (md5_str, sha_str) = checksums(tar_file)

            tar_size = os.stat(tar_file).st_size
            values = [{'local_file':tar_file,'size':tar_size,
                       'md5':md5_str,'sha256':sha_str}]
            write_out_csv(outfile, fields, values)
            log.info('Archive file created: '+tar_file)

        else:
            write_out_csv(outfile, fields) #write headers

    except Exception, e:
        log.error('Uh-Oh... %s', e)
        raise e  # (or?) continue with log to show error

def write_checksum_list(file_name="", outfile='checksum_file_list.csv'):
    """generate list of file_name, md5sum and sha256sum
       then write file_name, file_size, md5sum, sha256sum values to csv outfile
    """
    fields = ['local_file','size','format','md5','sha256']
    if not os.path.exists(outfile) and os.path.getsize(outfile) >= 0:
        write_out_csv(outfile, fields) #write headers
    try:
        if os.path.exists(file_name) and os.path.getsize(file_name) >= 0:
            (md5_str, sha_str) = checksums(file_name)
            log.info('checksums for file created: %s'+file_name)
            file_size = os.stat(file_name).st_size
            file_path = os.path.abspath(file_name)
            # format = re.split('.',file_name)[-1]
            # log.debug(format)

            file_formats = ['fastq', 'fasta', 'csv', 'what_else']
            for type in file_formats:
                if re.search(type, file_name):
                    format = type
                else:
                    format = 'other'

            values = [{'local_file':file_path,
                       'size':file_size,
                       'format':format,
                       'md5':md5_str,
                       'sha256':sha_str
                      }]
            write_out_csv(outfile, fields, values)

    except Exception, e:
        log.error('Uh-Oh with writing list %s... %s', file_name, e)

def convert_gz_to_bz2(file_name):
    """convert gzip to bzip2 compression format"""
    #TODO: batch mode someday.  ls *.gz | parallel "gunzip -c {} | bzip2 > {.}.bz2"
    try:
        if os.path.exists(file_name) and os.path.getsize(file_name) >= 0:
            dest_file, repcount = re.subn('\.gz$', '.bz2', file_name)
            cmd = ['gunzip', '-c', file_name,
                   '|', 'bzip2', '>', dest_file,
                   # '&&', 'rm', file_name
                   ]
            out = get_output(cmd)
            if not out:
                get_output('rm', file_name)
            log.info('File compression converted: '+dest_file)
            return dest_file
    except Exception, e:
        log.error('Uh-Oh... compressing: %s', e)
        raise e  # (or?) continue with log to show error

def archive_fastq_files(args):
    """ only run this func for raw fastq PE files!!
    """

    map_file = args.mapping_file
    checksum_file = args.checksum_list_file
    renamed_path = args.renamed_path

    archived = 0
    errored = 0
    errfiles = []
    for row in yield_csv_data(map_file):
        if row['dcc_file_base'] != '': #row is not empty
            dest = row['dcc_file_base']
            path = row['dir']
            try:
                file_path = os.path.join(renamed_path, path, dest)
                log.debug('(%s)... file_path: %s', 'archive', file_path)
                tar_file = generate_tar(file_path)
                archived += 1

            except Exception, e:
                log.error('Error in "%s"!!!   %s', 'archive_fastq_files', e)
                errored += 1
                errfiles.append(dest)
                # continue to next, instead of `raise e`

    # summarize:
    log.info( '-> Files successfully archived: %4s', str(archived))
    log.error('-> Errors found when moving:    %4s', str(errored))
    if errored:
        log.error('  -> For specifics on these files that errored, '
                  'please see the logfile.')
        log.error('Files: %s', str(errfiles))

def dir_checksum(args):
    """convert compression, checksum final file, writeout fields"""
    checksum_file = args.checksum_list_file

    checksummed = 0
    errored = 0
    errfiles = []
    try:
        for file in os.listdir(os.curdir):
            write_checksum_list(file, outfile=checksum_file)
            checksummed += 1

    except Exception, e:
        log.error('Error in "%s"!!!   %s', 'convert_and_checksum', e)
        errored += 1
        errfiles.append(srce)
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

    class summary:
        renamed = 0
        converted = 0
        errored = 0
        errfiles = []
        pending = 0
        pending_files = []

    for row in yield_csv_data(map_file):
        if row['original_file_base']: #row is not empty
            # dir = row['dir']
            srce = row['second_file_base'] \
                if row['second_file_base'] != '' \
                else row['original_file_base']
            dest = row['dcc_file_base']
            # log.debug('__srce:'+srce+', dest:'+dest)
            try:
                # for file in os.listdir(os.curdir):
                curpath = os.path.join(os.curdir, row['dir'])
                for file in os.listdir(curpath):
                    # log.debug('__ file='+file)
                    if re.match(srce, file):
                        file = os.path.join(curpath, file)
                        repl = re.sub(srce, dest, file)
                        # repl_path = os.path.abspath(os.path.dirname(repl))
                        repl_file = os.path.join(renamed_path, repl)
                        log.info('Moving "%s" to "%s"', file, repl)
                        os.renames(file, repl_file)
                        # if re.search('raw', row['dir']):
                        #     #WARN: presumes illumina 'L'ane and 'R'ead numbers!
                        #     repl_glob = re.sub('[-_][LRS][0-9]*[-_].*',
                        #                        '', repl_file)
                        #     tar_file = generate_tar(repl_glob)
                        #     repl_file = tar_file
                        if re.search('raw', row['dir']):
                            continue # pending archive pre-checksumming
                        if os.stat(repl_file).st_size > 0:
                            summary.renamed += 1
                            write_checksum_list(repl_file,
                                    outfile=checksum_file)
                            # else:
                    #     log.info('___Not Moving "%s"', file)
                    #     summary.pending += 1
                    #     summary.pending_files.append(file)

            except Exception, e:
                log.error('Error in "%s"!!!   %s', 'rename_files', e)
                summary.errored += 1
                summary.errfiles.append(srce)
                # continue to next, instead of `raise e`

    # summarize:
    log.info( '-> Files successfully renamed:   %4s', str(summary.renamed))
    # log.info( '-> Files compression converted:  %4s', str(summary.converted))
    # log.info( '-> Files pending renaming:       %4s', str(summary.pending))
    # if summary.pending:
    #     log.error('  -> Pending Files: %s', str(summary.pending_files))

    log.error('-> Errors found when processing: %4s', str(summary.errored))
    if summary.errored:
        log.error('  -> For specifics on these files that errored, '
                'please see the logfile.')
        log.error('  -> Files: %s', str(summary.errfiles))


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Make it Happen' ~~~~~

if __name__ == '__main__':
    """This script must be run from within the directory containing all the
        files to be processed!
    """

    class args:
        renamed_path = 'renamed'
        mapping_file = '20160616-HMP2_metadata-filename_changes.csv'
        mapping_file = '/data/HMP2/20160616-HMP2-filename_changes.csv'

        mapping_file = '/Volumes/helix_data-weinstock/projects/HMP2/submissions/data/data_files.fof_ready.ZOZOW1T_mwgs_raw.csv'
        checksum_list_file = '/Users/bleopold/JAXGM/projects/HMP2/submission/20160819-checksums.csv'

    # write_checksum_list(args)
    # dir_checksum(args)
    # rename_files(arg)
    archive_fastq_files(args)
