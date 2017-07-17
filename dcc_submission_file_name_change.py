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
import shutil

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

    # root = logging.getLogger(format=logFormat)
    # root.setLevel(loglevel)
    # root.addHandler(fh)

    return logger

log = log_it('dcc_submission_file_name_change')


def copy(src, dst):
    """found solution on: http://stackoverflow.com/a/4847660/1600630"""
    if os.path.islink(src):
        linkto = os.readlink(src)
        os.symlink(linkto, dst)
    else:
        shutil.copy(src,dst)

def move(src, dst):
    """mod from copy solution on: http://stackoverflow.com/a/4847660/1600630"""
    if os.path.islink(src):
        linkto = os.readlink(src)
        os.symlink(linkto, dst)
        os.remove(src)
    else:
        shutil.move(src,dst)

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

def generate_raw_tar(dest_path="./", file_prefix=""):
    """generate tar archives, return file name"""
    try:
        src_path = os.path.dirname(file_prefix)
        prefix = os.path.basename(file_prefix)
        os.chdir(src_path)
        # try:
            # statinfo = os.lstat(file_prefix+'*')
            # # log.debug(statinfo)
        # except Exception, e:
            # log.info("Prefix '%s' files do not exist!", file_prefix)
            # return None
        # else:
            # tar_file = os.path.join(dest_path, file_prefix + '.raw.fastq.tar')
            # tar_cmd = ' '.join(['tar', 'chf', tar_file, prefix+'*'])
            # tar_stat = get_output(tar_cmd)
            # log.info('Archive file created: %s', tar_file)
            # return tar_file
        tar_file = os.path.join(dest_path, file_prefix + '.raw.fastq.tar')
        tar_cmd = ' '.join(['tar', 'chf', tar_file, prefix+'*'])
        tar_stat = get_output(tar_cmd)
        log.info('Archive file created: %s', tar_file)
        return tar_file

    except Exception, e:
        log.error('Uh-Oh (generate_raw_tar)... %s', e)
        raise e

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

def dir_checksum(args, subdirs = ['raw', 'clean']):
    """convert compression, checksum final file, writeout fields"""
    log.info( '-> Beginning function %s', 'dir_checksum')
    renamed_path = args.renamed_path
    # data_path = args.data_path
    data_path = os.path.join(args.data_path, args.renamed_path)
    checksum_file = args.checksum_list_file

    checksummed = 0
    errored = 0
    errfiles = []
    try:
        for subdir in subdirs:
            subpath = os.path.join(data_path, subdir)
            log.debug('subpath: %s', subpath)
            try:
                for file in os.listdir(subpath):
                    if subdir == 'raw' and not file.endswith('tar'):
                        next
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
        log.error('Error in "%s"!!!   %s', 'dir_checksum', e)
        # continue to next, instead of `raise e`

    # summarize:
    log.info( '-> Files compression checksummed: %4s', str(checksummed))
    log.error('-> Errors found when processing:  %4s', str(errored))
    if errored:
        log.error('  -> For specifics on these files that errored, '
                  'please see the logfile.')
        log.error('Files: %s', str(errfiles))

def archive_raw_fastq_files(args):
    """ only run this func for raw fastq PE files!!
    """

    map_file = args.mapping_file
    data_path = args.data_path

    archived = 0
    errored = 0
    errfiles = []
    for row in yield_csv_data(map_file):
        if (re.search('raw', row['dir']) and
                row['dcc_file_base'] != ''):
            dest = row['dcc_file_base']
            dir = row['dir']
            # dir = 'raw'
            try:
                file_path = os.path.join(data_path, dir)
                # log.debug('(%s)... file_path: %s', 'archive', file_path)
                pref = os.path.join(file_path, dest)
                # log.debug('(%s)... file_path_pref: %s', 'archive', pref)
                tar_file = generate_raw_tar(file_path, file_prefix=pref)
                # archived += 1 #debug "+= 1" syntax errors

            except Exception, e:
                log.error('Error in "%s"!!!   %s', 'archive_raw_fastq_files', e)
                # errored += 1 #debug "+= 1" syntax errors
                errfiles.append(dest)
                # continue to next, instead of `raise e`

    # summarize:
    log.info( '-> Files successfully archived: %4s', str(archived))
    log.error('-> Errors found when moving:    %4s', str(errored))
    # if errored:
    if True: #debug "+= 1" syntax errors
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
    data_path = args.data_path
    renamed_path = args.renamed_path

    class summary:
        renamed = 0
        converted = 0
        errored = 0
        errfiles = []
        pending = 0
        pending_files = []

    for row in yield_csv_data(map_file):
        if row['original_file_base'] and row['dcc_file_base']: #row isn't empty
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
                        # repl_path = os.path.join(data_path, row['dir'])
                        repl_path = os.path.join(data_path, renamed_path, os.path.basename(row['dir']))
                        repl_file = os.path.join(repl_path, repl_sub)
                        file = os.path.join(filepath, file)
                        log.info('Copying "%s" to "%s"', file, repl_file)
                        try:
                            # copy(file, repl_file)
                            move(file, repl_file)
                            try:
                                #TODO: check is_symlink before .st_size? (only if on same host)
                                if int(os.lstat(repl_file).st_size) > 0:
                                    summary.renamed += 1
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
        else:
            summary.pending += 1

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

def create_mapping_file(args):
    """create the mapping file adds in the column dcc_file_base using 'prep_id'
       from the dnaprep_file jaxid_library field matched to the mapping_file
    """
    prep_file = args.dnaprep_file
    map_file = args.mapping_file
    map_outfile = args.map_outfile

    fields = ['dcc_file_base', 'dir', 'original_file_base', 'second_file_base',
              'final_sample_name', 'rand_subject_id', 'flag_meanings']
    write_out_csv(map_outfile, fields) # headers to outfiles

    preps = [row for row in yield_csv_data(prep_file)]
    for maprow in yield_csv_data(map_file):
        srce_file = maprow['second_file_base'] \
            if maprow['second_file_base'] != '' \
            else maprow['original_file_base']
        for prep in preps:
            libid = prep['jaxid_library']
            runid = prep['run_id']
            if libid:
                # log.debug('jaxid_library: %s', libid)
                if re.search(libid,srce_file) and re.search(runid,srce_file):
                    maprow['dcc_file_base'] = prep['prep_id']
        write_out_csv(map_outfile, fields, [maprow])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Make it Happen' ~~~~~

if __name__ == '__main__':
    """Mod main functions run called below as you need them (files need
       many steps, at different times...)
    """

    class args:
        # ...[ defaults ]...
        base_path = '/projects/weinstock-lab/projects/HMP2_iHMP_Snyder/submissions/dcc_osdf/submit_osdf/data_files/'
        data_path = '/projects/weinstock-lab/projects/HMP2_iHMP_Snyder/submissions/data/'
        renamed_path = 'renamed'

    # ...[ 16S ]...
    args.checksum_list_file = args.data_path + 'checksums/20170619_raw_clean_16S_checksums_latest.csv'
    args.mapping_file = args.data_path + '16S/new/20170619_new_renames.csv'
    args.renamed_path = '16S/latest'
    # rename_files(args)
    # archive_raw_fastq_files(args)
    dir_checksum(args, subdirs=['raw', 'clean'])

    # ...[ mwgs ]...
    # renamed_path = 'mwgs/raw_fastq'
    # args.mapping_file = args.base_path + '20170214_mwgs_prep_ids-unsubmitted.csv'
    # args.checksum_list_file = args.data_path + 'checksums/checksums.mwgs_raw-20170425.csv'
    # rename_files(args)
    # archive_raw_fastq_files(args)
    # args.renamed_path = 'renamed/mwgs'
    # dir_checksum(args, subdirs=['raw'])

    # ...[ rnaseq ]...
    # args.renamed_path = 'renamed'
    # args.mapping_file = args.data_path + 'data_files_rnaseq.fof_ready_2.csv'
    # args.checksum_list_file = args.data_path + 'checksums/checksums.rnaseq_raw-20170214.csv'
    # rename_files(args)
    # archive_raw_fastq_files(args)
    # args.renamed_path = 'renamed/rnaseq'
    # dir_checksum(args, subdirs=['raw'])
