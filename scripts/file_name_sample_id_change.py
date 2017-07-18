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
                    # write_checksum_list(os.path.abspath(file), checksum_file)
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

def archive_raw_fastq_files(args):
    """ only run this func for raw fastq PE files!!
    """

    map_file = args.mapping_file
    renamed_path = args.renamed_path
    data_path = args.data_path

    archived = 0
    errored = 0
    errfiles = []
    for old, new in filename_changes.items():
        if new:
            dest = new
            dir = 'raw'
            try:
                file_path = os.path.join(data_path, renamed_path, dir)
                # log.debug('(%s)... file_path: %s', 'archive', file_path)
                pref = os.path.join(file_path, dest)
                tar_file = generate_raw_tar(file_path, file_prefix=pref)
                # archived += 1 #debug "+= 1" syntax errors

            except Exception, e:
                log.error('Error in "%s"!!!   %s', 'archive_raw_fastq_files', e)
                # errored += 1 #debug "+= 1" syntax errors
                errfiles.append(dest)
                # continue to next, instead of `raise e`

    # summarize:
    # log.info( '-> Files successfully archived: %4s', str(archived))
    log.error('-> Errors found when moving:    %4s', str(errored))
    # if errored:
    if True: #debug "+= 1" syntax errors
        log.error('  -> For specifics on these files that errored, '
                  'please see the logfile.')
        log.error('Files: %s', str(errfiles))

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
        base_path = '/data/weinstocklab/projects/HMP2/submissions/dcc_osdf/submit_osdf/data_files/'
        data_path = '/data/weinstocklab/projects/HMP2/submissions/data/'
        renamed_path = 'renamed'

    filename_changes = {
        'HMP2_J09164_1_ST_T0_B0_0120_ZMGT937-04_ADM3N':        'HMP2_J09164_1_NS.*ZMGT937-04_ADM3N',
        'HMP2_J09166_1_ST_T0_B0_0120_ZLZQMEV-04_ADM3N':        'HMP2_J09166_1_NS.*ZLZQMEV-04_ADM3N',
        'HMP2_J09146_1_ST_T0_B0_0120_ZVNCGHM-1014X_ADM3N':     'HMP2_J09146_1_NS.*ZVNCGHM-1014_ADM3N',
        'HMP2_J09169_1_ST_T0_B0_0120_ZNQOVZV-1012_ADM3N':      'HMP2_J09169_1_NS.*ZNQOVZV-04_ADM3N',
        'HMP2_J09170_1_ST_T0_B0_0120_ZNQOVZV-1013_ADM3N':      'HMP2_J09170_1_NS.*ZNQOVZV-1012_ADM3N',
        'HMP2_J09171_1_ST_T0_B0_0120_ZNQOVZV-1014_ADM3N':      'HMP2_J09171_1_NS.*ZNQOVZV-1013_ADM3N',
        'HMP2_J09172_1_ST_T0_B0_0120_ZNQOVZV-1015_ADM3N':      'HMP2_J09172_1_NS.*ZNQOVZV-1014_ADM3N',
        'HMP2_J35107_1_NS_T0_B0_0120_ZTLUDS8-09_ANAY8':        'HMP2_J35107_1_NS.*ZTLUDS8-02_ANAY8',
        'HMP2_J43844_1_ST_T0_B0_0120_ZL9BTWF-1021_ANBEN':      'HMP2_J43844_1_ST.*ZL9BTWF-1022_ANBEN',
        'HMP2_J00830_1_ST_T0_B0_0120_ZN9YTFN-1015X_AA31J':     'HMP2_J00894_1_ST.*ZN9YTFN-1015X_AA31J',
        'HMP2_J00869_1_ST_T0_B0_0120_ZK112BX-2001_AA31J':      'HMP2_J00869_1_ST.*ZK112BX-2011_AA31J',
        'HMP2_J00870_1_ST_T0_B0_0120_ZK112BX-2002_AA31J':      'HMP2_J00870_1_ST.*ZK112BX-2012_AA31J',
        'HMP2_J00896_1_NS_T0_B0_0120_ZLZQMEV-01_AAH7B':        'HMP2_J00836_1_NS.*ZLZQMEV-01_AAH7B',
        'HMP2_J00896_1_ST_T0_B0_0120_ZLZQMEV-03_AA31J':        'HMP2_J00836_1_ST.*ZLZQMEV-03_AA31J',
        'HMP2_J05410_1_ST_T0_B0_0120_ZLZQMEV-01X_AA31J':       'HMP2_J05410_1_ST.*ZLZQMEV-01_AA31J',
        'HMP2_J35080_1_NS_T0_B0_0120_ZMBH10Z-08_ANAV8':        'HMP2_J35080_1_NS.*ZMBH10Z-07_ANAV8',
        'HMP2_J66974_1_ST_T0_B0_0120_ZVBQY1N-6035_APATM':      'HMP2_J66974_1_ST.*ZVBQY1N-6025_APATM',
        'HMP2_J44641_1_NS_T0_B0_0120_ZL9BTWF-1025_ANBEN':      'HMP2_J09162_1_NS.*ZL9BTWF-1025_ANBEN',
        'HMP2_J05417_1_NS_T0_B0_0122_ZLZNCLZ-03_AAH7B':        'HMP2_J05417_1_NS.*ZLZNCLZ-03-432_AAH7B',
        'HMP2_J05417_1_NS_T2_B0_0122_ZLZNCLZ-03_AAH7B':        'HMP2_J05417_1_NS.*ZLZNCLZ-03-432_AAH7B',
        'HMP2_J05449_1_NS_T0_B0_0120_ZK112BX-2001_AAH7B':      'HMP2_J05449_1_NS.*ZK112BX-2011_AAH7B',
        'HMP2_J05450_1_NS_T0_B0_0120_ZK112BX-2002_AAH7B':      'HMP2_J05450_1_NS.*ZK112BX-2012_AAH7B',
        'HMP2_J09110_1_ST_T0_B0_0120_ZKVR426-07_ADM3N':        'HMP2_J09110_1_ST.*ZKVR426-07-AL1_ADM3N',
        'HMP2_J09111_1_ST_T0_B0_0120_ZKFV71L-05_ADM3N':        'HMP2_J09111_1_ST.*ZKFV71L-05X_ADM3N',
        'HMP2_J09111_1_ST_T0_B0_0122_ZKFV71L-05_AAH7B':        'HMP2_J09111_1_ST.*ZKFV71L-05X_AAH7B',
        'HMP2_J09122_1_ST_T0_B0_0120_ZKVR426-07_ADM3N':        'HMP2_J09122_1_ST.*ZKVR426-07-AL2_ADM3N',
        'HMP2_J09126_1_ST_T0_B0_0120_ZNQOVZV-1012_ADM3N':      'HMP2_J09126_1_ST.*ZNQOVZV-04_ADM3N',
        'HMP2_J09127_1_ST_T0_B0_0120_ZNQOVZV-1013_ADM3N':      'HMP2_J09127_1_ST.*ZNQOVZV-1012_ADM3N',
        'HMP2_J09128_1_ST_T0_B0_0120_ZNQOVZV-1014_ADM3N':      'HMP2_J09128_1_ST.*ZNQOVZV-1013_ADM3N',
        'HMP2_J09129_1_ST_T0_B0_0120_ZNQOVZV-1015_ADM3N':      'HMP2_J09129_1_ST.*ZNQOVZV-1014_ADM3N',
        'HMP2_J09135_1_ST_T0_B0_0120_ZLZQMEV-1014_ADM3N':      'HMP2_J09135_1_ST.*ZLZQMEV-1014X_ADM3N',
        'HMP2_J09142_1_ST_T0_B0_0120_ZVNCGHM-02_ADM3N':        'HMP2_J09142_1_ST.*ZVNCGHM-02X_ADM3N',
        'HMP2_J09144_1_ST_T0_B0_0120_ZVNCGHM-1012_ADM3N':      'HMP2_J09144_1_ST.*ZVNCGHM-1011-AL2_ADM3N',
        'HMP2_J28591_1_ST_T0_B0_0120_ZLZQMEV-1014_AL5DH':      'HMP2_J28591_1_ST.*ZLZQMEV-1014-AL1_AL5DH',
        'HMP2_J28592_1_ST_T0_B0_0120_ZVNCGHM-1022_AL5DH':      'HMP2_J28592_1_ST.*ZVNCGHM-1022-AL1_AL5DH',
        'HMP2_J28593_1_ST_T0_B0_0120_ZL9BTWF-1023_AL5DH':      'HMP2_J28593_1_ST.*ZL9BTWF-1023-AL1_AL5DH',
        'HMP2_J28595_1_ST_T0_B0_0120_ZM8YXDM-01_AL5DH':        'HMP2_J28595_1_ST.*ZM8YXDM-01-AL1_AL5DH',
        'HMP2_J28597_1_ST_T0_B0_0120_ZVGW5FI-02_AL5DH':        'HMP2_J28597_1_ST.*ZVGW5FI-02-AL1_AL5DH',
        'HMP2_J28598_1_ST_T0_B0_0120_ZVM4N7A-01_AL5DH':        'HMP2_J28598_1_ST.*ZVM4N7A-01-AL1_AL5DH',
        'HMP2_J28599_1_ST_T0_B0_0120_ZYXQKWY-03_AL5DH':        'HMP2_J28599_1_ST.*ZYXQKWY-03-AL1_AL5DH',
        'HMP2_J28600_1_ST_T0_B0_0120_ZOZOW1T-65_AL5DH':        'HMP2_J28600_1_ST.*ZOZOW1T-65-AL1_AL5DH',
        'HMP2_J28601_1_ST_T0_B0_0120_ZOZOW1T-68_AL5DH':        'HMP2_J28601_1_ST.*ZOZOW1T-68-AL1_AL5DH',
        'HMP2_J28602_1_ST_T0_B0_0120_ZOZOW1T-61_AL5DH':        'HMP2_J28602_1_ST.*ZOZOW1T-61-AL1_AL5DH',
        'HMP2_J29046_1_NS_T0_B0_0120_ZK112BX-1013_ANAY8':      'HMP2_J29046_1_NS.*ZK112BX-2013_ANAY8',
        'HMP2_J29050_1_NS_T0_B0_0120_ZJTKAE3-07_AN77Y':        'HMP2_J29050_1_NS.*ZJTKAE3-6012_AN77Y',
        'HMP2_J35124_1_NS_T0_B0_0120_ZO9UWDL-6016_ANAY8':      'HMP2_J35124_1_NS.*ZR3WH7V-6016_ANAY8',
        'HMP2_J45095_1_ST_T0_B0_0120_ZVBQY1N-6031_APAJ2':      'HMP2_J45095_1_ST.*ZVBQY1N-6021_APAJ2',
        'HMP2_J45096_1_ST_T0_B0_0120_ZVBQY1N-6033_APAJ2':      'HMP2_J45096_1_ST.*ZVBQY1N-6023_APAJ2',
        'HMP2_J45393_1_ST_T0_B0_0120_ZVTCAK9-01_APB4D':        'HMP2_J45393_1_ST.*ZVTCAK9-01X_APB4D',
        'HMP2_J45511_1_ST_T0_B0_0120_ZMWEIX1-1025b-AL2_APA62': 'HMP2_J45511_1_ST.*ZMWEIX1-1025b_APA62',
        'HMP2_J45547_1_ST_T0_B0_0120_ZN3TBJM-2024_APA62':      'HMP2_J45547_1_ST.*ZN3TBJM-2024-AL2_APA62',
        'HMP2_J45595_1_ST_T0_B0_0120_ZX52KVK-2022_APAJ2':      'HMP2_J45595_1_ST.*ZX52KVK-2022-AL2_APAJ2',
        'HMP2_J66955_1_ST_T0_B0_0120_ZPXU188-03_APATM':        'HMP2_J66955_1_ST.*ZPXU188-02_APATM',
        'HMP2_J09122_1_NS_T0_B0_0120_ZKVR426-07_ANBEN':        'HMP2_J09122_1_NS.*ZKVR426-07-AL2_ANBEN',
        'HMP2_J29276_1_NS_T0_B0_0120_ZL9BTWF-1022_ANBEN':      'HMP2_J29276_1_NS.*ZL9BTWF-1022-AL1_ANBEN',
        'HMP2_J43843_1_ST_T0_B0_0120_ZTLUDS8-1023_ANBEN':      'HMP2_J43843_1_ST.*ZTLUDS8-1013-AL1_ANBEN',
        'HMP2_J08155_1_ST_T0_B0_0120_ZOZOW1T-60122_ACNUP':     'HMP2_J08155_1_ST.*ZOZOW1T-6012.2_ACNUP',
        'HMP2_J09143_1_ST_T0_B0_0120_ZVNCGHM-1011_ADM3N':      'HMP2_J09143_1_ST.*ZVNCGHM-1011-AL1_ADM3N',
        }
    # filename_changes = {
    #     # 'HMP2_J34933_1_NS_T0_B0_0120_ZY7IW45-5_APHU6':         'check "-5"',
    #     'HMP2_J09164_1_ST_T0_B0_0120_ZMGT937-04_ADM3N':        'HMP2_J09164_1_NS_T0_B0_0120_ZMGT937-04_ADM3N',
    #     'HMP2_J09166_1_ST_T0_B0_0120_ZLZQMEV-04_ADM3N':        'HMP2_J09166_1_NS_T0_B0_0120_ZLZQMEV-04_ADM3N',
    #     'HMP2_J09146_1_ST_T0_B0_0120_ZVNCGHM-1014X_ADM3N':     'HMP2_J09146_1_NS_T0_B0_0120_ZVNCGHM-1014_ADM3N',
    #     'HMP2_J09169_1_ST_T0_B0_0120_ZNQOVZV-1012_ADM3N':      'HMP2_J09169_1_NS_T0_B0_0120_ZNQOVZV-04_ADM3N',
    #     'HMP2_J09170_1_ST_T0_B0_0120_ZNQOVZV-1013_ADM3N':      'HMP2_J09170_1_NS_T0_B0_0120_ZNQOVZV-1012_ADM3N',
    #     'HMP2_J09171_1_ST_T0_B0_0120_ZNQOVZV-1014_ADM3N':      'HMP2_J09171_1_NS_T0_B0_0120_ZNQOVZV-1013_ADM3N',
    #     'HMP2_J09172_1_ST_T0_B0_0120_ZNQOVZV-1015_ADM3N':      'HMP2_J09172_1_NS_T0_B0_0120_ZNQOVZV-1014_ADM3N',
    #     'HMP2_J35107_1_NS_T0_B0_0120_ZTLUDS8-09_ANAY8':        'HMP2_J35107_1_NS_T0_B0_0120_ZTLUDS8-02_ANAY8',
    #     'HMP2_J43844_1_ST_T0_B0_0120_ZL9BTWF-1021_ANBEN':      'HMP2_J43844_1_ST_T0_B0_0120_ZL9BTWF-1022_ANBEN',
    #     'HMP2_J00830_1_ST_T0_B0_0120_ZN9YTFN-1015X_AA31J':     'HMP2_J00894_1_ST_T0_B0_0120_ZN9YTFN-1015X_AA31J',
    #     'HMP2_J00869_1_ST_T0_B0_0120_ZK112BX-2001_AA31J':      'HMP2_J00869_1_ST_T0_B0_0120_ZK112BX-2011_AA31J',
    #     'HMP2_J00870_1_ST_T0_B0_0120_ZK112BX-2002_AA31J':      'HMP2_J00870_1_ST_T0_B0_0120_ZK112BX-2012_AA31J',
    #     'HMP2_J00896_1_NS_T0_B0_0120_ZLZQMEV-01_AAH7B':        'HMP2_J00836_1_NS_T0_B0_0120_ZLZQMEV-01_AAH7B',
    #     'HMP2_J00896_1_ST_T0_B0_0120_ZLZQMEV-03_AA31J':        'HMP2_J00836_1_ST_T0_B0_0120_ZLZQMEV-03_AA31J',
    #     'HMP2_J05410_1_ST_T0_B0_0120_ZLZQMEV-01X_AA31J':       'HMP2_J05410_1_ST_T0_B0_0120_ZLZQMEV-01_AA31J',
    #     'HMP2_J35080_1_NS_T0_B0_0120_ZMBH10Z-08_ANAV8':        'HMP2_J35080_1_NS_T0_B0_0120_ZMBH10Z-07_ANAV8',
    #     'HMP2_J66974_1_ST_T0_B0_0120_ZVBQY1N-6035_APATM':      'HMP2_J66974_1_ST_T0_B0_0120_ZVBQY1N-6025_APATM',
    #     'HMP2_J44641_1_NS_T0_B0_0120_ZL9BTWF-1025_ANBEN':      'HMP2_J09162_1_NS_T0_B0_0120_ZL9BTWF-1025_ANBEN',
    #     'HMP2_J05417_1_NS_T0_B0_0122_ZLZNCLZ-03_AAH7B':        'HMP2_J05417_1_NS_T0_B0_0122_ZLZNCLZ-03-432_AAH7B',
    #     'HMP2_J05417_1_NS_T2_B0_0122_ZLZNCLZ-03_AAH7B':        'HMP2_J05417_1_NS_T2_B0_0122_ZLZNCLZ-03-432_AAH7B',
    #     'HMP2_J05449_1_NS_T0_B0_0120_ZK112BX-2001_AAH7B':      'HMP2_J05449_1_NS_T0_B0_0120_ZK112BX-2011_AAH7B',
    #     'HMP2_J05450_1_NS_T0_B0_0120_ZK112BX-2002_AAH7B':      'HMP2_J05450_1_NS_T0_B0_0120_ZK112BX-2012_AAH7B',
    #     'HMP2_J09110_1_ST_T0_B0_0120_ZKVR426-07_ADM3N':        'HMP2_J09110_1_ST_T0_B0_0120_ZKVR426-07-AL1_ADM3N',
    #     'HMP2_J09111_1_ST_T0_B0_0120_ZKFV71L-05_ADM3N':        'HMP2_J09111_1_ST_T0_B0_0120_ZKFV71L-05X_ADM3N',
    #     'HMP2_J09111_1_ST_T0_B0_0122_ZKFV71L-05_AAH7B':        'HMP2_J09111_1_ST_T0_B0_0122_ZKFV71L-05X_AAH7B',
    #     'HMP2_J09122_1_ST_T0_B0_0120_ZKVR426-07_ADM3N':        'HMP2_J09122_1_ST_T0_B0_0120_ZKVR426-07-AL2_ADM3N',
    #     'HMP2_J09126_1_ST_T0_B0_0120_ZNQOVZV-1012_ADM3N':      'HMP2_J09126_1_ST_T0_B0_0120_ZNQOVZV-04_ADM3N',
    #     'HMP2_J09127_1_ST_T0_B0_0120_ZNQOVZV-1013_ADM3N':      'HMP2_J09127_1_ST_T0_B0_0120_ZNQOVZV-1012_ADM3N',
    #     'HMP2_J09128_1_ST_T0_B0_0120_ZNQOVZV-1014_ADM3N':      'HMP2_J09128_1_ST_T0_B0_0120_ZNQOVZV-1013_ADM3N',
    #     'HMP2_J09129_1_ST_T0_B0_0120_ZNQOVZV-1015_ADM3N':      'HMP2_J09129_1_ST_T0_B0_0120_ZNQOVZV-1014_ADM3N',
    #     'HMP2_J09135_1_ST_T0_B0_0120_ZLZQMEV-1014_ADM3N':      'HMP2_J09135_1_ST_T0_B0_0120_ZLZQMEV-1014X_ADM3N',
    #     'HMP2_J09142_1_ST_T0_B0_0120_ZVNCGHM-02_ADM3N':        'HMP2_J09142_1_ST_T0_B0_0120_ZVNCGHM-02X_ADM3N',
    #     'HMP2_J09144_1_ST_T0_B0_0120_ZVNCGHM-1012_ADM3N':      'HMP2_J09144_1_ST_T0_B0_0120_ZVNCGHM-1011-AL2_ADM3N',
    #     'HMP2_J28591_1_ST_T0_B0_0120_ZLZQMEV-1014_AL5DH':      'HMP2_J28591_1_ST_T0_B0_0120_ZLZQMEV-1014-AL1_AL5DH',
    #     'HMP2_J28592_1_ST_T0_B0_0120_ZVNCGHM-1022_AL5DH':      'HMP2_J28592_1_ST_T0_B0_0120_ZVNCGHM-1022-AL1_AL5DH',
    #     'HMP2_J28593_1_ST_T0_B0_0120_ZL9BTWF-1023_AL5DH':      'HMP2_J28593_1_ST_T0_B0_0120_ZL9BTWF-1023-AL1_AL5DH',
    #     'HMP2_J28595_1_ST_T0_B0_0120_ZM8YXDM-01_AL5DH':        'HMP2_J28595_1_ST_T0_B0_0120_ZM8YXDM-01-AL1_AL5DH',
    #     'HMP2_J28597_1_ST_T0_B0_0120_ZVGW5FI-02_AL5DH':        'HMP2_J28597_1_ST_T0_B0_0120_ZVGW5FI-02-AL1_AL5DH',
    #     'HMP2_J28598_1_ST_T0_B0_0120_ZVM4N7A-01_AL5DH':        'HMP2_J28598_1_ST_T0_B0_0120_ZVM4N7A-01-AL1_AL5DH',
    #     'HMP2_J28599_1_ST_T0_B0_0120_ZYXQKWY-03_AL5DH':        'HMP2_J28599_1_ST_T0_B0_0120_ZYXQKWY-03-AL1_AL5DH',
    #     'HMP2_J28600_1_ST_T0_B0_0120_ZOZOW1T-65_AL5DH':        'HMP2_J28600_1_ST_T0_B0_0120_ZOZOW1T-65-AL1_AL5DH',
    #     'HMP2_J28601_1_ST_T0_B0_0120_ZOZOW1T-68_AL5DH':        'HMP2_J28601_1_ST_T0_B0_0120_ZOZOW1T-68-AL1_AL5DH',
    #     'HMP2_J28602_1_ST_T0_B0_0120_ZOZOW1T-61_AL5DH':        'HMP2_J28602_1_ST_T0_B0_0120_ZOZOW1T-61-AL1_AL5DH',
    #     'HMP2_J29046_1_NS_T0_B0_0120_ZK112BX-1013_ANAY8':      'HMP2_J29046_1_NS_T0_B0_0120_ZK112BX-2013_ANAY8',
    #     'HMP2_J29050_1_NS_T0_B0_0120_ZJTKAE3-07_AN77Y':        'HMP2_J29050_1_NS_T0_B0_0120_ZJTKAE3-6012_AN77Y',
    #     'HMP2_J35124_1_NS_T0_B0_0120_ZO9UWDL-6016_ANAY8':      'HMP2_J35124_1_NS_T0_B0_0120_ZR3WH7V-6016_ANAY8',
    #     'HMP2_J45095_1_ST_T0_B0_0120_ZVBQY1N-6031_APAJ2':      'HMP2_J45095_1_ST_T0_B0_0120_ZVBQY1N-6021_APAJ2',
    #     'HMP2_J45096_1_ST_T0_B0_0120_ZVBQY1N-6033_APAJ2':      'HMP2_J45096_1_ST_T0_B0_0120_ZVBQY1N-6023_APAJ2',
    #     'HMP2_J45393_1_ST_T0_B0_0120_ZVTCAK9-01_APB4D':        'HMP2_J45393_1_ST_T0_B0_0120_ZVTCAK9-01X_APB4D',
    #     'HMP2_J45511_1_ST_T0_B0_0120_ZMWEIX1-1025b-AL2_APA62': 'HMP2_J45511_1_ST_T0_B0_0120_ZMWEIX1-1025b_APA62',
    #     'HMP2_J45547_1_ST_T0_B0_0120_ZN3TBJM-2024_APA62':      'HMP2_J45547_1_ST_T0_B0_0120_ZN3TBJM-2024-AL2_APA62',
    #     'HMP2_J45595_1_ST_T0_B0_0120_ZX52KVK-2022_APAJ2':      'HMP2_J45595_1_ST_T0_B0_0120_ZX52KVK-2022-AL2_APAJ2',
    #     'HMP2_J66955_1_ST_T0_B0_0120_ZPXU188-03_APATM':        'HMP2_J66955_1_ST_T0_B0_0120_ZPXU188-02_APATM',
    #     'HMP2_J09122_1_NS_T0_B0_0120_ZKVR426-07_ANBEN':        'HMP2_J09122_1_NS_T0_B0_0120_ZKVR426-07-AL2_ANBEN',
    #     'HMP2_J29276_1_NS_T0_B0_0120_ZL9BTWF-1022_ANBEN':      'HMP2_J29276_1_NS_T0_B0_0120_ZL9BTWF-1022-AL1_ANBEN',
    #     'HMP2_J43843_1_ST_T0_B0_0120_ZTLUDS8-1023_ANBEN':      'HMP2_J43843_1_ST_T0_B0_0120_ZTLUDS8-1013-AL1_ANBEN',
    #     'HMP2_J08155_1_ST_T0_B0_0120_ZOZOW1T-60122_ACNUP':     'HMP2_J08155_1_ST_T0_B0_0120_ZOZOW1T-6012.2_ACNUP',
    #     'HMP2_J09143_1_ST_T0_B0_0120_ZVNCGHM-1011_ADM3N':      'HMP2_J09143_1_ST_T0_B0_0120_ZVNCGHM-1011-AL1_ADM3N',
    #     }


    # ...[ 16S ]...
    # args.mapping_file = args.data_path + 'data_files_16S.fof_ready.csv'
    # args.checksum_list_file = args.data_path + 'checksums/checksums.16S_raw-20170209.csv'
    # args.renamed_path = 'renamed/16S'
    # archive_raw_fastq_files(args)
    # dir_checksum(args)
    prep_file = 'data_files/20170202-samples_16S_merged.csv'
    for k,v in filename_changes.items():
        grep_cmd = ' '.join(['echo ', v, '; grep', '"'+v+'"', prep_file, '; exit 0'])
        # log.debug('cmd: %s', grep_cmd)
        # log.warning(get_output(grep_cmd))
        print(get_output(grep_cmd))
