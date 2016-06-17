#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Script to change and record all filenames from our original names at JAX to
   the newly-Stanford-privated names without subject-specific info; to
   publicize and upload to the DCC's OSDF, SRA, etc

   All filenames (original, final) are read from a CSV

   End filename structure:
        PROJ_Joriginal_Jlibrary_Tn_Bn_0000_SeqType_TissueSource_CollabID_RunID
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
    l = logging.getLogger(logname)
    l.setLevel(loglevel)

    formatter = logging.Formatter(logFormat)

    # ch = logging.StreamHandler()
    # ch.setLevel(loglevel)
    # ch.setFormatter(formatter)
    # l.addHandler(ch)

    fh = logging.FileHandler(logfile, mode='a')
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)
    l.addHandler(fh)
    return l

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
    with open(csv_file) as csvfh:
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
            writer = csv.DictWriter(csvout,fieldnames)
            if values[0] is not None:
                try:
                    for row in values:
                        if isinstance(row,dict):
                            log.debug(row)
                            writer.writerow(row)
                except Exception, e:
                    log.exception('Writing CSV file %s, %s',
                            csv_file, str(e))
                    raise e
            else:
                log.info('Writing header of fieldnames to {}'.format(csv_file))
                writer.writeheader()
    except IOError, e:
        raise e

def checksums(filename):
    """run md5 and sha256 checksums, return array of two digests"""
    log.info('Running checksums on: '+filename)

    md5_cmd = ' '.join(['md5sum', filename])
    md5_out = get_output(md5_cmd, stderr=None)
    md5_str = re.split(' ',md5_out)[0]
    log.debug('checksum md5: '+md5_str)

    sha_cmd = ' '.join(['sha256sum', filename])
    sha_out = get_output(sha_cmd, stderr=None)
    sha_str = re.split(' ',sha_out)[0]
    log.debug('checksum sha: '+sha_str)

    return md5_str,sha_str

def generate_tarbz(file_prefix="", outfile='list.log.csv'):
    """generate tar.bz2 archives, run md5sum and sha256sum
       then write file_name, file_size, md5sum, sha256sum values to csv outfile
    """
    fields = ['local_file','size','md5','sha256']
    try:
        if os.path.exists(outfile) and os.path.getsize(outfile) >= 0:
            tar_file = file_prefix + '.tar.bz2'
            tar_cmd  = ' '.join(['tar', 'cjf', tar_file, file_prefix+'*fastq*'])
            tar_stat = get_output(tar_cmd)

            md5_str, sha_str = checksums(tar_file)

            tar_size = os.stat(tar_file).st_size
            values = [ {'local_file':tar_file,'size':tar_size,
                        'md5':md5_str,'sha256':sha_str} ]
            write_out_csv(outfile,fields,values)
            log.info('Archive file created: '+tar_file)

        else:
            write_out_csv(outfile,fields) #write headers

    except Exception, e:
        log.error('Uh-Oh... %s', e)
        raise e  # (or?) continue with log to show error

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Make it Happen' ~~~~~

def rename_files(args):
    map_file = args.mapping_file

    renamed = 0
    errored = 0
    errfiles = []
    # log.info('PWD='+os.getcwd())
    # oldpwd = os.curdir
    for row in yield_csv_data(map_file):
        if row['original_file_base']: #row is not empty
            # dir = row['dir']
            srce = row['second_file_base'] \
                if row['second_file_base'] != '' \
                else row['original_file_base']
            dest = row['dcc_file_base']
            # log.debug('__srce:'+srce+', dest:'+dest)

            # srce = os.path.join(dir, srce)
            # dest = os.path.join(dir, dest)
            try:
                #TODO: travel diff dirs, adjust listdir to match
                # log.info('Entering "%s"',dir)
                # os.chdir(dir)
                # log.info('PWD='+os.getcwd())
                for f in os.listdir(os.curdir):
                    # log.debug('__ f='+f)
                    if re.match(srce,f):
                        d = re.sub(srce,dest,f)
                        # log.debug('__ f='+f+',d='+d)
                        log.info('Moving "%s" to "%s"',f,d)
                        os.rename(f,d)
                        if os.stat(d).st_size > 0:
                            renamed += 1

            except Exception, e:
                log.error('Error!!!   %s', e)
                errored += 1
                errfiles.append(srce)
                # continue to next, instead of `raise e`
        # os.chdir(oldpwd)

    # summarize:
    log.info( '-> Files successfully renamed: %4s', str(renamed))
    log.error('-> Errors found when moving:   %4s', str(errored))
    if errored:
        log.error(
        '  -> For specifics on these files that errored, please see the logfile.')
        log.error('Files: %s', str(errfiles))

def archive_fastq_files(args):
    map_file = args.mapping_file

    archived = 0
    errored = 0
    errfiles = []
    # generate_tarbz() #write 'outfile' headers
    for row in yield_csv_data(map_file):
        if row['dcc_file_base'] != '': #row is not empty
            dest = row['dcc_file_base']
            try:
                # only run the following gen_tar for raw fastq PE files!!
                tar_status = generate_tarbz(dest)
                archived += 1

            except Exception, e:
                log.error('Error!!!   %s', e)
                errored += 1
                errfiles.append(dest)
                # continue to next, instead of `raise e`

    # summarize:
    log.info( '-> Files successfully archived: %4s', str(archived))
    log.error('-> Errors found when moving:    %4s', str(errored))
    if errored:
        log.error(
        '  -> For specifics on these files that errored, please see the logfile.')
        log.error('Files: %s', str(errfiles))


if __name__ == '__main__':
    class args:
        mapping_file = '20160616-HMP2_metadata-filename_changes.csv'

    # status = rename_files(args)
    status = archive_fastq_files(args)
    sys.exit(status)
