#!/usr/bin/env python

# written 2017-02-09 10:48:43-0500
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


def move(src, dst):
    """mod from copy solution on: http://stackoverflow.com/a/4847660/1600630"""
    log.debug('moving paths: %s, %s', src, dst)
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


def extract_tar(dest_path="./", tar_name=""):
    """generate tar archives, return file name"""
    try:
        os.chdir(dest_path)
        tar_cmd = ' '.join(['tar', 'xvf', tar_name])
        tar_stat = get_output(tar_cmd)

        log.info('Archive file extracted: %s', tar_name)
        return tar_name

    except Exception, e:
        log.error('Uh-Oh (extract_raw_tar)... %s', e)
        raise e


if __name__ == '__main__':

    log = log_it()

    filename_changes = {
        # 'HMP2_J34933_1_NS_T0_B0_0120_ZY7IW45-5_APHU6':         'check "-5"',
        'HMP2_J09164_1_ST_T0_B0_0120_ZMGT937-04_ADM3N':        'HMP2_J09164_1_NS_T0_B0_0120_ZMGT937-04_ADM3N',
        'HMP2_J09166_1_ST_T0_B0_0120_ZLZQMEV-04_ADM3N':        'HMP2_J09166_1_NS_T0_B0_0120_ZLZQMEV-04_ADM3N',
        'HMP2_J09146_1_ST_T0_B0_0120_ZVNCGHM-1014X_ADM3N':     'HMP2_J09146_1_NS_T0_B0_0120_ZVNCGHM-1014_ADM3N',
        'HMP2_J09169_1_ST_T0_B0_0120_ZNQOVZV-1012_ADM3N':      'HMP2_J09169_1_NS_T0_B0_0120_ZNQOVZV-04_ADM3N',
        'HMP2_J09170_1_ST_T0_B0_0120_ZNQOVZV-1013_ADM3N':      'HMP2_J09170_1_NS_T0_B0_0120_ZNQOVZV-1012_ADM3N',
        'HMP2_J09171_1_ST_T0_B0_0120_ZNQOVZV-1014_ADM3N':      'HMP2_J09171_1_NS_T0_B0_0120_ZNQOVZV-1013_ADM3N',
        'HMP2_J09172_1_ST_T0_B0_0120_ZNQOVZV-1015_ADM3N':      'HMP2_J09172_1_NS_T0_B0_0120_ZNQOVZV-1014_ADM3N',
        'HMP2_J35107_1_NS_T0_B0_0120_ZTLUDS8-09_ANAY8':        'HMP2_J35107_1_NS_T0_B0_0120_ZTLUDS8-02_ANAY8',
        'HMP2_J43844_1_ST_T0_B0_0120_ZL9BTWF-1021_ANBEN':      'HMP2_J43844_1_ST_T0_B0_0120_ZL9BTWF-1022_ANBEN',
        'HMP2_J00830_1_ST_T0_B0_0120_ZN9YTFN-1015X_AA31J':     'HMP2_J00894_1_ST_T0_B0_0120_ZN9YTFN-1015X_AA31J',
        'HMP2_J00869_1_ST_T0_B0_0120_ZK112BX-2001_AA31J':      'HMP2_J00869_1_ST_T0_B0_0120_ZK112BX-2011_AA31J',
        'HMP2_J00870_1_ST_T0_B0_0120_ZK112BX-2002_AA31J':      'HMP2_J00870_1_ST_T0_B0_0120_ZK112BX-2012_AA31J',
        'HMP2_J00896_1_NS_T0_B0_0120_ZLZQMEV-01_AAH7B':        'HMP2_J00836_1_NS_T0_B0_0120_ZLZQMEV-01_AAH7B',
        'HMP2_J00896_1_ST_T0_B0_0120_ZLZQMEV-03_AA31J':        'HMP2_J00836_1_ST_T0_B0_0120_ZLZQMEV-03_AA31J',
        'HMP2_J05410_1_ST_T0_B0_0120_ZLZQMEV-01X_AA31J':       'HMP2_J05410_1_ST_T0_B0_0120_ZLZQMEV-01_AA31J',
        'HMP2_J35080_1_NS_T0_B0_0120_ZMBH10Z-08_ANAV8':        'HMP2_J35080_1_NS_T0_B0_0120_ZMBH10Z-07_ANAV8',
        'HMP2_J66974_1_ST_T0_B0_0120_ZVBQY1N-6035_APATM':      'HMP2_J66974_1_ST_T0_B0_0120_ZVBQY1N-6025_APATM',
        'HMP2_J44641_1_NS_T0_B0_0120_ZL9BTWF-1025_ANBEN':      'HMP2_J09162_1_NS_T0_B0_0120_ZL9BTWF-1025_ANBEN',
        'HMP2_J05417_1_NS_T0_B0_0122_ZLZNCLZ-03_AAH7B':        'HMP2_J05417_1_NS_T0_B0_0122_ZLZNCLZ-03-432_AAH7B',
        'HMP2_J05417_1_NS_T2_B0_0122_ZLZNCLZ-03_AAH7B':        'HMP2_J05417_1_NS_T2_B0_0122_ZLZNCLZ-03-432_AAH7B',
        'HMP2_J05449_1_NS_T0_B0_0120_ZK112BX-2001_AAH7B':      'HMP2_J05449_1_NS_T0_B0_0120_ZK112BX-2011_AAH7B',
        'HMP2_J05450_1_NS_T0_B0_0120_ZK112BX-2002_AAH7B':      'HMP2_J05450_1_NS_T0_B0_0120_ZK112BX-2012_AAH7B',
        'HMP2_J09110_1_ST_T0_B0_0120_ZKVR426-07_ADM3N':        'HMP2_J09110_1_ST_T0_B0_0120_ZKVR426-07-AL1_ADM3N',
        'HMP2_J09111_1_ST_T0_B0_0120_ZKFV71L-05_ADM3N':        'HMP2_J09111_1_ST_T0_B0_0120_ZKFV71L-05X_ADM3N',
        'HMP2_J09111_1_ST_T0_B0_0122_ZKFV71L-05_AAH7B':        'HMP2_J09111_1_ST_T0_B0_0122_ZKFV71L-05X_AAH7B',
        'HMP2_J09122_1_ST_T0_B0_0120_ZKVR426-07_ADM3N':        'HMP2_J09122_1_ST_T0_B0_0120_ZKVR426-07-AL2_ADM3N',
        'HMP2_J09126_1_ST_T0_B0_0120_ZNQOVZV-1012_ADM3N':      'HMP2_J09126_1_ST_T0_B0_0120_ZNQOVZV-04_ADM3N',
        'HMP2_J09127_1_ST_T0_B0_0120_ZNQOVZV-1013_ADM3N':      'HMP2_J09127_1_ST_T0_B0_0120_ZNQOVZV-1012_ADM3N',
        'HMP2_J09128_1_ST_T0_B0_0120_ZNQOVZV-1014_ADM3N':      'HMP2_J09128_1_ST_T0_B0_0120_ZNQOVZV-1013_ADM3N',
        'HMP2_J09129_1_ST_T0_B0_0120_ZNQOVZV-1015_ADM3N':      'HMP2_J09129_1_ST_T0_B0_0120_ZNQOVZV-1014_ADM3N',
        'HMP2_J09135_1_ST_T0_B0_0120_ZLZQMEV-1014_ADM3N':      'HMP2_J09135_1_ST_T0_B0_0120_ZLZQMEV-1014X_ADM3N',
        'HMP2_J09142_1_ST_T0_B0_0120_ZVNCGHM-02_ADM3N':        'HMP2_J09142_1_ST_T0_B0_0120_ZVNCGHM-02X_ADM3N',
        'HMP2_J09144_1_ST_T0_B0_0120_ZVNCGHM-1012_ADM3N':      'HMP2_J09144_1_ST_T0_B0_0120_ZVNCGHM-1011-AL2_ADM3N',
        'HMP2_J28591_1_ST_T0_B0_0120_ZLZQMEV-1014_AL5DH':      'HMP2_J28591_1_ST_T0_B0_0120_ZLZQMEV-1014-AL1_AL5DH',
        'HMP2_J28592_1_ST_T0_B0_0120_ZVNCGHM-1022_AL5DH':      'HMP2_J28592_1_ST_T0_B0_0120_ZVNCGHM-1022-AL1_AL5DH',
        'HMP2_J28593_1_ST_T0_B0_0120_ZL9BTWF-1023_AL5DH':      'HMP2_J28593_1_ST_T0_B0_0120_ZL9BTWF-1023-AL1_AL5DH',
        'HMP2_J28595_1_ST_T0_B0_0120_ZM8YXDM-01_AL5DH':        'HMP2_J28595_1_ST_T0_B0_0120_ZM8YXDM-01-AL1_AL5DH',
        'HMP2_J28597_1_ST_T0_B0_0120_ZVGW5FI-02_AL5DH':        'HMP2_J28597_1_ST_T0_B0_0120_ZVGW5FI-02-AL1_AL5DH',
        'HMP2_J28598_1_ST_T0_B0_0120_ZVM4N7A-01_AL5DH':        'HMP2_J28598_1_ST_T0_B0_0120_ZVM4N7A-01-AL1_AL5DH',
        'HMP2_J28599_1_ST_T0_B0_0120_ZYXQKWY-03_AL5DH':        'HMP2_J28599_1_ST_T0_B0_0120_ZYXQKWY-03-AL1_AL5DH',
        'HMP2_J28600_1_ST_T0_B0_0120_ZOZOW1T-65_AL5DH':        'HMP2_J28600_1_ST_T0_B0_0120_ZOZOW1T-65-AL1_AL5DH',
        'HMP2_J28601_1_ST_T0_B0_0120_ZOZOW1T-68_AL5DH':        'HMP2_J28601_1_ST_T0_B0_0120_ZOZOW1T-68-AL1_AL5DH',
        'HMP2_J28602_1_ST_T0_B0_0120_ZOZOW1T-61_AL5DH':        'HMP2_J28602_1_ST_T0_B0_0120_ZOZOW1T-61-AL1_AL5DH',
        'HMP2_J29046_1_NS_T0_B0_0120_ZK112BX-1013_ANAY8':      'HMP2_J29046_1_NS_T0_B0_0120_ZK112BX-2013_ANAY8',
        'HMP2_J29050_1_NS_T0_B0_0120_ZJTKAE3-07_AN77Y':        'HMP2_J29050_1_NS_T0_B0_0120_ZJTKAE3-6012_AN77Y',
        'HMP2_J35124_1_NS_T0_B0_0120_ZO9UWDL-6016_ANAY8':      'HMP2_J35124_1_NS_T0_B0_0120_ZR3WH7V-6016_ANAY8',
        'HMP2_J45095_1_ST_T0_B0_0120_ZVBQY1N-6031_APAJ2':      'HMP2_J45095_1_ST_T0_B0_0120_ZVBQY1N-6021_APAJ2',
        'HMP2_J45096_1_ST_T0_B0_0120_ZVBQY1N-6033_APAJ2':      'HMP2_J45096_1_ST_T0_B0_0120_ZVBQY1N-6023_APAJ2',
        'HMP2_J45393_1_ST_T0_B0_0120_ZVTCAK9-01_APB4D':        'HMP2_J45393_1_ST_T0_B0_0120_ZVTCAK9-01X_APB4D',
        'HMP2_J45511_1_ST_T0_B0_0120_ZMWEIX1-1025b-AL2_APA62': 'HMP2_J45511_1_ST_T0_B0_0120_ZMWEIX1-1025b_APA62',
        'HMP2_J45547_1_ST_T0_B0_0120_ZN3TBJM-2024_APA62':      'HMP2_J45547_1_ST_T0_B0_0120_ZN3TBJM-2024-AL2_APA62',
        'HMP2_J45595_1_ST_T0_B0_0120_ZX52KVK-2022_APAJ2':      'HMP2_J45595_1_ST_T0_B0_0120_ZX52KVK-2022-AL2_APAJ2',
        'HMP2_J66955_1_ST_T0_B0_0120_ZPXU188-03_APATM':        'HMP2_J66955_1_ST_T0_B0_0120_ZPXU188-02_APATM',
        'HMP2_J09122_1_NS_T0_B0_0120_ZKVR426-07_ANBEN':        'HMP2_J09122_1_NS_T0_B0_0120_ZKVR426-07-AL2_ANBEN',
        'HMP2_J29276_1_NS_T0_B0_0120_ZL9BTWF-1022_ANBEN':      'HMP2_J29276_1_NS_T0_B0_0120_ZL9BTWF-1022-AL1_ANBEN',
        'HMP2_J43843_1_ST_T0_B0_0120_ZTLUDS8-1023_ANBEN':      'HMP2_J43843_1_ST_T0_B0_0120_ZTLUDS8-1013-AL1_ANBEN',
        'HMP2_J08155_1_ST_T0_B0_0120_ZOZOW1T-60122_ACNUP':     'HMP2_J08155_1_ST_T0_B0_0120_ZOZOW1T-6012.2_ACNUP',
        'HMP2_J09143_1_ST_T0_B0_0120_ZVNCGHM-1011_ADM3N':      'HMP2_J09143_1_ST_T0_B0_0120_ZVNCGHM-1011-AL1_ADM3N',
        }

    for old, new in filename_changes.items():
        BASEDIR = "/data/weinstocklab/projects/HMP2/submissions/data"
        # OLDDIR = os.path.join(BASEDIR, "renamed_scr/16S/raw")
        # NEWDIR = os.path.join(BASEDIR, "renamed/16S/raw")
        OLDDIR = os.path.join(BASEDIR, "renamed_scr/16S/clean")
        NEWDIR = os.path.join(BASEDIR, "renamed/16S/clean")
        log.info('files: %s: %s', old, new)
        for dirp, subdirs, fpaths in os.walk(OLDDIR):
            for fp in fpaths:
                # log.info('file: %s', fp)
                if re.match(old,fp):
                    log.info('file match: %s', fp)
                    old_fpath = os.path.join(dirp, fp)
                    new_fpath = os.path.join(NEWDIR, fp)
                    # move to new folder
                    move(old_fpath, new_fpath)
                    log.info('file moved: %s', fp)

                    if fp.endswith('.tar'):
                        extract_tar(NEWDIR, fp)

                    # enact sample name changes
                    newpath = re.sub(old, new, fp)
                    move(
                         os.path.join(NEWDIR, fp),
                         os.path.join(NEWDIR, newpath)
                        )
                    log.info('file name changedd: %s -> %s', fp, newpath)

