#!/opt/compsci/python/3.4.2/bin/python3.4
# -*- coding: utf-8 -*-

import os
import re
import csv
import logging
import time

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~
# Log It!
def log_it(logname=os.path.basename(__file__), logdir="logs"):
    """log_it setup"""
    curtime = time.strftime("%Y%m%d-%H%M")
    logfile = '.'.join([curtime, logname, 'log'])
    logfile = os.path.join(logdir, logfile)

    loglevel = logging.DEBUG
    logFormat = \
        "%(asctime)s %(levelname)5s: %(module)15s %(funcName)10s: %(message)s"
    formatter = logging.Formatter(logFormat)

    logging.basicConfig(format=logFormat, filename=logfile, level=loglevel)
    # logging.basicConfig(format=logFormat, level=loglevel)
    l = logging.getLogger(logname)

    ch = logging.StreamHandler()
    ch.setLevel(loglevel)
    ch.setFormatter(formatter)
    l.addHandler(ch)

    # fh = logging.FileHandler(logfile, mode='a')
    # fh.setLevel(loglevel)
    # fh.setFormatter(formatter)
    # l.addHandler(fh)

    # warnlogfile = '.'.join([curtime, logname, 'WARN', 'log'])
    # warnlogfile = os.path.join(logdir, warnlogfile)
    # wfh = logging.FileHandler(warnlogfile, mode='a')
    # wfh.setLevel(logging.WARNING)
    # warn_logFormat = "%(levelname)5s: %(message)s"
    # warn_formatter = logging.Formatter(warn_logFormat)
    # wfh.setFormatter(formatter)
    # l.addHandler(wfh)

    return l

log = log_it(logdir='.')

def get_cur_datetime():
    """return datetime stamp of NOW"""
    return time.strftime("%Y-%m-%d %H:%M")


def load_data(csv_file, delim=',', quotechar='"'):
    """yield row dicts from csv_file using DictReader
    """
    log.info('Loading rows from {}'.format(csv_file))
    with open(csv_file, 'rU') as csvfh:
        reader = csv.DictReader(csvfh, dialect='excel',
                                delimiter=delim, quotechar=quotechar)
        # log.debug('csv dictreader opened')
        try:
            for row in reader:
                # log.debug(row)
                yield row
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


def write_out_csv(csv_file,fieldnames,values=[]):
    """write all values in csv format to outfile.
    Values is list of dicts w/ keys matching fieldnames.
    To write header to file, omit `values`
    """
    try:
        with open(csv_file, 'a') as csvout:
            writer = csv.DictWriter(csvout, fieldnames)
            if values:
                # log.info('Writing csv to {}'.format(csv_file))
                try:
                    for row in values:
                        # log.debug('Next row {}'.format(str(row)[0:50]+'...'))
                        if isinstance(row, dict):
                            log.info(row)
                            writer.writerow(row)
                except Exception as e:
                    log.exception('Error writing CSV file %s, %s', csv_file, str(e))
                    raise e
            else:
                log.info('Writing header of fieldnames to {}'.format(csv_file))
                writer.writeheader()
    except IOError as e:
        raise e


def read_and_match(args):
    """read index and tw odata file, then match data records by index rows"""
    # indexes = [row for row in load_data(args.index_file)]
    # for row in indexes:

    fields = ['orig_name','new_name']
    write_out_csv(args.outfile, fields)

    orig = [row for row in load_data(args.data_file1)]
    news = [row for row in load_data(args.data_file2)]

    for row in load_data(args.index_file):
        for old in orig:
            if re.search(row['orig'], old['orig_name']):
                for new in news:
                    if re.search(row['new'], new['new_name']):
                        vals = [{'orig_name': old['orig_name'],
                                 'new_name': new['new_name']}]
                        write_out_csv(args.outfile, fields, vals)


if __name__ == '__main__':
    class args:
        index_file = 'rna_jaxid_lib_samp.csv'
        data_file1 = 'orig_fastq_5c.fof'
        data_file2 = 'raw_fastq_5d.fof'
        outfile = 'map_old_new_fastq_5d.csv'
    read_and_match(args)
