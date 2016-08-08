#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
File: track_jaxid_parents.py
Author: Benjamin Leopold
Date: 2016-08-05T12:59:11
Description: Tracks parents and children within jaxid hierarchy.

Usage:
    track_jaxid_parents -l <jaxid_libaries.csv> -o outfile(stdout)

"""

# TODO: move from reference spreadsheet usage to direct jaxid_db checking

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~~
from __future__ import print_function

import os
import re
import csv
import json
import yaml
import logging
import argparse

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Constants ~~~~~
COOLNESS = True

class settings():
    """default values for  settings"""
    jaxid_ref_file = 'jaxid_database_export_20160801.csv'
    id_ref_map = None

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  Utility Functions  ~~~~~
# Log It!
def log_it(logname=os.path.basename(__file__)):
    import time
    """log_it setup"""
    curtime = time.strftime("%Y%m%d-%H%M")
    logfile = '{}_{}.log'.format(curtime, logname)

    loglevel = logging.INFO
    # logFormat="%(asctime)s %(levelname)5s: %(funcName)15s: %(message)s"
    logFormat="%(asctime)s %(levelname)5s: %(message)s"

    logging.basicConfig(format=logFormat)
    logger = logging.getLogger(logname)
    logger.setLevel(loglevel)

    fh = logging.FileHandler(logfile, mode='a')
    formatter = logging.Formatter(logFormat)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

log = log_it('track_jaxid_parents')


def get_field_header(csv_file):
    """returns first row of csv file as list of fieldnames"""
    log.info('Loading fields from {}'.format(csv_file))
    with open(csv_file) as csvfh:
        try:
            reader = csv.DictReader(csvfh)
            return reader.fieldnames
        except csv.Error as e:
            log.exception('Trying to read CSV file %s, line %d: %s',
                    csv_file, reader.line_num, e)


def load_data(csv_file):
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

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~   Local Functions   ~~~~~

def get_parent_node_id(ref_map, jaxid, parent_id):
    """ read node ids from csv tracking file
        return first "parent" node matching node_type
    """
    log.debug("in function: %s", 'get_parent_node_id')
    log.debug('--> args: '+ id_file_name +','+ node_type +','+ parent_id)
    try:
        pass
        # for row in load_data(id_file_name):
            # if re.match(node_type.lower(),row['node_type']):
                # if row['jaxid']:
                    # log.debug('--> matching node row: '+ str(row))
                    # return row['parent_id']
                # else:
                    # log.debug('--> no match node row for: '+ str(parent_id))
            # else:
                # log.debug('--> no match node row: '+ str(node_type))
    except Exception, e:
        raise e


def get_child_node_ids(id_file_name, node_type, parent_id):
    """ read node ids from csv tracking file
        yield "child" node ids matching node_type
    """
    log.debug("in function: %s", 'get_child_node_id')
    try:
        for row in load_data(id_file_name):
            if re.match(node_type,row['node_type']):
                if re.match(parent_id,row['internal_id']):
                    log.debug('--> matching node row: '+ str(row))
                    log.debug('parent type: {}, osdf_node_id: {}'.format(
                        node_type,str(row['osdf_node_id'])))
                    return row['osdf_node_id']
    except Exception, e:
        raise e


def build_id_ref_map(ref_in):
    """construct reference mapping dict of ids from csv file"""
    log.debug("in function: %s", 'build_id_ref_map')
    ref_map = {}
    for row in load_data(ref_in):
        ref_map[row['jaxid']] = row
    log.debug("finished function: %s", 'build_id_ref_map')
    return ref_map


def which_id_type(record_dict):
    """determine which id type is being parsed"""
    # log.debug("in function: %s", 'which_id_type')
    # spreadsheet logic:
    #   =IF(NOT(ISERROR(search("pool",D73))),"pool",
    #       IF(AND(F73="Z",G73="Z",H73="Z"),"NULL",
    #           IF(and(G73="Z",H73="Z"),"specimen",
    #              IF(H73="Z","extraction",
    #                         "library")
    #           )
    #         )
    #      )
    # log.debug("in function: %s", 'which_id_type')
    try:
        idtype = ''

        collab = record_dict['collab_id']
        sample = record_dict['sample']
        nucleic = record_dict['nucleic_acid']
        seqtype = record_dict['seq_type']

        if re.search('pool', collab):
            idtype = 'pool'
        elif sample == 'Z':
            idtype = 'NULL' # bad!!!
        elif nucleic == 'Z':
            idtype = 'specimen'
        elif seqtype == 'Z':
            idtype = 'extraction'
        else:
            idtype = 'library'
        # log.debug("function: %s, idtype==%s", 'which_id_type', idtype)
        return idtype
    except Exception, e:
        # raise e
        # log.error("function: %s, idtype==%s", 'which_id_type', idtype)
        return ''


def which_parent_type(id_type):
    """Goes up the hierarchical list of types
       returning the direct parent type.
    """
    log.debug("in function: %s", 'which_parent_type')
    id_list = [
                None,
                'specimen',
                'extraction',
                'library',
               ]
    family_tree = {
            id_list[t-1]:id_list[t]
            for t in id_list
            if t > 0
            }
    return family_tree[id_type]


def get_received_id(idrow):
    """use get_parent_id to track recursively"""
    parent_id = idrow['parent_id']
    if parent_id = 'received':
        return parent_id
    elif parent_id in settings.id_ref_map:
        return get_received_id(idrow)
    else:
        return ''




def track_library_parents(library_csv, outfile):
    """Incoming: csv file with library jaxids
       Outgoing: csv outfile with new column 'jaxid_parent'
       Uses the 'settings.jaxid_ref_file' as mapping source.
       Return: highest-level jaxid, either parent=received or
               id_type=specimen.
    """
    log.debug("in function: %s", 'track_library_parents')
    outfields = get_field_header(library_csv)
    outfields.insert(outfields.index('jaxid')+1, 'jaxid_parent')
    write_out_csv(outfile, outfields) # field headers

    for row in load_data(library_csv):
        if row['jaxid'] in settings.id_ref_map:
            idrow = (settings.id_ref_map[row['jaxid']])
            idtype = which_id_type(idrow)
            if not idtype:  # just in case there are not fields in that idrow
                idtype = 'library'
            try:
                jaxid = row['jaxid']
                row['jaxid_parent'] = get_received_id(idrow)
                write_out_csv(outfile, outfields, [row])
            except Exception, e:
                raise e

    return True


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Make it happen! ~~~~~

def parse_args(args):
    """all args to script are parsed here"""
    #TODO: add inbound/outbound streams, not files (read/write csv and/or sql)
    parser = argparse.ArgumentParser()
    add = parser.add_argument

    add('-l', '--library_csv',
        metavar='LIBRARY_CSV', default=None,
        help="CSV File containing column 'jaxid'")
    add('-o', '--outfile',
        metavar='OUTFILE', default='parent_ids.csv',
        # type=argparse.FileType('a'),
        help='File to store output') # (default stdout)')
    add("-j", "--jaxid_ref_file",
        metavar='JAXID_REF_FILE',
        default=settings.jaxid_ref_file,
        dest='ref_file',
        help="jaxid relation reference spreadsheet")
    add("-v", "--verbose",
        default=False,
        action="store_true",
        help="increase output verbosity")
    return parser.parse_args(args)


def main(args):
    """get it done!"""
    success = False
    if args.verbose:
        log.setLevel(logging.DEBUG)

    # ref_file = settings.jaxid_ref_file
    if os.access(args.ref_file, os.R_OK):
        settings.id_ref_map = build_id_ref_map(args.ref_file)
    else:
        log.error("JAXid reference file is not readable or does not exist!")
        return False


    if (args.library_csv):
        success = track_library_parents(args.library_csv, args.outfile)


    return success


if __name__ == '__main__':
    import sys
    args = parse_args('-v -l test_jaxid_libs.csv'.split())
    # args = parse_args(sys.argv)
    sys.exit(main(args))
