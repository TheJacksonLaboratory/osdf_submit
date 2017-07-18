#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: jaxid_parse_uploaded_mismatches.py
Intent: parse each row of node_id_tracking csv
        grep the 1st (received) and 2nd jaxid (library) for prep ids
        check up the library's parent chain for each
        plus tail jaxid, tissue type, collab id from samples

        error flag if != the received id from updated master sample sheet
Author: Benjamin Leopold
Date: 2016-09-08 15:54:04-0400
"""

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~~

import sys
import re

from cutlass_utils import load_data, write_out_csv, get_field_header, log_it

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Constants ~~~~~
COOLNESS = True

class settings:
    """default values for  settings"""
    path = '/Users/bleopold/osdf/ipop_osdf/submit_osdf/data_files/'

    master_sample_file = path + 'HMP2_SampleTracking_MasterSheet_v1.1_erroneous.csv'
    node_id_tracking_file = path + 'prediabetes_node_id_tracking.csv'

    jaxid_ref_file = path + 'jaxid_database_export_20160801.csv'
    id_ref_map = OrderedDict()
    mod_field_name = 'jaxid_parent_mismatch'


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~
log = log_it('jaxid_parse_uploaded_mismatches')


def import_whole_csv(filename):
    """loads data from concatenated sample sheet of all HMP2 samples """
    csv_data = []
    for row in load_data(filename):
        csv_data.append(row)
    return csv_data


def build_id_ref_map(ref_in):
    """construct reference mapping dict of ids from csv file"""
    log.debug("begin function")
    ref_map = {}
    for row in load_data(ref_in):
        ref_map[row['jaxid']] = row
    log.debug("finished function")
    return ref_map


def get_received_id(idrow):
    """use get_parent_id to track recursively to original sample received"""
    log.debug("going up the heritage")
    # log.debug("idrow: %s", str(idrow))
    try:
        if idrow:
            parent_id = idrow['parent_id']
            if parent_id == 'received':
                # log.debug("if1 parent_id: %s", parent_id)
                return idrow['jaxid']
            elif parent_id in settings.id_ref_map:
                # log.debug("elif2 parent_id: %s", parent_id)
                return get_received_id(settings.id_ref_map[parent_id])
        else:
            # log.debug("else parent_id: %s", parent_id)
            return ''

    except Exception, e:
        log.exception(str(e))
        raise e


def gen_node_dict(node_id_file, node_type):
    """from incoming node file
       create dict generator of node_type with row's details
    """
    for row in load_data(node_id_file):
        if row['node_type'] == node_type:
            yield row


def parse_jaxid_mismatches_samples(node_id_file,
        new_file, new_fieldnames, mod_field_name, master_samples):
    """read through rows of tracking file
       append to mod_field_name to mark any mismatch errors found
    """
    node_type = 'sample'
    log.info('checking master sample erroneous list')
    for row in master_samples:
        collab_sample = row['Random Final Sample Name']
        tissue_sample = row['Specimen'].lower()
        jaxid_sample = row['Recd JAXid']

        log.info('checking node id tracking list for SAMPLES')
        for idrow in gen_node_dict(node_id_file, node_type):
            int_id = idrow['internal_id']
            # expected sample.internal_id format: collab-id_tissue_Jaxid
            collab_id, tissue_id, jaxid = re.split('_', int_id)

            if collab_sample == collab_id
            and tissue_sample == tissue_id:
            and jaxid_sample != jaxid:
                idrow[mod_field_name] = ','.join(
                    idrow[mod_field_name],
                    'ERR:{},{}!={}?'.format(node_type,int_id,jaxid_sample))
                log.debug('node err? %s', idrow[mod_field_name])
                err_jaxid_samples.append(int_id)

            write_out_csv(new_file, fieldnames=new_fieldnames, values=[idrow])


def parse_jaxid_mismatches_seqfiles(node_id_file,
        new_file, new_fieldnames, mod_field_name, master_samples):
    """read through rows of tracking file
       append to mod_field_name to mark any mismatch errors found
    """
    node_type = 'sixteensdnaprep'
    log.info('checking master sample erroneous list')
    for row in master_samples:
        collab_sample = row['Random Final Sample Name']
        tissue_sample = row['Specimen'].lower()
        jaxid_sample = row['Recd JAXid']

        log.info('checking node id tracking list for SAMPLES')
        for idrow in gen_node_dict(node_id_file, node_type):
            int_id = idrow['internal_id']
            # expected dna prep id format:
            #   HMP2_J16531_J16664_1_NS_T0_B0_0120_ZVGW5FI-03_AF11U
            # need only elem[1:2] after _.split
            collab_id, tissue_id, jaxid = re.split('_', int_id)

            if collab_sample == collab_id
            and tissue_sample == tissue_id:
            and jaxid_sample != jaxid:
                idrow[mod_field_name] = ','.join(
                    idrow[mod_field_name],
                    'ERR:{},{}!={}?'.format(node_type,int_id,jaxid_sample))
                log.debug('node err? %s', idrow[mod_field_name])

            write_out_csv(new_file, fieldnames=new_fieldnames, values=[idrow])


def parse_jaxid_mismatches_seqfiles(node_id_file,
        new_file, new_fieldnames, mod_field_name, master_samples):
    """read through rows of tracking file
       append to mod_field_name to mark any mismatch errors found
    """




def main():
    """make it happen'"""

    master_samples = import_whole_csv(settings.master_sample_file)

    log.info('loading node tracking file''s fieldnames')
    fieldnames_nodes = get_field_header(settings.node_id_tracking_file)
    log.info('adding column "%s"',settings.mod_field_name )
    fieldnames_nodes.insert(1, settings.mod_field_name)

    log.info('checking mismatch jaxids, writing to new master file')
    new_node_file = settings.node_id_tracking_file[0:-4] + '_idmatch_errors.csv'

    write_out_csv(new_node_file, fieldnames=fieldnames_nodes)
    parse_jaxid_mismatches_samples(settings.node_id_tracking_file,
                                   new_node_file,
                                   fieldnames_nodes,
                                   settings.mod_field_name,
                                   master_samples)

    parse_jaxid_mismatches_seqfiles(settings.node_id_tracking_file,
                                    new_node_file,
                                    fieldnames_nodes,
                                    settings.mod_field_name,
                                    master_samples)


    log.info('The modified node tracking sheet is now: %s', new_node_file)


if __name__ == '__main__':

    sys.exit(main())

