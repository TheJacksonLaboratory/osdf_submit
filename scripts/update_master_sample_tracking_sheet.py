#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: sync_metadata_spreadsheets.py
Intent: get data from several spreadsheet sources, glean relevant, output to
        new sample, dnaprep(&? raw and trimmed) sheets.
Author: Benjamin Leopold
Date: 2016-07-25 10:46:07-0400
"""

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~~

from __future__ import print_function

import sys
import re

from cutlass_utils import load_data, write_out_csv, get_field_header, log_it

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Constants ~~~~~
COOLNESS = True

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~
log = log_it('master_sheet_update')


def import_whole_csv(filename):
    """loads data from concatenated sample sheet of all HMP2 samples """
    csv_data = []
    # csv_dialect = csv_type_sniff(filename)
    # csv_data = load_data(filename)
    for row in load_data(filename):
        csv_data.append(row)
    return csv_data


def trans_sample_type(type_code):
    return 'stool' if type_code == 'ST' \
            else 'nasal' if type_code == 'NS' \
            else ''


def add_jaxid_to_master_list(jaxids, samples, new_master_file, fieldnames):
    """determine and add values to jaxid_tissue column
       Incoming:
            jaxid datasheet filename
            master sample sheet filename
            new filename for master sheet output
       Return: number of IDs matched
       Output: write new_master_file
    """
    log.info('in add_jaxid_to_master_list function')
    matched = 0
    for row in load_data(samples):
        sample_old = row['Original Sample Name']
        # sample_final = row['Final Sample Name']
        sample_rand = row['Random Final Sample Name']
        material_recd = row['Nucleic Acid']
        dna_vs_rna = row['DNAorRNA']

        row['RNA JAX-ID'] = ""
        jaxid_gdna = row['gDNA JAX-ID']
        jaxid_tissue = row['Tissue JAX-ID']

        log.info('processing row: %s, %s', sample_rand, dna_vs_rna)
        body_site = row['Specimen'].lower()
        # check jaxid_db for match
        for idrow in load_data(jaxids):
            # collab_id = idrow['collab_id'][0:len(sample_id)+1]
            collab_id = idrow['collab_id']
            nucleic_acid = idrow['nucleic_acid']
            if idrow['parent_id'] == 'received':
                if body_site == trans_sample_type(idrow['sample']):
                    sample = sample_rand.lower()
                    collab = collab_id.lower()
                    # if re.search(sample, collab):
                    if sample == collab:
                        if idrow['id_type'] == material_recd == 'specimen':
                            if (body_site == "nasal" and\
                                re.match(dna_vs_rna, idrow['notes']))\
                               or body_site == "stool":
                                row['Tissue JAX-ID'] += ' ' + idrow['jaxid']
                                matched += 1
                                log.debug('Matched: tissue %s == %s',
                                          row['Tissue JAX-ID'], idrow['jaxid'])

                        elif idrow['id_type'] == 'extraction':
                            if nucleic_acid == material_recd == 'gDNA':
                                row['gDNA JAX-ID'] += ' ' + idrow['jaxid']
                                matched += 1
                                log.debug('Matched: gDNA %s == %s',
                                          row['gDNA JAX-ID'], idrow['jaxid'])
                            if nucleic_acid == material_recd and \
                                material_recd in \
                                ['cDNA', 'Rib Depleted RNA', 'Total RNA']:
                                row['RNA JAX-ID'] += ' ' + idrow['jaxid']
                                matched += 1
                                log.debug('Matched: RNA %s == %s',
                                          row['RNA JAX-ID'], idrow['jaxid'])
                    # else:
                        # log.debug('No match on id: %s', collab)

        write_out_csv(new_master_file,
                      fieldnames=fieldnames,
                      values=[row])

    return matched


def add_visit_id_to_master_list(visit_ids, master_sample_file,
        new_master_file, new_master_fieldnames):
    """add columns to determine visit_ids
       using visit name, number and interval
    """
    log.info('mod\'ing master list pre-coalescing, adding visit_id fields')

    for row in load_data(master_sample_file):
        sample_final = row['Final Sample Name']
        # reset to blank all fields being added
        row['visit_number'] = ''
        row['interval'] = ''
        row['study_date'] = ''

        for vrow in load_data(visit_ids):
            visit_id = vrow['Visit ID']
            if sample_final == visit_id:
                row['visit_number'] = vrow['visit_num']
                row['interval'] = vrow['interval']
                row['study_date'] = vrow['Study Day']
                row['visit_id'] = '_'.join([visit_id,
                                            vrow['visit_num'],
                                            vrow['interval']])

        write_out_csv(new_master_file,
                      fieldnames=new_master_fieldnames,
                      values=[row])


def parse_jaxid_column_dupes(master_sample_file,
        new_file, new_fieldnames, mod_field_name):
    """add column to mark any dupes found/removed
    """


    log.info('mod''ing master list removal of dupe jaxids')

    for row in load_data(master_sample_file):
        row[mod_field_name] = '' # init/create column

        # vars from columns
        jaxid_tis = re.split('\W+', row['Tissue JAX-ID'])
        jaxid_rna = re.split('\W+', row['RNA JAX-ID'])
        jaxid_dna = re.split('\W+', row['gDNA JAX-ID'])

        if len(jaxid_tis) > 1:
            row[mod_field_name] += "dupe tissue jaxid! "
            jaxid_tis = sorted(set(jaxid_tis))

        if len(jaxid_rna) > 1:
            row[mod_field_name] += "dupe rna jaxid! "
            jaxid_rna = sorted(set(jaxid_rna))

        if len(jaxid_dna) > 1:
            row[mod_field_name] += "dupe dna jaxid! "
            jaxid_dna = sorted(set(jaxid_dna))

        # columns from vars
        row['Tissue JAX-ID'] = ' '.join(jaxid_tis)
        row['RNA JAX-ID'] = ' '.join(jaxid_rna)
        row['gDNA JAX-ID'] = ' '.join(jaxid_dna)

        write_out_csv(new_file,
                      fieldnames=new_fieldnames,
                      values=[row])


def main(args):
    """manage imports, cross-matches"""


    log.info('loading master sample file''s fieldnames')
    fieldnames_master_sheet = get_field_header(args.master_samples)
    # args.fieldnames.jaxid_list = get_field_header(args.jaxid_list)


    log.info('matching jaxids, writing to new master file')
    new_master_file = args.master_samples[0:-4] + '_wJAXids.csv'
    write_out_csv(new_master_file,
                  fieldnames=fieldnames_master_sheet)
    add_jaxid_to_master_list(args.jaxid_list,
                             args.master_samples,
                             new_master_file,
                             fieldnames_master_sheet)


    log.info('matching visit ids, writing to new master file')
    vis_master_file = new_master_file[0:-4] + '_wVisitIDs.csv'

    vis_master_fieldnames = get_field_header(new_master_file)
    # elem 8 is Project Code...
    for elem in ['study_date', 'interval', 'visit_number', 'visit_id']:
        vis_master_fieldnames.insert(8, elem)
    write_out_csv(vis_master_file,
                  fieldnames=vis_master_fieldnames)
    add_visit_id_to_master_list(args.visit_numbers,
                                new_master_file,
                                vis_master_file,
                                vis_master_fieldnames)


    fieldnames_master_sheet = get_field_header(args.master_sample_file)
    mod_field_name = 'jaxid_dupes_removed'
    log.info('adding column "%s"',mod_field_name )
    fieldnames_master_sheet.insert(13, mod_field_name)

    log.info('checking dupe jaxids, writing to new master file')
    new_master_file = args.master_sample_file[0:-4] + '_noDupeJAXids.csv'

    write_out_csv(new_master_file,
                  fieldnames=fieldnames_master_sheet)
    parse_jaxid_column_dupes(args.master_sample_file,
                                new_master_file,
                                fieldnames_master_sheet,
                                mod_field_name)


    log.info('The modified master sample sheet is now: %s', vis_master_file)


if __name__ == '__main__':
    # TODO: run_tests()

    path = '/Users/bleopold/osdf/ipop_osdf/submit_osdf/coalesce/20160831/'
    class args(object):
        jaxid_list = path + '20160906-JAXId-DB-List-HMP2-updates.csv'
        master_samples = path + 'HMP2_SampleTracking_01Aug2016_MasterSheet.csv'
        visit_numbers = path + 'HMPs_wenyu-visit-list.csv'

    sys.exit(main(args))

