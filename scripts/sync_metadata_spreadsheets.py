#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File: sync_metadata_spreadsheets.py
Intent: get data from several spreadsheet sources, glean relevant, output to
        new sample, dnaprep(&? raw and trimmed) sheets.
Author: Benjamin Leopold
Date: 2016-07-25 10:46:07-0400
"""

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~~
import sys
import os
import re
import csv
import logging
import time

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
    logFormat = "%(asctime)s %(levelname)5s: %(message)s"

    logging.basicConfig(format=logFormat)
    logger = logging.getLogger(logname)
    logger.setLevel(loglevel)

    formatter = logging.Formatter(logFormat)

    fh = logging.FileHandler(logfile, mode='a')
    fh.setLevel(loglevel)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
log = log_it()

# def get_output(cmd_string, stderr=STDOUT, shell=True,
#                universal_newlines=True, **kwargs):
#     """wrapper for subprocess.call; takes single or list as arg"""
#     return check_output(cmd_string, stderr=stderr, shell=shell,
#                         universal_newlines=universal_newlines, **kwargs)

def get_field_header(csv_file):
    """returns first row of csv file as list of fieldnames"""
    log.info('Loading fields from %s', csv_file)
    with open(csv_file) as csvfh:
        try:
            reader = csv.DictReader(csvfh)
            return reader.fieldnames
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

def load_data(csv_file):
    """yield row dicts from csv_file using DictReader
    """
    log.info('Loading rows from %s', csv_file)
    with open(csv_file, 'U') as csvfh:
        reader = csv.DictReader(csvfh)
        # log.debug('csv dictreader opened')
        try:
            for row in reader:
                # log.debug(row)
                yield row
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
                          csv_file, reader.line_num, e)

def write_out_csv(csv_file, fieldnames, values=[]):
    """write all values in csv format to outfile.
    Values is list of dicts w/ keys matching fieldnames.
    To write header to file, omit `values`
    """
    log.info('Writing csv to %s', csv_file)
    try:
        with open(csv_file, 'a') as csvout:
            writer = csv.DictWriter(csvout, fieldnames)
            if values:
                try:
                    for row in values:
                        if isinstance(row, dict):
                            # log.debug(row)
                            writer.writerow(row)
                except Exception as e:
                    log.exception('Writing CSV file %s, %s', csv_file, str(e))
                    raise e
            else:
                log.info('Writing header of fieldnames to %s', csv_file)
                writer.writeheader()
    except IOError as e:
        raise e

def write_csv_headers(filenames,fieldnames):
    """init csv files with fieldname headers"""
    [write_out_csv(name, fieldnames=fieldnames)
     for name in filenames
     if not os.path.exists(name)]

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

def notempty(value):
    return value if len(value) > 0 else ''

def add_jaxid_to_master_list(jaxids, samples, new_master_file):
    """determine and add values to jaxid_tissue column"""
    log.info('in add_jaxid_to_master_list function')
    matched = 0
    for row in samples:
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
        # if notempty(row['jaxid_sample']):
        #     continue
        # else:
        # check jaxid_db for match
        for idrow in jaxids:
            # collab_id = idrow['collab_id'][0:len(sample_id)+1]
            collab_id = idrow['collab_id']
            nucleic_acid = idrow['nucleic_acid']
            if idrow['parent_id'] == 'received':
                if body_site == trans_sample_type(idrow['sample']):
                    sample = sample_rand.lower()
                    collab = collab_id.lower()
                    if sample == collab:
                        if idrow['id_type'] == material_recd == 'specimen':
                            if (body_site == "nasal" and\
                                re.match(dna_vs_rna,idrow['notes']))\
                               or body_site == "stool":
                                row['Tissue JAX-ID'] += ' ' + idrow['jaxid']
                                matched +=1
                                log.debug('Matched: tissue %s == %s',
                                          row['Tissue JAX-ID'], idrow['jaxid'])

                        elif idrow['id_type'] == 'extraction':
                            if nucleic_acid == material_recd == 'gDNA':
                                row['gDNA JAX-ID'] += ' ' + idrow['jaxid']
                                matched +=1
                                log.debug('Matched: gDNA %s == %s',
                                          row['gDNA JAX-ID'], idrow['jaxid'])
                            if nucleic_acid == material_recd and \
                                material_recd in \
                                ['cDNA', 'Rib Depleted RNA', 'Total RNA']:
                                row['RNA JAX-ID'] += ' ' + idrow['jaxid']
                                matched +=1
                                log.debug('Matched: RNA %s == %s',
                                          row['RNA JAX-ID'], idrow['jaxid'])

        write_out_csv(new_master_file,
                      fieldnames=args.fieldnames.master_list,
                      values=[row])

    return matched

def add_visit_id_to_master_list(visit_ids, master_samples,
                                master_filename, new_master_file):
    """add columns to determine visit_ids
       using visit name, number and interval
    """
    log.info('mod\'ing master list pre-coalescing, adding visit_id fields')

    visit_fieldnames = get_field_header(master_filename)
    for elem in ['study_date', 'interval', 'visit_number','visit_id']:
        visit_fieldnames.insert(8, elem)

    write_csv_headers([new_master_file],fieldnames=visit_fieldnames)

    for row in master_samples:
        sample_final = row['Final Sample Name']
        # reset to blank all fields being added
        row['visit_number'] = ''
        row['interval'] = ''
        row['study_date'] = ''

        for vrow in visit_ids:
            visit_id = vrow['Visit ID']
            if sample_final == visit_id:
                row['visit_number'] = vrow['visit_num']
                row['interval'] = vrow['interval']
                row['study_date'] = vrow['Study Day']

        write_out_csv(new_master_file,
                      fieldnames=visit_fieldnames,
                      values=[row])

def add_visit_id_to_master_list_old_name(visit_ids, master_samples,
                                         master_filename, new_master_file):
    """add columns to determine visit_ids
       using visit name, number and interval
    """
    log.info('mod\'ing master list pre-coalescing, adding visit_id fields')

    visit_fieldnames = get_field_header(master_filename)
    # for elem in ['study_date', 'interval', 'visit_number','visit_id']:
    #     visit_fieldnames.insert(8, elem)

    write_csv_headers([new_master_file],fieldnames=visit_fieldnames)

    for row in master_samples:
        if row['visit_number'] == 'UNK':
            ######################################################### do stuff
            # sample_final = row['Final Sample Name']
            sample_orig = row['Original Sample Name']
            # visit_name = row['visit name']
            # row['visit_number'] = ''
            # row['interval'] = ''
            # row['study_date'] = ''

            for vrow in visit_ids:
                visit_id = vrow['Visit ID']
                if sample_orig == visit_id:
                    row['visit_number'] = vrow['visit_num']
                    row['interval'] = vrow['interval']
                    row['study_date'] = vrow['Study Day']

        write_out_csv(new_master_file,
                      fieldnames=visit_fieldnames,
                      values=[row])

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ In the Mainline ~~~~~
def run_tests(test_type='all'):
    # csv_dialect = csv_type_sniff(filename)
    """run tests of all usage scenarios"""
    csv_file = 'tmp_file.csv'
    fieldname_list = ['foo', 'bar', 'spam']
    value_dict = dict(foo=1, bar=2, spam=123)
    assert write_out_csv(csv_file, fieldname_list), \
           log.error('write_out_csv headers test failed!')

    assert write_out_csv(csv_file, fieldname_list, \
                         values=[value_dict, value_dict]), \
           log.error('write_out_csv test failed!')

    assert load_data(csv_file), \
           log.error('load_data test failed!')

    pass

def main(args):
    """manage imports, cross-matches"""

    # import dicts from all files
    # sample_sheets = import_whole_csv(args.sample_sheet)
    master_sample = import_whole_csv(args.master_samples)
    # changed_names = import_whole_csv(args.master_changed)
    # jaxid_db_list = import_whole_csv(args.jaxid_list)
    visit_numbers = import_whole_csv(args.visit_numbers)
    log.info('Done loading files')

    new_master_file = args.master_samples[0:-4] + '_new.csv'
    # log.debug('new_master_file: %s', new_master_file)

    ### pre-coalesce, ensure all sample jaxid's in master list for comparison
    # log.info('mod\'ing master list pre-coalescing')

    # write_csv_headers([new_master_file],fieldnames=args.fieldnames.master_list)
    # matched_total = add_jaxid_to_master_list(jaxid_db_list,
    #                                            master_sample,
    #                                            new_master_file)

    # add_visit_id_to_master_list(visit_numbers, master_sample,
    #                             args.master_samples, new_master_file)

    add_visit_id_to_master_list_old_name(visit_numbers, master_sample,
                                         args.master_samples, new_master_file)

    ### cross-matching
    # TODO: generate logic for cross-matching, start in pseudo code



if __name__ == '__main__':
    # TODO: run_tests()

    filepath = '/Users/bleopold/osdf/ipop_osdf/coalesce/'
    class args(object):
        sample_sheet = filepath + 'sample_sheets_0713-concat.csv'
        jaxid_list = filepath + 'JAXId-DB-List-2016-07-05-HMP2.csv'
        master_samples = filepath + 'HMP2_metadata-MasterSampleSheet_0713_new_new.csv'
        master_changed = filepath + 'samples_0713_subset_changed.csv'
        visit_numbers = filepath + 'HMPs_wenyu-visit-list.csv'

        class fieldnames:
            jaxid_db = ['id','project_code','creation_date','collab_id',
                        'collab_id_old',
                        'sample','nucleic_acid','seq_type','jaxid','parent_id',
                        'id_type','entered_into_lims','external_data','notes']
            sample_sheet = ['run_date','run_id','seq_type','seq_model',
                            'run_name','collab_id_old','index1_id',
                            'index1_seq','index2_id','index2_seq','use_bases',
                            'file_base_old','filebase_new','split_filebase',
                            'jaxid_library','seq_type','sample_type_code',
                            'tech_rep','bio_rep','zero_flags','collab_id_old',
                            'run_id']
            master_list = ['Sort#', 'Subject Name', 'Random Subject ID',
                           'Original Sample Name', 'Corrected Sample Name',
                           'Final Sample Name', 'Random Final Sample Name',
                           'Project Code',
                           'Tissue JAX-ID', 'RNA JAX-ID', 'gDNA JAX-ID',
                           'Specimen', 'Nucleic Acid', 'HMP2 Study',
                           'Visit Type', 'JAX_Visit_Code', 'Subject_timeline',
                           'Collection Date', 'DNAorRNA', 'Additional Info',
                           'Bag/Box Info', 'Shipment Date', 'Date Received',
                           'Lot #', '16S Required', '16S Sequencing Done?',
                           'mWGS Required?', 'WGS Done?', 'RNA-Seq Required?',
                           'RNA-Seq Done?', 'Name Change Date', 'JAX Comments',
                           'Run 1 Sequencing Platform', 'Run 1 Name',
                           'Run 1 Flow cell', 'Run 1 Finish Date',
                           'Run 2 Sequencing Platform', 'Run 2 Name',
                           'Run 2 Flow cell', 'Run 2 Finish Date',
                           'Run 3 Sequencing Platform', 'Run 3 Name',
                           'Run 3 Flow cell', 'Run 3 Finish Date',
                           'Sequencing Platform (Run 1)', 'Run 1 Name',
                           'Run 1 Flow Cell', 'Run 1 Finish Date',
                           'Run 1 Library Protocol',
                           'Run 1 # of Reads (Just R1)', 'Run 1 Genome Size',
                           'Run 1 Coverage', 'Sequencing Platform (Run 2)',
                           'Run 2 Name', 'Run 2 Flow Cell',
                           'Run 2 Finish Date', 'Run 2 Library Protocol',
                           'Run 2 # of Reads (Just R1)', 'Run 2 Genome Size',
                           'Run 2 Coverage', 'Sequencing Platform (Run 3)',
                           'Run 3 Name', 'Run 3 Flow Cell',
                           'Run 3 Finish Date', 'Run 3 Library Protocol',
                           'Run 3 # of Reads (Just R1)', 'Run 3 Genome Size',
                           'Run 3 Coverage', 'Sequencing Platform (Run 4)',
                           'Run 4 Name', 'Run 4 Flow Cell',
                           'Run 4 Finish Date', 'Run 4 Library Protocol',
                           'Run 4 # of Reads (Just R1)', 'Run 4 Genome Size',
                           'Run 4 Coverage', 'Sequencing Platform (Run 4)',
                           'Run 4 Name', 'Run 4 Flow Cell',
                           'Run 4 Finish Date', 'Run 4 Library Protocol',
                           'Run 4 # of Reads (Just R1)', 'Run 4 Genome Size',
                           'Run 4 Coverage', 'Sequencing Platform (Run 1)',
                           'Run 1 Name', 'Run 1 Flow Cell',
                           'Run 1 Finish Date', 'Run 1 Library Protocol',
                           'Sequencing Platform (Run 2)', 'Run 2 Name',
                           'Run 2 Flow Cell', 'Run 2 Finish Date',
                           'Run 2 Library Protocol']
            visit_numbers = ['Subject ID',
                             'Visit ID',
                             'visit_num',
                             'interval',
                             'Study Day',
                             'Microbiome Note']

            final = []


    sys.exit(main(args))

