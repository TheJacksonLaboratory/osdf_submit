"""Set of utility functions shared amongst the Stanford/JAXGM OSDF script suite
"""

import os
import sys
import csv
import re
import logging
import time

import cutlass
import settings

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Functional ~~~~~
# Log It!
def log_it(filename=os.path.basename(__file__)):
    """log_it setup"""
    curtime = time.strftime("%Y%m%d-%H%M")
    logfile = curtime + '_osdf_submit.log'

    loglevel = logging.DEBUG
    logFormat="%(asctime)s %(levelname)5s: %(name)15s %(funcName)15s: %(message)s"

    logging.basicConfig(format=logFormat)
    l = logging.getLogger(filename)
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
log = log_it()
# log.setLevel(logging.INFO)

# dump_args decorator
# orig from: https://wiki.python.org/moin/PythonDecoratorLibrary#Easy_Dump_of_Function_Arguments
def dump_args(func):
    "This decorator dumps out the arguments passed to a function before calling it"
    argnames = func.func_code.co_varnames[:func.func_code.co_argcount]
    fname = func.func_name

    def func_args(*args,**kwargs):
        log.debug("'{}' args: {}".format(
            fname, ', '.join('%s=%r' % entry
                for entry in zip(argnames,args) + kwargs.items())) )
            # "'"+fname+"' args: "+', '.join(
            # '%s=%r' % entry
            # for entry in zip(argnames,args) + kwargs.items()))
        return func(*args, **kwargs)

    return func_args

# use example:
# @dump_args
# def f1(a,b,c):
#     print a + b + c
#
# f1(1, 2, 3)

def get_field_header(csv_file):
    """returns first row of csv file as list of fieldnames"""
    log.info('Loading fields from {}'.format(csv_file))
    with open(csv_file) as csvfh:
        try:
            reader = csv.DictReader(csvfh)
            return reader.fieldnames
        except csv.Error as e:
            log.exception('Reading CSV file %s, line %d: %s',
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


def csv_type_sniff(csv_file):
    """find the line/ending type using csv.sniffer"""
    try:
        with open(csv_file, 'rb') as f:
            dialect = csv.Sniffer().sniff(f.read(1024))
            return dialect
    except Exception, e:
        raise e


id_fields = settings.node_id_tracking.id_fields
# sample: subject,69-01,610a491c,study,prediabetes,610a4911a5c

def write_out_csv(csv_file,fieldnames=id_fields,values=[]):
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


def values_to_node_dict(values=[],keynames=id_fields):
    """pass list of lists of values and list of keys of desired dict
       This converts to list of dicts
    """
    from collections import OrderedDict
    log.info('In values_to_node_dict')
    final_list = []

    key_dict = OrderedDict()
    for key in keynames:
        key_dict[key] = ''

    for vals in values:
        l = vals
        d = key_dict.copy()
        k = d.keys()
        for x in range(len(d)):
            lx = l[x] if len(l) > x and l[x] is not None else ''
            d[k[x]] = lx
        # log.debug(d)
        final_list.append(d)

    return final_list

# values_to_node_dict([['foo','bar','1'],['spam','eggs','2','spoof','filled']])


def get_parent_node_id(id_file_name, node_type, parent_id):
    """ read node ids from csv tracking file
        return first "parent" node matching node_type
    """
    log.debug('--> args: '+ id_file_name +','+ node_type +','+ parent_id)
    try:
        for row in load_data(id_file_name):
            if re.match(node_type,row['node_type']):
                # node_ids.append(row.parent_id)
                if re.match(parent_id,row['internal_id']):
                    log.debug('--> matching node row: '+ str(row))
                    log.debug('parent type: {}, osdf_node_id: {}'.format(
                        node_type,str(row['osdf_node_id'])))
                    return row['osdf_node_id']
    except Exception, e:
        raise e

# @dump_args
def get_child_node_ids(id_file_name, node_type, parent_id):
    """ read node ids from csv tracking file
        yield "child" node ids matching node_type
    """
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


# @dump_args
def format_query(strng, patt='[-. ]', field='rand_subj_id', mode='&&'):
    """format OQL query by removing characterset (e.g. '[-\.]')
           1) Split 'strng' on 'patt';
           2) append 'field' text to each piece;
           3) join using 'mode'
           4) return lowercased strng
    """
    mode = ' '+mode.strip()+' ' # spaces between and/or's and strng splits
    strngs = re.split(patt,strng)
    if len(strngs) > 1:
        strngs = ['"{}"[{}]'.format(s,field) for s in strngs]
        #TODO: insert () around first two strngs, plus third, then...
        if len(strngs) > 2:
            strng = "("+mode.join(strngs[0:2])+")"
            for piece in strngs[2:]:
                strng = "("+mode.join([strng,piece])+")"
        else:
            strng = "("+mode.join(strngs)+")"
    else:
        strng = '("{}"[{}])'.format(strng,field)
    log.debug('formatted query: '+ strng.lower())
    return strng.lower()


def list_tags(node_tags, *tags):
    """generate list of all tags to be used in add_tag method, then rm dupes"""
    end_tags = []
    [end_tags.append(t) for t in node_tags]
    [end_tags.append(t) for t in tags]
    return sorted(set(end_tags))


def run_tests():
    log = log_it('Testing functions')
    tests = 0
    failures = 0

    from tempfile import mkstemp

    # test csv read/write/headers:
    tests += 1
    (csvfh1, csv_file1) = mkstemp('test','test',text=True)
    field_names1 = ['one','two','three']
    # print('field_names1: '+str(field_names1))
    csv_values1 = [{'one':'a', 'two':'b', 'three':'c'},
                   {'one':'d', 'two':'e', 'three':'f'}]
    write_out_csv(csv_file1,field_names1) #write headers
    write_out_csv(csv_file1,field_names1,csv_values1)
    field_names_read1 = get_field_header(csv_file1)
    # print('field_names_read1: '+str(field_names_read1))

    (csvfh2, csv_file2) = mkstemp('test2','test',text=True)
    values2 = [['g', 'h', 'i'], ['j', 'k', 'l']]
    csv_values2 = values_to_node_dict(values2,field_names_read1)
    write_out_csv(csv_file2,field_names1) #write headers
    write_out_csv(csv_file2,field_names_read1,csv_values2)
    field_names_read2 = get_field_header(csv_file2)
    # print('field_names_read2: '+str(field_names_read2))

    failures += 1 if field_names1 != field_names_read2 else 0

    log.warn('Tests run: %s', tests)
    log.warn('Test failures: %s', failures)


if __name__ == '__main__':
    run_tests()
    pass
