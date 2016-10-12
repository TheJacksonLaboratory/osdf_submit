#!/usr/bin/env python
from __future__ import print_function

import logging, sys, os, time
from cutlass import iHMPSession
from pprint import pprint

# Log It!
def log_it(logname=os.path.basename(__file__)):
    """log_it setup"""
    curtime = time.strftime("%Y%m%d-%H%M")
    logfile = curtime + logname + '.log'

    loglevel = logging.DEBUG
    logFormat = \
        "%(asctime)s %(levelname)5s: %(name)15s %(funcName)s: %(message)s"
    formatter = logging.Formatter(logFormat)

    logging.basicConfig(format=logFormat)
    l = logging.getLogger(logname)
    l.setLevel(loglevel)

    # root = logging.getLogger()
    # root.setLevel(loglevel)

    # fh = logging.FileHandler(logfile, mode='a')
    # fh.setFormatter(formatter)

    # root.addHandler(fh)
    # l.addHandler(fh)

    return l

def load_string_from_file(filename):
    with open(os.path.join(os.curdir, filename)) as f:
        return f.read().strip()

class auth():
    username = load_string_from_file('auth/username.txt')
    password = load_string_from_file('auth/password.txt')

def dprint(*args):
    """dprint is print with a prefix"""
    pref='  -> '
    print(pref,
          ' '.join([str(arg) for arg in args])
         )

import re

try:
    log = log_it()
    session = iHMPSession(auth.username, auth.password)
    info = session.get_osdf().get_info()
    # info = session.get_session()
    # info = session.port
    print(info)
except Exception as e:
    raise e

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def query_all_oql(session, namespace, node_type, query):
    """use oql_query_all_pages for complete sets of results"""
    #TODO: append to cutlass_utils and/or to osdf_python
    cumulative = session.get_osdf().oql_query_all_pages(namespace, query)
    results = cumulative['results']
    # ids = [ r['id'] for r in results
             # if r['node_type'] == node_type ]
    meta = [ r['meta'] for r in results
             if r['node_type'] == node_type ]
    nodes = {r['id']:r['meta']
             for r in results
             if r['node_type'] == node_type}
    count = len(nodes)
    # return (meta, count)
    return (nodes, count)

def query_all_samples(query):
    """use oql_query_all_pages for complete sets of results"""
    #TODO: refactor without presuming pre-existing session
    from cutlass.Sample import Sample
    sn = session
    ns = Sample.namespace
    nt = 'sample'
    (meta, count) = query_all_oql(sn,ns,nt,query)
    return (meta, count)

def query_all_visits(query):
    """use oql_query_all_pages for complete sets of results"""
    #TODO: refactor without presuming pre-existing session
    from cutlass.Visit import Visit
    return query_all_oql(session, Visit.namespace, 'visit', query)

def query_all_wgsdna(query):
    """use oql_query_all_pages for complete sets of results"""
    #TODO: refactor without presuming pre-existing session
    from cutlass.WgsDnaPrep import WgsDnaPrep
    return query_all_oql(session, WgsDnaPrep.namespace, 'wgsdnaprep', query)

def format_query(query, patt='[-. ]', field='rand_subj_id', mode='&&'):
    """format OQL query by removing characterset (e.g. '[-\.]')
           1) Split 'strng' on 'patt';
           2) append 'field' text to each piece;
           3) join using 'mode'
           4) return lowercased strng
    """
    mode = ' '+mode.strip()+' ' # spaces between and/or's and strng splits
    qbits = re.split(patt,query)
    if len(qbits) > 1:
        qbits = ['"{}"[{}]'.format(s,field) for s in qbits]
        #TODO: insert () around first two qbits, plus third, then...
        if len(qbits) > 2:
            strng = "("+mode.join(qbits[0:2])+")"
            for piece in qbits[2:]:
                strng = "("+mode.join([strng,piece])+")"
        else:
            strng = "("+mode.join(qbits)+")"
    else:
        strng = '("{}"[{}])'.format(query,field)
    # log.debug('formatted query: '+ strng.lower())
    return strng.lower()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def WgsDnaSearch():
    """search for node info in the iHMPSession"""
    print('\n____WGS DNA Search____')
    from cutlass.WgsDnaPrep import WgsDnaPrep
    q = format_query("prediabetes", field="tags")
    s = WgsDnaPrep.search(q)
    dprint('count: ',len(s))

    if len(s):
        dprint('prep_ids: ',[ x.prep_id for x in s])
        dprint('node ids: ',[ x.id for x in s])

    dprint('~~~~~wgsDnaPreps~~~~~~~')
    q = '"prediabetes"[tags]'
    (s,c) = query_all_wgsdna(q)
    dprint('query:  ',q)
    dprint('count:  ',c)
    if len(s):
        dprint('count:  ',c)
        dprint('result: ',s)

# WgsDnaSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SixteenSTrimmedSearch():
    print('\n____16S TrimSeq Search____')
    from cutlass.SixteenSTrimmedSeqSet import SixteenSTrimmedSeqSet
    q = format_query("prediabetes", field="tags")
    # q = format_query("raw.fastq", '\.', field="local_file")
    # q = format_query("ZOZOW1T", field="local_file")
    s = SixteenSTrimmedSeqSet.search(q)

    dprint('count: ',len(s))
    if len(s):
        dprint(q+': ',s)
        # dprint('first record fields:')
        # dprint('name: ',s[0].comment)
        # dprint('tags: ',s[0].tags)
        dprint('local_files: ',[ x.local_file for x in s])
        dprint('node ids: ',[ x.id for x in s])

# SixteenSTrimmedSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def SixteenSDnaSearch():
    print('\n____16S DNA Search____')
    from cutlass.SixteenSDnaPrep import SixteenSDnaPrep
    q = format_query("ZOZOW1T", field="name")
    q = format_query("prediabetes", field="tags")

    q = format_query("prediabetes", field="tags")
    # nodeid = 'c22b9238b5b9beec7a9a1fc7c33eca0f'
    # q = format_query(nodeid, field="_id")
    s = SixteenSDnaPrep.search(q)
    if len(s):
        dprint('count:', len(s))
        dprint('name,id: ', [[n.prep_id, n._id] for n in s])

# SixteenSDnaSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SampleSearch():
    print('\n____Sample Search____')
    from cutlass.Sample import Sample

    q = format_query("prediabetes", field="tags")
    (s,c) = query_all_samples(q)
    dprint('count: ',c)
    if len(s):
        dprint('query: ',q)
        # dprint('results: ',s)
        dprint('name''s: ',[ s[k]['name'] for k in s ])
        dprint('node ids: ', s.keys())
        dprint('name,id: ',[ ','.join([s[k]['name'],k]) for k in s ])

# SampleSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def VisitSearch():
    print('\n____Visit Search____')
    from cutlass.Visit import Visit

    q = format_query("prediabetes", field="tags")
    # q = format_query("ZOZOW1T-7024.2_198_1305",
    #                  field="visit_id", patt='[-.]')
    # dprint('query: ',q)
    # node_id = '932d8fbc70ae8f856028b3f67c327c1f'
    # q = format_query(node_id, field="_id")
    (s,c) = query_all_visits(q)
    dprint('query: ',q)
    if len(s):
        # dprint('query: ',q)
        dprint('count: ',c)
        # dprint('results: ',s)
        dprint('visit_id''s: ',[ s[k]['visit_id'] for k in s ])
        dprint('name,id: ',[ ','.join([s[k]['visit_id'],k]) for k in s ])

# VisitSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SubjectSearch():
    print('\n____Subject Search____')
    from cutlass.Subject import Subject
    # q = format_query('ZOZOW1T','','rand_subject_id')
    # q = '"zozow1t"[rand_subject_id]'
    q = format_query("prediabetes", field="tags")
    s = Subject.search(q)

    if len(s):
        dprint('query: ',q)
        dprint('count: ',len(s))
        # dprint('results: ',s)
        dprint('name,id: ',[ ','.join([k.rand_subject_id,k._id]) for k in s ])

# SubjectSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def delete_nodes():
    node_ids = [
        '932d8fbc70ae8f856028b3f67cfd3f64',
        ]
    nodeid = ','.join(node_ids)
    q = format_query(nodeid, patt=",", field="_id", mode="||")

    from cutlass import SixteenSRawSeqSet
    s = SixteenSRawSeqSet.search(q)

    dprint('results to delete: ',s)
    success = [n.delete() for n in s]
    dprint('deleted: ', success.count(True),
           ', erred: ', success.count(False))

delete_nodes()


if __name__ == '__main__':
    pass

#  vim: set ts=4 sw=4 tw=79 et :
