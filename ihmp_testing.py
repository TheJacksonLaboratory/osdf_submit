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

    root = logging.getLogger()
    root.setLevel(loglevel)

    fh = logging.FileHandler(logfile, mode='a')
    fh.setFormatter(formatter)

    root.addHandler(fh)
    l.addHandler(fh)

    return l

def load_string_from_file(filename):
    with open(os.path.join(os.curdir, filename)) as f:
        return f.read().strip()

class auth():
    username = load_string_from_file('auth/username.txt')
    password = load_string_from_file('auth/password.txt')

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

def dprint(*args):
    """dprint is print with a prefix"""
    pref='  -> '
    print(pref,
          ' '.join([str(arg) for arg in args])
         )

import re

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
    # log.debug('formatted query: '+ strng.lower())
    return strng.lower()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def WgsDnaSearch():
    """search for node info in the iHMPSession"""
    print('\n____WGS DNA Search____') 
    from cutlass.WgsDnaPrep import WgsDnaPrep
    q = format_query("ZOZOW1T", field="name")
    q = format_query("prediabetes", field="tags")
    s = WgsDnaPrep.search(q)
    dprint('count: ',len(s))

    if len(s):
        dprint('count:', s)
        # dprint('first record fields:')
        # dprint('name: ',s[0].prep_id)
        # dprint('tags: ',s[0].tags)
        dprint('prep_ids: ',[ x.prep_id for x in s])
        dprint('node ids: ',[ x.id for x in s])

    # deletion
    # success = [n.delete() for n in s]
    # dprint('-> deleted: ',success)

    q = '"prediabetes"[tags]'
    (s,c) = query_all_wgsdna(q)
    if len(s):
        dprint('~~~~~wgsDnaPreps~~~~~~~')
        dprint('query: ',q)
        dprint('count: ',c)
# WgsDnaSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SixteenSTrimmedSearch():
    print('\n____16S TrimSeq Search____')
    from cutlass.SixteenSTrimmedSeqSet import SixteenSTrimmedSeqSet
    q = format_query("prediabetes", field="tags")
    # q = format_query("raw.fastq", '\.', field="local_file")
    # q = format_query("ZOZOW1T", field="local_file")
    s = SixteenSTrimmedSeqSet.search(q)

    # deletion
    # success = [n.delete() for n in s]
    # dprint('deleted: ',success)

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

def SixteenSRawSearch():
    print('\n____16S RawSeq Search____')
    from cutlass.SixteenSRawSeqSet import SixteenSRawSeqSet
    q = format_query("prediabetes", field="tags")
    # q = format_query("raw.fastq", field="comment")
    # q = format_query("ZOZOW1T", field="local_file")
    s = SixteenSRawSeqSet.search(q)

    # deletion
    # success = [n.delete() for n in s]
    # dprint('deleted: ',success)

    dprint('count: ',len(s))
    if len(s):
        dprint(q+': ',s)
        # dprint('first record fields:')
        # dprint('name: ',s[0].comment)
        # dprint('tags: ',s[0].tags)
        # dprint('body_site''s: ',[ x.body_site for x in s])
        dprint('local_files: ',[ x.local_file for x in s])
        dprint('node ids: ',[ x.id for x in s])
# SixteenSRawSearch():

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def SixteenSDnaSearch():
    print('\n____16S DNA Search____')
    from cutlass.SixteenSDnaPrep import SixteenSDnaPrep
    q = format_query("ZOZOW1T", field="name")
    q = format_query("prediabetes", field="tags")
    s = SixteenSDnaPrep.search(q)
    dprint('count: ',len(s))

    # deletion
    # success = [n.delete() for n in s]
    # dprint('-> deleted: ',success)

    if len(s):
        dprint('count:', s)
        # dprint('first record fields:')
        # dprint('name: ',s[0].prep_id)
        # dprint('tags: ',s[0].tags)
        # dprint('body_site''s: ',[ x.body_site for x in s])
        dprint('prep_ids: ',[ x.prep_id for x in s])
        dprint('node ids: ',[ x.id for x in s])
# SixteenSDnaSearch():

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SampleSearch():
    print('\n____Sample Search____')
    from cutlass.Sample import Sample
    q = format_query("prediabetes", field="tags")
    # q = '"vaginal"[body_site]'
    # q = format_query('ZL9BTWF-1023-AL1','-','name')
    # q = format_query("ZLGD9M0-10", field="name")
    # q = format_query("ZOZOW1T-1010", field="name")
    # q = format_query("ZOZOW1T", field="name")
    s = Sample.search(q)
    if len(s):
        dprint(q+': ',s)
        # dprint('first record fields:')
        # dprint('name: ',s[0].name)
        # dprint('body_site: ',s[0].body_site)
        # dprint('tags: ',s[0].tags)
        # dprint('body_site''s: ',[ x.body_site for x in s])
        dprint('names: ',[ x.name for x in s])
        # dprint('node ids: ',[ x.id for x in s])
    dprint('count: ',len(s))

    # deletion
    # success = [n.delete() for n in s]
    # dprint('deleted: ',success)

    q = '"prediabetes"[tags]'
    (s,c) = query_all_samples(q)
    if len(s):
        dprint('~~~~~all~~~~~samples~~~~~~~')
        dprint('query: ',q)
        # dprint('results: ',s)
        # dprint('body_site''s: ',[ x.body_site for x in s])
        dprint('count: ',c)
        print([n['id'] for n in s])
        # print('names: '+ [n['id']['name'] for n in s])
SampleSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def VisitSearch():
    print('\n____Visit Search____')
    from cutlass.Visit import Visit
    q = format_query("prediabetes", field="tags")
    # q = format_query("ZOZOW1T-1010", field="visit_id")
    # q = format_query("ZOZOW1T-E11.1A", field="visit_id")
    s = '' #Visit.search(q)
    dprint('count: ',len(s))

    if len(s):
        dprint(q+': ',s)
        # dprint('first record fields:')
        # dprint('id: ',s[0].id)
        # dprint('tags: ',s[0].tags)
        # dprint('samples: ',s[0].samples)
        dprint('visit_id''s: ',[ x.visit_id for x in s])
        dprint('node ids: ',[ x.id for x in s])

    # deletion
    # success = [n.delete() for n in s]
    # dprint('-> deleted: ',success)

    q = '"prediabetes"[tags]'
    (s,c) = query_all_visits(q)
    if len(s):
        dprint('~~~~~visits~~~~~~~')
        dprint('query: ',q)
        # dprint('results: ',s)
        # dprint('body_site''s: ',[ x.body_site for x in s])
        dprint('count: ',c)
# VisitSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def SubjectSearch():
    print('\n____Subject Search____')
    from cutlass.Subject import Subject
    q = format_query("prediabetes", field="tags")
    # q = format_query('ZOZOW1T','','rand_subject_id')
    # q = '"ZOZOW1T"[rand_subject_id]'
    # q = '"zozow1t"[rand_subject_id]'
    # q = format_query("ZOZOW1T", field="rand_subject_id")
    s = Subject.search(q)
    dprint('count: ',len(s))

    # deletion
    # success = [n.delete() for n in s]
    # dprint('deleted: ',success)

    if len(s):
        dprint('results ',q+': ',s)
        # dprint('subject_id: ',s[0].rand_subject_id)
        # dprint('id: ',s[0].id)
        # dprint('tags: ',s[0].tags)
        dprint('subject_id''s: ',[ x.rand_subject_id for x in s])
        dprint('node ids: ',[ x.id for x in s])
# SubjectSearch()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if __name__ == '__main__':
    pass

#  vim: set ts=4 sw=4 tw=79 et :
