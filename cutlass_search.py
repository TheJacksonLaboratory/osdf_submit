#! /usr/bin/env python
# -*- coding: utf-8 -*-
"""
Functions to query all records for OSDF nodes using OQL (OSDF Query Language).
"""
from __future__ import print_function

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Imports ~~~~
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Core ~~ Imports ~~~~
import os
import json
import importlib

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Third Party ~~ Imports ~~~~
import settings
from cutlass_utils import format_query, write_out_csv, log_it

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Constants ~~~~
filename = os.path.basename(__file__)
log = log_it(filename)

NodeDict = {
            # 'node_type': {
            #     'module_name': 'module_name',
            #     'id_field': 'field_name', },
            'subject': {
                'module_name': 'Subject',
                'id_field': 'rand_subject_id', },
            'visit': {
                'module_name': 'module_name',
                'id_field': 'visit_id', },
            'sample': {
                'module_name': 'module_name',
                'id_field': 'field_name', },
            '16s_dna_prep': {
                'module_name': 'SixteenSDnaPrep',
                'id_field': 'prep_id', },
            '16s_raw_seq_set': {
                'module_name': 'SixteenSRawSeqSet',
                # 'id_field': "os.path.basename(node['local_file'])",
                'id_field': 'comment', },
            '16s_trimmed_seq_set': {
                'module_name': 'SixteenSTrimmedSeqSet',
                'id_field': 'comment', },
            'wgs_dna_prep': {
                'module_name': 'WgsDnaPrep',
                'id_field': 'prep_id', },
            'wgs_raw_seq_set': {
                'module_name': 'WgsDnaPrep',
                'id_field': 'comment', },
            'microb_transcriptomics_raw_seq_set': {
                'module_name': 'WgsDnaPrep',
                'id_field': 'comment', },
           }


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Internal Functions & Classes ~~~~"
def query_all_oql(session, namespace, node_type, query):
    """use oql_query_all_pages for complete sets of results
    [ Requires pre-existing 'iHMPSession'! ]
    """
    cumulative = session.get_osdf().oql_query_all_pages(namespace, query)
    results = cumulative['results']
    # meta = [ r['meta'] for r in results
    #          if r['node_type'] == node_type ]
    nodes = {r['id']:r
             for r in results
             if r['node_type'] == node_type}
    count = len(nodes)
    return (nodes, count)


def query_all_nodes(session, node_name, node_type_name, query):
    """use oql_query_all_pages for complete sets of results"""
    NodeName = importlib.import_module('cutlass.'+node_name)
    log.debug('NodeName: %s', NodeName)
    NodeObject = getattr(NodeName, node_name)
    log.debug('NodeObject: %s', NodeObject)
    NameSpace = getattr(NodeObject, 'namespace')
    log.debug('NameSpace: %s', NameSpace)
    return query_all_oql(session, NameSpace, node_type_name, query)


def retrieve_nodes(session, data_file, node_type):
    """Retrieve node info for each 'internal_id' found in search()
    """
    log.info('Starting retrieval of %ss.', node_type)

    '''write headers if file ne or empty'''
    fields = ['node_type', 'node_id',
              'internal_id', 'parent_linkage',
              'metadata_json']
    try:
        if os.path.exists(data_file):
            pass
    except OSError, e:
        pass
    else:
        dfh = open(data_file, 'a')
        dfh.close()
    finally:
        file_stat = os.stat(data_file)
        if file_stat.st_size <= 0:
            write_out_csv(data_file, fieldnames=fields)

    node_name = NodeDict[node_type]['module_name']
    id_field =  NodeDict[node_type]['id_field']
    qry = format_query("prediabetes", field="tags")
    results, count = query_all_nodes(session, node_name, node_type, qry)
    for node_id, result in results.iteritems():
        try:
            internal_id = result['meta'][id_field]
            # internal_id = getattr(result, "['meta']['"+id_field+"']")
            log.debug('Next data node: %s', str(internal_id))
            # json_meta = json.dumps(result['meta'])
            vals = [result['node_type'],
                    node_id,
                    internal_id,
                    json.dumps(result['linkage']),
                    json.dumps(result['meta']),
                   ]
            log.warn('vals,%s', str(vals))
            write_out_csv(data_file, fieldnames=fields, values=vals)

        except Exception, e:
            log.exception(e)
            raise e

# TODO: for updates, use 'from ast import literal_eval' for str->dict conversion, OR json.load(s)
def update_nodes(session, data_file, node_type):
    """update existing nodes with new info field values"""
    pass


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Node Specific Functions ~~~~"
def query_all_samples(query, session):
    """use oql_query_all_pages for complete sets of results"""
    #TODO: refactor without presuming pre-existing session
    from cutlass.Sample import Sample
    sn = session
    ns = Sample.namespace
    nt = 'sample'
    (meta, count) = query_all_oql(sn,ns,nt,query)
    return (meta, count)

def query_all_visits(query, session):
    """use oql_query_all_pages for complete sets of results"""
    #TODO: refactor without presuming pre-existing session
    from cutlass.Visit import Visit
    return query_all_oql(session, Visit.namespace, 'visit', query)

def query_all_wgsdna(query, session):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.WgsDnaPrep import WgsDnaPrep
    return query_all_oql(session, WgsDnaPrep.namespace, 'wgs_dna_prep', query)

def query_all_wgsraw(query, session):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.WgsRawSeqSet import WgsRawSeqSet
    return query_all_oql(session, WgsRawSeqSet.namespace, 'wgs_raw_seq_set', query)

def query_all_16sdna(query, session):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.SixteenSDnaPrep import SixteenSDnaPrep
    return query_all_oql(session, SixteenSDnaPrep.namespace, '16s_dna_prep', query)

def query_all_16sraw(query, session):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.SixteenSRawSeqSet import SixteenSRawSeqSet
    return query_all_oql(session, SixteenSRawSeqSet.namespace, '16s_raw_seq_set', query)

def query_all_16strimmed(query, session):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.SixteenSTrimmedSeqSet import SixteenSTrimmedSeqSet
    return query_all_oql(session, SixteenSTrimmedSeqSet.namespace, '16s_trimmed_seq_set', query)

def query_all_micrornaraw(query, session):
    """use oql_query_all_pages for complete sets of results"""
    from cutlass.MicrobTranscriptomicsRawSeqSet import MicrobTranscriptomicsRawSeqSet
    return query_all_oql(session, MicrobTranscriptomicsRawSeqSet.namespace, 'microb_transcriptomics_raw_seq_set', query)

