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
import time

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Third Party ~~ Imports ~~~~
import settings
from cutlass_utils import format_query, load_data, write_out_csv, \
                          get_field_header, log_it, get_cur_datetime, \
                          values_to_node_dict
from osdf import OSDF
# for osdf.oql_query_all_pages, osdf.edit_node

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Constants ~~~~
filename = os.path.basename(__file__)
log = log_it(filename)
curtime = time.strftime("%Y-%m-%d %H:%M")

NodeDict = {
            # 'node_type': {
            #     'module_name': 'module_name',
            #     #'id_field': "os.path.basename(node['local_file'])",
            #     'id_field': 'field_name',
            #     'load_method': 'load_....', },
            'subject': {
                'module_name': 'Subject',
                'id_field': 'rand_subject_id',
                'load_method': '', },
            'visit': {
                'module_name': 'Visit',
                'id_field': 'visit_id',
                'load_method': '', },
            'sample': {
                'module_name': 'Sample',
                'id_field': 'name',
                'load_method': '', },
            '16s_dna_prep': {
                'module_name': 'SixteenSDnaPrep',
                'id_field': 'prep_id',
                'load_method': 'load_sixteenSDnaPrep', },
            '16s_raw_seq_set': {
                'module_name': 'SixteenSRawSeqSet',
                'id_field': 'comment',
                'load_method': 'load_16s_raw_seq_set', },
            '16s_trimmed_seq_set': {
                'module_name': 'SixteenSTrimmedSeqSet',
                'id_field': 'comment',
                'load_method': 'load_sixteenSTrimmedSeqSet', },
            'wgs_dna_prep': {
                'module_name': 'WgsDnaPrep',
                'id_field': 'prep_id',
                'load_method': '', },
            'wgs_raw_seq_set': {
                'module_name': 'WgsRawSeqSet',
                'id_field': 'comment',
                'load_method': '', },
            'microb_transcriptomics_raw_seq_set': {
                'module_name': 'MicrobTranscriptomicsRawSeqSet',
                'id_field': 'comment',
                'load_method': '', },
           }


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Internal Functions & Classes ~~~~

def byteify(input):
    if isinstance(input, dict):
        return {byteify(key):byteify(value) for key,value in input.iteritems()}
    elif isinstance(input, list):
        return [byteify(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input


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


def query_all(session, query, namespace='ihmp'):
    """use osdf.oql_query_all_pages for complete sets of results regardless of node_type
    [ Requires pre-existing 'iHMPSession'! ]
    """
    try:
        cumulative = session.get_osdf().oql_query_all_pages(namespace, query)
        results = cumulative['results']
        nodes = {r['id']:r for r in results}
    except Exception as e:
        raise e
    return nodes


def retrieve_query_all(session, query, data_file='node_retrievals_.csv'):
    """wrapper for 'query_all' to retrieve all matching node_types"""
    log.info('Starting retrieval of ""%s".', query)

    '''write headers if file ne or empty'''
    fields = ['node_type', 'id', 'internal_id', 'linkage', 'meta', 'ns', 'ver', 'acl', 'date_retrieved']
    log.warn('fields,%s', str(fields))
    try:
        write_out_csv(data_file, fieldnames=fields)
    except Exception as e:
        log.exception('Write headers, Except... %s', e)

    results = query_all(session, query)
    log.info("Number of Query Results: %s", len(results))
    for node_id, result in results.iteritems():
        try:
            node_type = result['node_type']
            if node_type in NodeDict:
                id_field = NodeDict[node_type]['id_field']
                internal_id = result['meta'][id_field]
            else:
                internal_id = node_type
            # id_field = getattr(NodeDict, "[node_type]['id_field']", "unk")
            # internal_id = getattr(result, "['meta'][id_field]", "unk")
            log.debug('Current data node: %s', str(internal_id))
            values = [node_type,
                      node_id,
                      internal_id,
                      json.dumps(result['linkage']),
                      json.dumps(result['meta']),
                      result['ns'],
                      result['ver'],
                      json.dumps(result['acl']),
                      get_cur_datetime(),
                     ]
            log.warn('vals,%s', str(values))
            vals = values_to_node_dict([values], fields)
            write_out_csv(data_file, fieldnames=fields, values=[vals])
            write_out_csv(data_file, fieldnames=fields, values=[values])

        except Exception, e:
            log.exception(e)
            raise e
    pass


def query_all_nodes(session, node_name, node_type_name, query):
    """use oql_query_all_pages for complete sets of results"""
    NodeName = importlib.import_module('cutlass.'+node_name)
    log.debug('NodeName: %s', NodeName)
    NodeObject = getattr(NodeName, node_name)
    # log.debug('NodeObject: %s', NodeObject)
    NameSpace = getattr(NodeObject, 'namespace')
    # log.debug('NameSpace: %s', NameSpace)
    return query_all_oql(session, NameSpace, node_type_name, query)


def retrieve_nodes(session, data_file, node_type):
    """Retrieve node info for each 'internal_id' found in search()
    """
    log.info('Starting retrieval of %ss.', node_type)

    '''write headers if file ne or empty'''
    fields = ['node_type', 'id', 'internal_id', 'linkage', 'meta', 'ver', 'date_retrieved']
    log.warn('fields,%s', str(fields))
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
            log.debug('Next data node: %s', str(internal_id))
            # json_meta = json.dumps(result['meta'])
            vals = [node_type,
                    node_id,
                    internal_id,
                    json.dumps(result['linkage']),
                    json.dumps(result['meta']),
                    result['ver'],
                    get_cur_datetime(),
                   ]
            log.warn('vals,%s', str(vals))
            write_out_csv(data_file, fieldnames=fields, values=[vals])

        except Exception, e:
            log.exception(e)
            raise e


def import_node_and_loader(session, node_name, load_method_name):
    """return instantiated node object and loader method"""
    NodeName = importlib.import_module('cutlass.'+node_name)
    # log.debug('NodeName: %s', NodeName)
    NodeObject = getattr(NodeName, node_name)
    # log.debug('NodeObject: %s', NodeObject)
    NodeLoader = getattr(NodeObject, load_method_name)
    # log.debug('NodeLoader: %s', NodeLoader)
    return NodeObject, NodeLoader


def update_node(session, record, node_type):
    """update existing node with new info field values"""
    log.info('Starting update of %ss.', node_type)
    osdf = session.get_osdf()

    node_id = record['node_id']
    try:
        log.info('Checking for %s node: %s', node_type, node_id)
        result = osdf.get_node(node_id)
        # log.warn("node result: %s", result)
        if result:
            internal_id = record['internal_id']
            # internal_id = result['meta']['comment']
            log.info('Updating %s node: %s', node_type, str(internal_id))
            # log.debug('linkage: %s', record['linkage'] )
            if record['linkage']:
                result['linkage'] = json.loads(record['linkage'])
            if record['meta']:
                result['meta'] = json.loads(record['meta'])
            # log.warn('node update: %s', result)
            try:
                osdf.edit_node(result)
                log.info("Saved updated %s: %s, %s", node_type, node_id, internal_id)
            except Exception, e:
                log.exception('node update problem: %s', e)
                # raise e
        else:
            log.info('unable to get_node: %s', node_id)

    except Exception, e:
        log.exception(e)
        # raise e


def update_nodes(session, data_file, node_type):
    """Retrieve node info for each 'internal_id' found in search()
    """
    log.info('Starting updates of %ss.', node_type)
    data_file_log = data_file + '.updated.csv'

    for record in load_data(data_file):
        try:
            # log.debug("record: %s", record)
            update_node(session, record, node_type)
        except Exception, e:
            log.exception(e)
            raise e


def update_nodes_general(session, data_file):
    """Retrieve node info for each 'internal_id' found in search()
    """
    data_file_log = data_file + '.updated.csv'

    for record in load_data(data_file):
        try:
            log.info('Starting updates of %ss.', record['node_type'])
            # log.debug("record: %s", record)
            update_node(session, record, record['node_type'])
        except Exception, e:
            log.exception(e)
            raise e


def delete_node(session, node_id):
    """delete existing node """
    osdf = session.get_osdf()
    try:
        log.info('Starting delete of node with id: %s.', node_id)
        if osdf.get_node(node_id):
            osdf.delete_node(node_id)
            log.info("Deleted: %s", node_id)
    except Exception, e:
        log.exception('node delete problem: %s', e)
        # raise e


def delete_nodes(session, node_ids):
    """Delete all nodes in passed dict"""
    log.info('Starting deletions of nodes')
    for internal_id, node_id in node_ids.iteritems():
        try:
            delete_node(session, node_id)
        except Exception, e:
            log.exception(e)
            # raise e


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

