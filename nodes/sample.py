#!/usr/bin/env python

import logging

# from cutlass_utils import default_mixs_dict
from cutlass import Sample

print("Required fields: {}".format(OSDFNode.required_fields()))

# load info sets from info_file
    node.visit_id = node_info['visit_id']
    node.visit_number = node_info['visit_number']
    node.date = str(node_info['date']) # cutlass expects 'str' format, not 'datetime'
    node.interval = node_info['interval']
    node.clinic_id = node_info['clinic_id']
    node.fma_body_site = node_info['fma_body_site'] 
    node.tags = node_info['tags']
    node.links = node_info['links']
    node.mixs = node_info['mixs']
    node._attribs = node_info['attributes']

