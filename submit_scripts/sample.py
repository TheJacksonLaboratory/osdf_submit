#!/usr/bin/env python

import logging

from cutlass_utils import save_if_valid, \
    load_string_from_file, \
    load_config_from_file
from cutlass import iHMPSession
from cutlass import Sample as OSDFNode

# Subject info file name
info_file = '../data_files/samples_info.yaml'

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')

session = iHMPSession(username, password)

print("Required fields: {}".format(OSDFNode.required_fields()))

# load info sets from info_file
config = load_config_from_file(info_file)
for node_info in config:
    node = OSDFNode()

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

    # print(node.to_json(indent=2))
    save_if_valid(node, OSDFNode)

