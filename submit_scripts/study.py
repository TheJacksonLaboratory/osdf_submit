#!/usr/bin/env python

import logging

from cutlass_utils import save_if_valid, \
    load_string_from_file, \
    load_config_from_file
from cutlass import iHMPSession
from cutlass import Study as OSDFNode

# Study info file name
info_file = '../data_files/study_info.yaml'

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')

session = iHMPSession(username, password)

print("Required fields: {}".format(OSDFNode.required_fields()))

# load info sets from info_file
config = load_config_from_file(info_file)
for node_info in config:
    node = OSDFNode()

    node.name = node_info['name']
    node.description = node_info['description']
    node.center = node_info['center']
    node.contact = node_info['contact']
    node.srp_id = node_info['srp_id']
    node.subtype = node_info['subtype']
    node.tags = node_info['tags']
    node.links = node_info['links']

    # print(node.to_json(indent=2))
    save_if_valid(node, OSDFNode)
