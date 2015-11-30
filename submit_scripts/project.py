#!/usr/bin/env python

import json
import logging

from cutlass_utils import save_if_valid, \
    load_string_from_file, \
    load_config_from_file
from cutlass import iHMPSession
from cutlass import Project as OSDFNode

# Project info yaml file name
info_file = '../data_files/project_info.yaml'

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')

session = iHMPSession(username, password)

print("Required fields: {}".format(OSDFNode.required_fields()))

# load project info from info_file
config = load_config_from_file(info_file)
for node_info in config:
    node = OSDFNode()

    node.name = node_info['name']
    node.description = node_info['description']
    node.center = node_info['center']
    node.contact = node_info['contact']
    node.srp_id = node_info['srp_id']
    node.tags = node_info['tags']
    node.mixs = node_info['mixs']

    # print(node.to_json(indent=2))
    save_if_valid(node, OSDFNode)
