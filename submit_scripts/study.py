#!/usr/bin/env python

import json
import logging
from pprint import pprint
import yaml

from cutlass_utils import save_if_valid, load_string_from_file
from cutlass import iHMPSession
from cutlass import Study as OSDFNode

# Study info yaml file name
yaml_info_file = '../data_files/study_info.yaml'

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')

session = iHMPSession(username, password)

print("Required fields: {}".format(OSDFNode.required_fields()))
node = OSDFNode()

# load info from yaml_info_file
node_info = yaml.load(open(yaml_info_file))

node.name = node_info['name']
node.description = node_info['description']
node.center = node_info['center']
node.contact = node_info['contact']
node.srp_id = node_info['srp_id']
node.tags = node_info['tags']
node.links = node_info['links']

# print(node.to_json(indent=2))
save_if_valid(node, OSDFNode)
