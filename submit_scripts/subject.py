#!/usr/bin/env python

import json
import logging
from pprint import pprint
import yaml

from cutlass_utils import save_if_valid, load_string_from_file
from cutlass import iHMPSession
from cutlass import Subject as OSDFNode

# Subject info yaml file name
yaml_info_file = '../data_files/subjects_info.yaml'

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')

session = iHMPSession(username, password)

print("Required fields: {}".format(OSDFNode.required_fields()))

# load  subject info sets from yaml_info_file
for node_info in yaml.load_all(open(yaml_info_file)):
    node = OSDFNode()

    node.rand_subject_id = node_info['subject_id']
    node.gender = node_info['gender']
    node.race = node_info['race']
    node.tags = node_info['tags']
    node.links = node_info['links']
    node._attribs = node_info['attributes']

    # print(node.to_json(indent=2))
    save_if_valid(node, OSDFNode)
