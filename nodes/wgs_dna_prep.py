#!/usr/bin/env python

import logging

from cutlass_utils import save_if_valid, \
    load_string_from_file, \
    load_config_from_file
from cutlass import iHMPSession
from cutlass import WgsDnaPrep as OSDFNode

# 16S dna prep info file name
info_file = '../data_files/wgs_dna_prep.yaml'

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')

session = iHMPSession(username, password)

print("Required fields: {}".format(OSDFNode.required_fields()))

# load  subject info sets from info_file
config = load_config_from_file(info_file)
for node_info in config:
    node = OSDFNode()

    node.comment = node_info['comment']
    node.frag_size = node_info['frag_size']
    node.lib_layout = node_info['lib_layout']
    node.lib_selection = node_info['lib_selection']
    node.mims = node_info['mims']
    node.storage_duration = node_info['storage_duration']
    node.sequencing_center = node_info['sequencing_center']
    node.sequencing_contact = node_info['sequencing_contact']
    node.prep_id = node_info['prep_id']
    node.ncbi_taxon_id = node_info['ncbi_taxon_id']
    node.tags = node_info['tags']
    node.links = node_info['links']

    # print(node.to_json(indent=2))
    save_if_valid(node, OSDFNode)

