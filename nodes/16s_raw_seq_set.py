#!/usr/bin/env python

import tempfile

from cutlass_utils import save_if_valid, \
    load_string_from_file, \
    load_config_from_file
from cutlass import iHMPSession
from cutlass import SixteenSRawSeqSet as OSDFNode

# 16S dna prep info file name
info_file = '../data_files/16S_raw_seq_set.yaml'

# load username, password from files
username = load_string_from_file('../auth/username.txt')
password = load_string_from_file('../auth/password.txt')

session = iHMPSession(username, password)

print('Required fields: {}'.format(OSDFNode.required_fields()))

# load  subject info sets from info_file
config = load_config_from_file(info_file)
for node_info in config:
    node = OSDFNode()

    node.comment = node_info['comment']
    node.checksums = node_info['checksums']
    node.exp_length = node_info['exp_length']
    node.format = node_info['format']
    node.format_doc = node_info['format_doc']
    node.seq_model = node_info['seq_model']
    node.sequence_type = node_info['sequence_type']
    node.size = node_info['size']
    node.study = node_info['study']
    node.subtype = node_info['subtype']
    node.local_file = node_info['local_file']
    node.tags = node_info['tags']
    node.links = node_info['links']


    # print("Creating a temp file for example/testing purposes.")
    # temp_file = tempfile.NamedTemporaryFile(delete=False).name
    # print("Local file: %s" % temp_file)
    # node.local_file = temp_file

    # print(node.to_json(indent=2))
    save_if_valid(node, OSDFNode)





