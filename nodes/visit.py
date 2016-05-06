#!/usr/bin/env python

import logging
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(name=basename(__file__))

from cutlass import Visit as OSDFNode

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
    node.tags = node_info['tags']
    node.links = node_info['links']
    node._attribs = node_info['attributes']

    # print(node.to_json(indent=2))
    save_if_valid(node, OSDFNode)

