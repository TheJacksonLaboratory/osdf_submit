#!/usr/bin/env python

"""Converts CSV file to YAML
"""

import yaml
import csv


#TODO: argparse?
CSV_infile = open('hmp2_samplesheets.csv', "r")
YAML_outfile = open("hmp2_samplesheets.yaml", "w")
items = []

try:
    reader = csv.DictReader(CSV_infile)
    for row in reader:
        print('Row: '+str(row))
        items.append( row )
        # print('Items: '+items)
    YAML_outfile.write( yaml.dump_all(items, default_flow_style=False) )

finally:
    CSV_infile.close()
    YAML_outfile.close()


