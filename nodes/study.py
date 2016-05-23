#!/usr/bin/env python
""" load Study into OSDF using info from file
    Note: this only uses the 'part_of' linkage for the project!
"""

import os

from cutlass.Study import Study

from cutlass_utils import log_it
import settings

# filename=os.path.basename(__file__)
# log = log_it(filename)

# Log It!
import os, logging # needed for LogIt function!
def log_it(filename=os.path.basename(__file__)):
    """log_it setup"""
    logging.basicConfig(level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(funcName)s: %(message)s")
    return logging.getLogger(filename)
log = log_it()

node_type='study'
parent_type = 'project'
node_tracking_file = settings.node_id_tracking.path


class node_values:
    name = 'prediabetes'
    description = 'Type 2 diabetes mellitus (T2D) is a significant health problem facing our nation. Close to 20 million individuals in the United States have T2D, and 79 million aged 20 years or older are clinically pre-diabetic, with a 5-year conversion rate of 10% to 23% from prediabetes to T2D. In a collaborative effort to systematically understand diabetes and its etiology, the team is comprised of leading experts in research on both the human host as well as the microbiome, as properties of both are likely relevant in ' #T2D development. For a better elucidation of mechanisms of onset and progression of T2D disease, the group is performing a detailed analysis of the biological processes that occur in the microbiome and human host by longitudinal profiling of patients at risk for T2D. Both microbiome and host profiles are being analyzed by state-of-the-art omics platforms, and these large-scale and diverse data sets will be integrated to determine the dynamic pathways that change during the onset and progression of T2D, especially during viral infections and other stresses. This longitudinal study is expected to reveal changes in the microbiome and host at an unprecedented level of detail, and identify molecules and pathways that play important roles in diabetes onset and progression.'
    center = 'Stanford University / Jackson Laboratory'
    contact = 'Wenyu Zhou <wenyuz@stanford.edu>'
    subtype = 'prediabetes'
    tags = [ 'prediabetes', 'overeaters', ]

# print("Required fields: {}".format(OSDFNode.required_fields()))

def load(query):
    """search for existing node to update, else create new"""
    log.debug("in load: "+node_type)
    try:
        n = Study.search(query)
        for info in n:
            if info['meta']['name'] == query:
                log.info("Study record found!")
                return Study.load_study(info)
    except Exception, e:
        return Study()

def validate_record(node,values,parent_id):
    """update all related values"""
    node.name = values.name
    node.description = values.description
    node.center = values.center
    node.contact = values.contact
    node.subtype = values.subtype
    node.tags = values.tags
    # node.links({'part_of':[parent_id]})
    node.links = {'part_of':[parent_id]}
    log.debug(str(node.links))
    if not node.is_valid():
        invalidities = node.validate()
        err_str = "Invalid!: {}".format(",".join(invalidities))
        log.error(err_str)
        raise Exception(err_str)
    elif node.save():
        return node
    else:
        return False

def get_study(project_node_id):
    """use get_study to load or create, then update"""
    log.info('Starting submission of study.')
    log.debug("in get_node: "+node_type)
    s = load(node_values.name)
    node = validate_record(s,node_values,project_node_id)
    node.add_tag('TEST') # for debug
    return node

if __name__ == '__main__':
    pass
