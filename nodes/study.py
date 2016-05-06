#!/usr/bin/env python

import os, logging
filename=os.path.basename(__file__)
logging.basicConfig(level=logging.DEBUG,
        format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(filename)

from cutlass.Study import Study

class node_values:
    name = 'prediabetes'
    description = 'Type 2 diabetes mellitus (T2D) is a significant health problem facing our nation. Close to 20 million individuals in the United States have T2D, and 79 million aged 20 years or older are clinically pre-diabetic, with a 5-year conversion rate of 10% to 23% from prediabetes to T2D. In a collaborative effort to systematically understand diabetes and its etiology, the team is comprised of leading experts in research on both the human host as well as the microbiome, as properties of both are likely relevant in T2D development. For a better elucidation of mechanisms of onset and progression of T2D disease, the group is performing a detailed analysis of the biological processes that occur in the microbiome and human host by longitudinal profiling of patients at risk for T2D. Both microbiome and host profiles are being analyzed by state-of-the-art omics platforms, and these large-scale and diverse data sets will be integrated to determine the dynamic pathways that change during the onset and progression of T2D, especially during viral infections and other stresses. This longitudinal study is expected to reveal changes in the microbiome and host at an unprecedented level of detail, and identify molecules and pathways that play important roles in diabetes onset and progression.'
    center = 'Stanford University & Jackson Laboratory'
    contact = 'Wenyu Zhou <wenyuz@stanford.edu>'
    # subtype = 'overeaters'
    tags = [ 'prediabetes', 'overeaters', ]

# print("Required fields: {}".format(OSDFNode.required_fields()))

def load(query):
    """search for existing node to update, else create new"""
    try:
        studs = Study.search(query)
        for info in studs:
            if info['meta']['name'] == node_values.name 
                return Study.load_study(info)
    except Exception, e:
        studs = Study.create_study()
    finally:
        return studs

def get_study():
    """use get_study to load or create, then update"""
    log.info
    s = load(node_values.name)
    s.update(node_values)
    return s

def (node):
    
