import cutlass
from cutlass import iHMPSession

from auth import username,password
dcc_user=username
dcc_pass=password

# cutlass/Annotation.py:            505:    def search(query = "\"annotation\"[node_type]"):
# cutlass/Base.py:                  229:    def search(self, query):
# cutlass/HostAssayPrep.py:         764:    def search(query = "\"host_assay_prep\"[node_type]"):
# cutlass/MicrobiomeAssayPrep.py:   764:    def search(query = "\"microb_assay_prep\"[node_type]"):
# cutlass/Project.py:               292:    def search(query = "\"project\"[node_type]"):
# cutlass/Proteome.py:              564:    def search_engine(self):
# cutlass/Proteome.py:              572:    def search_engine(self, search_engine):
# cutlass/Proteome.py:              1057:   def search(query = "\"proteome\"[node_type]"):
# cutlass/Sample.py:                366:    def search(query = "\"sample\"[node_type]"):
# cutlass/SixteenSDnaPrep.py:       478:    def search(query = "\"16s_dna_prep\"[node_type]"):
# cutlass/SixteenSRawSeqSet.py:     526:    def search(self, query):
# cutlass/SixteenSRawSeqSet.py:     572:    def search(query = "\"16s_raw_seq_set\"[node_type]"):
# cutlass/SixteenSTrimmedSeqSet.py: 423:    def search(query = "\"16s_trimmed_seq_set\"[node_type]"):
# cutlass/Study.py:                 297:    def search(query = "\"study\"[node_type]"):
# cutlass/Subject.py:               293:    def search(query = "\"subject\"[node_type]"):
# cutlass/Visit.py:                 322:    def search(query = "\"visit\"[node_type]"):
# cutlass/WgsAssembledSeqSet.py:    459:    def search(query = "\"wgs_assembled_seq_set\"[node_type]"):
# cutlass/WgsDnaPrep.py:            475:    def search(query = "\"wgs_dna_prep\"[node_type]"):
# cutlass/WgsRawSeqSet.py:          507:    def search(query = "\"wgs_raw_seq_set\"[node_type]"):

import cutlass


session = iHMPSession.get_session(settings.dcc_user,settings.dcc_pass)
#module_logger.info("Got iHMP session.")
sample_data = session.get_osdf().oql_query(Sample.namespace, query)

class SearchOSDFNodes(object):
    """Search all of OSDF for specific terms, in specified Nodes"""

    all_nodes = ['project','study','subject','visit','sample','sixteen','wgs']

    def __init__(self, query, nodetypes=[]):
        self.query = query
        if nodetypes == []:
            self.nodes = all_nodes
        else:
            self.nodes = nodes

    def project(self):
        """search for query in: project"""
        pass

    def study(self,query):
        """search for query in: study"""
        cutlass.Study.search(query)
        pass

    def subject(self):
        """search for query in: subject"""
        pass

    def visit(self):
        """search for query in: visit"""
        pass

    def sample(self):
        """search for query in: sample"""
        pass

    def sixteen(self):
        """search for query in: 16S samples"""
        pass

    def wgs(self):
        """search for query in: wgs samples"""
        pass

