#!/usr/bin/env python

from cutlass import iHMPSession
from pprint import pprint

username = 'bleopold'
password = 'undrogin15'
session = iHMPSession(username, password)

# try:
info = session.get_osdf().get_info()
pprint(info)
# except Exception as e:
    # import pdb; pdb.set_trace()
