#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys

def print_map():

    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')
        col_sid = set()
        col_mname = set()

        org_data = s[:17]
        col_sid.add(s[17].replace('\N', '').strip())
        col_sid.add(s[18].replace('\N', '').strip())
        col_sid.add(s[19].replace('\N', '').strip())
        col_sid.add(s[20].replace('\N', '').strip())
       
        col_mname.add(s[21].replace('\N', '').strip())
        col_mname.add(s[22].replace('\N', '').strip())

        for sid in col_sid:
            for mname in col_mname:
                print '\t'.join(org_data + [sid, mname])
 

        



print_map()
	


