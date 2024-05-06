#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'dmp_pi_id_pool'
    pass


for line in sys.stdin: 
    s = line.rstrip('\n').split('\t')

    if 'dmp_pi_id' in src:

	dmp_uid = s[0]
	mbr_id  = s[1]

	if mbr_id != '':
            print '\t'.join( [ mbr_id, dmp_uid ]) 
    else:
        print line.rstrip('\n')

