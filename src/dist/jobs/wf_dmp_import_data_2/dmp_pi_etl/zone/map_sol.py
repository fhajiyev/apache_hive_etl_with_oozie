#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os

try:
    src = os.environ['mapreduce_map_input_file']
except:
#    src = 'temp_ocb'
#    src = 'temp_sw'
    src = 'sol_mkt'
#    src = 'id_pool'
    pass

def print_mbr():

    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        ocb_id = s[1].strip()
        sw_id  = s[2].strip()
        ci     = s[0].strip()

        if ocb_id != '':
            print '\t'.join([ 'OCB_' + ocb_id, ci])

        if sw_id != '':
            print '\t'.join([ 'SYRUP_' + sw_id, ci])


if 'id_pool' in src:
    print_mbr()
else:
    for line in sys.stdin:
	print line,
	


