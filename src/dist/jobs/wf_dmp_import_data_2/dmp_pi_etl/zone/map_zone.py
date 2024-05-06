#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'temp_ble_tr'
    src = 'id_pool'
    pass

def print_mbr():

    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        mdn    = s[4].strip()
        ci     = s[0].strip()

        if mdn != '':
            print '\t'.join([ mdn, ci])

def print_ble_tr():
    for line in sys.stdin:
	line = '\t'.join(line.rstrip('\n').split('\001')[:10])
	print line

if 'temp_ble_tr' in src:
    print_ble_tr()
else:
    print_mbr()


