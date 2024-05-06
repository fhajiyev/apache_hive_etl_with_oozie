#!/usr/bin/python
#-*- coding: utf-8 -*-

# mt3_mem_card
# hdfs://skpds/data_bis/smartwallet/raw/mt3_mem_card/000000_0

import sys
import os
import datetime

try:
   src = os.environ['mapreduce_map_input_file']
except:
    src = ''
    src = 'dmp_pi_id_pool'
    src = 'hdfs://skpds/data_bis/smartwallet/raw/mt3_member'
    pass


def print_sw_os():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')
        mbr_id = s[0].strip()
        dev_model = s[6].strip()

        if mbr_id == '' or mbr_id == '\N' or mbr_id == '-1':
            continue

        if dev_model[0:4].upper() in ['IPHO', 'IPOD', 'IPAD']:
            os_str = 'IOS'
        else:
            os_str = 'Android'

        print '\t'.join([ mbr_id, os_str, 'os' ])

def print_sw_mbr():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        ci        = s[0].strip()
        mbr_id    = s[2].strip()

        if mbr_id == '' or mbr_id == '\N' or mbr_id == '-1':
            continue

        print '\t'.join([ mbr_id, ci ])

if 'dmp_pi_id_pool' in src:
    print_sw_mbr()
elif 'mt3_member' in src:
    print_sw_os()
    


