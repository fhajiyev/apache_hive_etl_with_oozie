#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys

prev_mbr_id = ''
prev_os_str = ''
prev_dmp_uid = ''

f = open('sw_agree.dat', 'r')
lines = f.readlines()
f.close()

set_agr = set()

for line in lines:
    set_agr.add(line.strip())

lines = None


def print_result(ci, agr, os_str):
    if ci == '':
        return
    print '\t'.join([ci, agr, os_str])

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    mbr_id         = s[0]
    if len(s) == 3:
        os_str = s[1]
    else:
	dmp_uid    = s[1]
	
    if prev_mbr_id == '':
        prev_mbr_id = mbr_id
	if len(s) == 2:
	    prev_dmp_uid = dmp_uid
        elif len(s) == 3:
            prev_os_str = os_str
	
    if prev_mbr_id != mbr_id:
        if prev_mbr_id in set_agr:
            agr = '1'
        else:
            agr = '0'

        print_result(prev_dmp_uid, agr, prev_os_str)

        prev_dmp_uid = ''
        prev_os_str = ''
    

    prev_mbr_id  = mbr_id
    
    if len(s) == 3:
        prev_os_str = os_str
    else:
        prev_dmp_uid = dmp_uid
	
if prev_mbr_id == mbr_id:
    if prev_mbr_id in set_agr:
        agr = '1'
    else:
        agr = '0'

    print_result(prev_dmp_uid, agr, prev_os_str)
