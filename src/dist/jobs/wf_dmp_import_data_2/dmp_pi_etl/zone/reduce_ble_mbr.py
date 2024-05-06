#!/usr/bin/python
#-*- coding: utf-8 -*-


## mart_anal_sale_info
# hdfs://skpds/data_bis/ocb/MART/BIL/mart_anal_sale_info

prev_mbr_id = ''
prev_ocb_tr = list()
ocb_tr = list()
prev_ci = ''
import sys

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    mbr_id = s[0]

    if len(s) == 2:
	ci = s[1]
    else:
	ocb_tr = s[1:]

    if prev_mbr_id == '':
	prev_mbr_id = mbr_id
	if len(s) == 2:
            prev_ci     = ci
	else:
	    prev_ocb_tr = list()
	    prev_ocb_tr.append(ocb_tr)

    if prev_mbr_id != mbr_id:
	if len(prev_ocb_tr) > 0:
	    for tr in prev_ocb_tr:
		t = [''] * 101
		t[0] = prev_ci
		t[1:len(tr)] = tr
		print '\001'.join(t)
		

        prev_ocb_tr = list()

    prev_mbr_id = mbr_id
    
    if len(s) == 2:
	prev_ci = ci
    else:
	prev_ocb_tr.append(ocb_tr)

if prev_mbr_id == mbr_id:
    if len(prev_ocb_tr) > 0:
        for tr in prev_ocb_tr:
            t = [''] * 101
            t[0] = prev_ci
            t[1:len(tr)] = tr
            print '\001'.join(t)
	

