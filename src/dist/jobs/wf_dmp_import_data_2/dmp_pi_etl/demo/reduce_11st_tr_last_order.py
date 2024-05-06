#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import random
import copy

# 회원번호 - 배송지일련번호

# test
# cat 11st_tr_mapped.dat | reduce_demo_11st_tr_last_order.py 
# -D mapred.text.key.partitioner.options=-k1,2 


prev_mem_no   = ''
prev_ord_no   = ''
prev_ci       = ''
prev_agree    = ''
prev_delvplace_seq = ''
prev_os_str   = ''
max_date      = ''

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')
    mem_no        = s[0]

    if len(s) == 2:
	ci      = s[1]
    elif len(s) == 3:
        agree = s[1]
    elif len(s) == 5:
        os_str = s[1]
        update_dt = s[2]
    else:
        ord_no        = s[1]
        delvplace_seq = s[2]

    if prev_mem_no == '':
        prev_mem_no        = mem_no
	if len(s) == 2:
	    prev_ci = ci
        elif len(s) == 3:
            prev_agree = agree
        elif len(s) == 5:
            prev_os_str = os_str
            max_date    = update_dt
	else:

            if delvplace_seq != '':
	        if prev_ord_no < ord_no:
                    prev_delvplace_seq = delvplace_seq 
		    prev_ord_no = ord_no

    if prev_mem_no != mem_no:
	if prev_delvplace_seq != '' and prev_ci != '':
            print '\t'.join([ prev_delvplace_seq, prev_ci, prev_agree, prev_os_str, 'tb1' ])
	prev_delvplace_seq = ''
	prev_ord_no        = ''
	prev_ci            = ''
        prev_agree         = ''
        max_date           = ''

	
    prev_mem_no        = mem_no

    if len(s) == 2:
	prev_ci = ci
    elif len(s) == 3:
        prev_agree = agree
    elif len(s) == 5:
        if update_dt > max_date:
            max_date = update_dt
            prev_os_str = os_str
    else:
        if delvplace_seq != '':
            if prev_ord_no < ord_no:
                prev_delvplace_seq = delvplace_seq 
	        prev_ord_no = ord_no

if prev_mem_no == mem_no:
    if prev_delvplace_seq != '' and prev_ci != '':
        print '\t'.join([ prev_delvplace_seq, prev_ci, prev_agree, prev_os_str, 'tb1' ])
