#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import copy

# 회원번호 - 배송지일련번호

# test
# cat 11st_tr_mapped.dat | reduce_demo_11st_tr_last_order.py 
# -D mapred.text.key.partitioner.options=-k1,2 


prev_seq    = ''
prev_ci     = ''
prev_agree  = ''
prev_os_str = ''
prev_si     = ''
prev_gun    = ''
prev_dong   = ''
for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    seq           = s[0]

    if len(s) == 5:
        ci            = s[1]
        agree         = s[2]
        os_str        = s[3]

    else:
        si            = s[1]
        gun           = s[2]
        dong          = s[3]

    if prev_seq == '':
        prev_seq    = seq
        if len(s) == 5:

	    prev_ci     = ci
            prev_agree  = agree
            prev_os_str = os_str 
        
	else:
            prev_si     = si
            prev_gun    = gun
            prev_dong   = dong


    if prev_seq != seq:
	if prev_ci != '':
            print '\t'.join([ prev_ci, prev_si, prev_gun, prev_dong, prev_agree, prev_os_str ])
        prev_ci     = ''
        prev_agree  = ''
        prev_os_str = ''
        prev_si     = ''
        prev_gun    = ''
        prev_dong   = ''

    prev_seq    = seq


    if len(s) == 5:
	prev_ci     = ci
        prev_agree  = agree
        prev_os_str = os_str
    else:
        prev_si     = si
        prev_gun    = gun
        prev_dong   = dong
	

if prev_seq == seq:
    if prev_ci != '':
        print '\t'.join([ prev_ci, prev_si, prev_gun, prev_dong, prev_agree, prev_os_str ])
    

