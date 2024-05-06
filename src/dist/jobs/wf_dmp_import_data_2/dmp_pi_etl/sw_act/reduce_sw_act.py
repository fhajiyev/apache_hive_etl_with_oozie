#!/usr/bin/python
#-*- coding: utf-8 -*-


import sys
import copy
prev_mbr_id = ''
prev_tr = list()
curr_tr = list()
prev_ci = ''

def print_result(ci, trs):

    list_all = list()
    for tr in trs:
        t = [''] * 101
        t[0] = ci
        t[1] = tr[0]
        t[3] = tr[2]


        if tr[1] == '00':
            t[5] = '00'
        elif tr[1] == '01':
            t[5] = '01'
            t[6] = tr[3]
        elif tr[1] == '02':
            t[5] = '02'
            t[7] = tr[4]

        if t not in list_all:
            print '\001'.join(t)

        list_all.append(t)

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    mbr_id = s[0]

    if len(s) == 2:
	ci = s[1]
    else:
	curr_tr = s[1:]

    if prev_mbr_id == '':
	prev_mbr_id = mbr_id
	if len(s) == 2:
            prev_ci     = ci
	else:
	    prev_tr = list()
	    prev_tr.append(curr_tr)

    if prev_mbr_id != mbr_id:
	if len(prev_tr) > 0 and prev_ci != '':
	    print_result(prev_ci, prev_tr)

        prev_tr = list()
	prev_ci = ''

    prev_mbr_id = mbr_id
    
    if len(s) == 2:
	prev_ci = ci
    else:
	prev_tr.append(curr_tr)

if prev_mbr_id == mbr_id:
    if len(prev_tr) > 0 and prev_ci != '':
        print_result(prev_ci, prev_tr)

