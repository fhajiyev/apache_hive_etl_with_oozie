#!/usr/bin/python
#-*- coding: utf-8 -*-


import sys
prev_mbr_id = ''
prev_tr = list()
curr_tr = list()
prev_ci = ''

def print_result(ci, trs):
    for tr in trs:
        t = [''] * 101
        t[0] = ci
        t[1:len(tr)] = tr

        print '\001'.join(t)


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

