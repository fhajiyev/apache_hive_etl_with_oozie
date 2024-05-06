#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import copy

prev_ci      = ''
prev_info    = [''] * 9
info         = list()

def print_result(ci, info):
    print '\t'.join([ci] + info)

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    ci        = s[0]
    info      = s[1:]

    if prev_ci == '':
        prev_ci            = ci
        prev_info          = copy.deepcopy(info)

        continue

	
    if prev_ci != ci:

        print_result(prev_ci, prev_info)
	prev_info = [''] * 9
    

    prev_ci  = ci
    if info[0] != '':
        prev_info[0] = info[0]

    if info[1] != '':
        prev_info[1] = info[1]

    if info[2] != '':
        prev_info[2] = info[2]

    if info[3] != '':
        prev_info[3] = info[3]

    if info[4] != '':
        if prev_info[4] != '':
            if int(prev_info[4]) < int(info[4]):
                prev_info[4] = info[4]
        else:
            prev_info[4] = info[4]
            
    if info[5] != '':
        prev_info[5] = info[5]

    if info[6] != '':
        prev_info[6:] = info[6:]

if prev_ci == ci:
    print_result(prev_ci, info)
