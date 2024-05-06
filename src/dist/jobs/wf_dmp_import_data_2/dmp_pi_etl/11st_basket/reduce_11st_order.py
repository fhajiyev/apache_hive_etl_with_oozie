#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import copy

prev_prd_id   = ''
prev_data     = list()
prev_pr       = list()

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    prd_id         = s[0]

    if prev_prd_id == '':
        prev_prd_id = prd_id
	if len(s) != 7:
	    prev_data.append(s[1:])
	else:
            prev_pr = copy.deepcopy(s[1:])

	
    if prev_prd_id != prd_id:
	if len(prev_data) > 0 and len(prev_pr) > 0:
            for data in prev_data:
                if data[1] != '':
                    print '\t'.join([prev_prd_id] + data + prev_pr)
        prev_data = list()
        prev_pr   = list()
    

    prev_prd_id  = prd_id

    if len(s) != 7:
        prev_data.append(s[1:])
    else:
        prev_pr = copy.deepcopy(s[1:])

if prev_prd_id == prd_id:
    if len(prev_data) > 0 and len(prev_pr) > 0:
        for data in prev_data:
            if data[1] != '':
                print '\t'.join([prev_prd_id] + data + prev_pr)
