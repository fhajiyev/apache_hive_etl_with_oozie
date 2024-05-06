#!/usr/bin/python
#-*- coding: utf-8 -*-


import sys
prev_tr = list()
curr_tr = list()
prev_ci = ''

def print_result(ci, trs):

    printed = list()
    for tr in trs:
	t = [''] * 101
	t[0] = ci
	t[1:len(tr)] = tr
        
        if t not in printed:
            print '\001'.join(t)

        printed.append(t)



for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    ci = s[0]

    curr_tr = s[1:]

    if prev_ci == '':
	prev_ci = ci
        prev_tr = list()
        prev_tr.append(curr_tr)

    if prev_ci != ci:
	if len(prev_tr) > 0 and prev_ci != '':
	    print_result(prev_ci, prev_tr)

        prev_tr = list()

    prev_ci = ci
    prev_tr.append(curr_tr)

if prev_ci == ci:
    print_result(prev_ci, prev_tr)

