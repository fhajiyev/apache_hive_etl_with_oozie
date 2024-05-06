#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import datetime

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = ''
#    src = 'evs_mb_mem' # 11번가
#    src = 'mt3_member' # 시럽
    pass

def print_sw():
    i_member_id = 0
    i_ci        = 32
    i_sex       = 22 
    i_birth_day = 21
    i_mdn       = 2

    today_year = datetime.date.today().year

    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')

	ci = s[i_ci]

	if ci == '' or ci == '\N':
	    continue

	sex       = s[i_sex]
	birth_day = s[i_birth_day]
	member_id = s[i_member_id]
	mdn       = s[i_mdn]


	if len(birth_day) == 8:
            age = str(int(today_year) - int(birth_day[:4]) + 1)
	else:
	    age = ''


	print '\t'.join([ ci, '', member_id, '', mdn, age, sex, '', '', ''])

def print_elev():
    i_ci          = 145
    i_mem_clf     = 4 # 회원코드 정상 = 01
    i_mem_typ_cd  = 6 # 회원유형코드 일반 = 01
    i_mem_stat_cd = 7 # 회원항태 코드 정상 = 01
    i_mem_no      = 0

    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')

        ci      = s[i_ci].strip();
        mem_no  = s[i_mem_no].strip();

        if ci == '\N' or ci == '':
            continue

        if mem_no == '\N' or mem_no == '':
            continue

        if s[i_mem_clf] != '01':
            continue

        if s[i_mem_typ_cd] != '01':
            continue

        if s[i_mem_stat_cd] != '01':
            continue

        print '\t'.join([ ci, '', '', mem_no, '', '', '', '', '', '' ])   

if 'evs_mb_mem' in src:
    print_elev()
    
elif 'mt3_member' in src:
    print_sw()
else:
    for line in sys.stdin:
        print line.rstrip('\n')
    


