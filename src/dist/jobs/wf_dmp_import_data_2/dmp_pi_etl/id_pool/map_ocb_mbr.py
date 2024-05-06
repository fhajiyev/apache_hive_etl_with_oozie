#!/usr/bin/python
#-*- coding: utf-8 -*-


# mart_card_mst
# hdfs://skpds/data_bis/ocb/MART/CTR/mart_card_mst/

import sys
import os


laddr = dict()
maddr = dict()
saddr = dict()

f = open('ocb_code.dat', 'r')
lines = f.readlines()
f.close()

for line in lines:
    s = line.rstrip('\n').split('\t')

    cate  = s[0].strip()
    ak    = s[1].strip()
    av    = s[6].strip()

    if cate == 'HJD_LGRP_CD':
        laddr[ak] = av

    elif cate == 'HJD_MGRP_CD':
        maddr[ak] = av

    elif cate == 'HJD_SGRP_CD':
        saddr[ak] = av

lines = None

def print_ocb_mbr():
    i_mbr_id = 0
    i_ci     = 2
    i_sex    = 10
    i_age    = 11
    i_addr   = 40
    i_mdn    = 63 # (clphn_no)

    for line in sys.stdin:
        line = line.rstrip('\n').split('\t', 1)
        s = line[1][1:-1].split(',')

        ci = s[i_ci].strip()

        if ci == '' or ci == '\N' or u'\u0001' in ci:
            continue

	mbr_id  = s[i_mbr_id].strip()
	sex_raw = s[i_sex].strip() 
	
        # 성별

        if sex_raw == '1':
            sex = 'M'
        elif sex_raw == '2':
            sex = 'F'
        else:
            sex = ''

        # 나이
        age = s[i_age].strip()

        # 시, 도
        addr = s[i_addr].strip()
        lm = ''
        mm = ''
        sm = ''

        lm_dic = laddr.get(addr[0:2], '')
	mm_dic = maddr.get(addr[0:5], '')
	sm_dic = saddr.get(addr,      '')

        if addr != 'Y':
            lm = lm_dic
	    if mm_dic != '' and lm_dic != '':
		mm = '%s %s' % (lm_dic, mm_dic)
	    else:
		mm = ''
	    if sm_dic != '' and mm_dic != '' and lm_dic != '':
		sm = '%s %s %s' % (lm_dic, mm_dic, sm_dic)
	    else:
		sm = ''

	mdn = s[i_mdn].strip()


        print '\t'.join([ ci, mbr_id, '', '', mdn, age, sex, lm.strip(), mm.strip(), sm.strip() ])


print_ocb_mbr()

