#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os

# mart_alcmpn_mst
# 제휴사 hdfs://skpds/data_bis/ocb/MART/CTR/mart_alcmpn_mst \

# mart_mcnt_mst
# 가맹점 hdfs://skpds/data_bis/ocb/MART/CTR/mart_mcnt_mst
try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'mart_alcmpn_mst'
#    src = 'mart_mcnt_mst'
    pass

# 제휴사 대중소
dict_l = dict()
dict_m = dict()
dict_s = dict()
# test
# cat test_mart_alcmpn_mst | map ..
# cat test_mart_mcnt_mst | map ..


if 'mart_alcmpn_mst' in src:

    f = open('ocb_code.dat', 'r')
    lines = f.readlines()
    for line in lines:
        s = line.rstrip('\n').split('\t')

        cate = s[0].strip()

        if cate == 'OCB_BIZTP_SGRP_CD':
            dict_s[s[1]] = s[6].strip()

        elif cate == 'OCB_BIZTP_MGRP_CD':
            dict_m[s[1]] = s[6].strip()

        elif cate == 'OCB_BIZTP_LGRP_CD':
            dict_l[s[1]] = s[6].strip()

    f.close()
for line in sys.stdin:
    line = line.rstrip('\n').split('\t', 1)
    s = line[1][1:-1].rstrip('\n').split(', ')
    
    if 'mart_mcnt_mst' in src:

        i = 0 
	for i in range(1, len(s) - 18 + 1):
	    s[1] += s[1 + i]

        # 제휴사 코드 - M - 가맹점 코드 - 가맹점 이름
        print '%s\tM\t%s\t%s' % (s[2 + i].strip(), s[0].strip(), s[1].strip())

    elif 'mart_alcmpn_mst' in src:

        j_code = s[0].strip()

	# 업종 코드
	ocb_biztp_sgrp_cd = s[5].strip()

        # 하이브 쿼리는 1부터 시작 길이 substr

	lc = ocb_biztp_sgrp_cd[0:2]
	mc = ocb_biztp_sgrp_cd[0:4]
	sc = ocb_biztp_sgrp_cd


	ll = dict_l.get(lc, '')
	mm = dict_m.get(mc, '')
        ss = dict_s.get(sc, '')

	if ll == '미입력':
	    ll = ''
	if mm == '미입력':
	    mm = ''
	if ss == '미입력':
	    ss = ''

	if lc == 'Y':
	    lc = ''
	if mc == 'Y':
	    mc = ''
	if sc == 'Y':
	    sc = ''


        # 제휴사 코드 - A - 제휴사 이름 - 대 - 중 - 소 - 업종 코드
        print '%s\tA\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (j_code, s[1].strip(), ll, mm, ss, lc, mc, sc)

