#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os

try:
    src = os.environ['mapreduce_map_input_file']
except:
#    src = 'hdfs://skpds/product_bis/BIS_SERVICES/ISTORE/SS3/MA/SUM/SS3_D_SLTN_CUST_ASST_SRC_STA/dt=20170524'
    src = 's3_d_sltn_cust_asst_src_sta'
    src = 'hdfs://skpds/data_db/istore/raw/mkt_use_plc/2017/05/24'
#    src = 'SW'
#    src = 'ELEV'
    src = 'dmp_pi_id_pool'
    pass


def print_mbr():
#dt = 20170523		/ 날짜 파티션
#9   mbr_id		/ 회원 ID 시럽이냐 OCB냐 다름
#11  cust_ind_cd	/ = I 여야 함
#14  sltn_full_actn_cd  / = 0107EN01 미리줌 적립
#13  mkt_id		/ 마케팅 ID
#1   std_tm 중		/ 발생 시각, 가장 큰 것(= 최근 것)
#8   poc_ind_cd   = OCB = O, 시럽이면 S
#7   sid
#18  svc_sltn_id


    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        mbr_id            = s[9].strip()
        cust_ind_cd       = s[11].strip()
        sltn_full_actn_cd = s[14].strip()
        mkt_id            = s[13].strip()
        std_tm            = s[1].strip()
        poc_ind_cd        = s[8].strip()
        sid               = s[7].strip()   # 오씨비 가맹점 코드 
        svc_sltn_id       = s[18].strip()   

        if mbr_id == '' or mbr_id == '\N':
            continue

        if cust_ind_cd != 'I':
            continue

        if mkt_id == '' or mkt_id == '\N':
            continue

        if sltn_full_actn_cd == '0107EN01':
            action = '01'
        elif sltn_full_actn_cd == '0107PU01':
            action = '02'
        elif sltn_full_actn_cd == '0107PU02':
            action = '03'
        elif sltn_full_actn_cd == '0104PU01':
            action = '04'
        else:
            continue


        if poc_ind_cd == 'O':
            mbr_id = 'OCB_' + mbr_id
	    channel = '01'

        elif poc_ind_cd =='S':
            mbr_id = 'SYRUP_' + mbr_id
	    channel = '02'
        else:
            continue

        print '\t'.join([mkt_id, '', std_tm, mbr_id, action, channel, sid, svc_sltn_id])

# 1 use_plc_type = 02 --> taid  / 01 --> mid
# 0   marketing_id
# 2   use_plc_id
def print_mkt():

    dic_taid = dict()
    f = open('mid_taid.dat', 'r')
    lines = f.readlines()
    f.close()

    for line in lines:
        s = line.rstrip('\n').split('\t')

        mid  = s[0].strip()
        taid = s[1].strip()

        dic_taid[mid] = taid

    lines = None

    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')

	mkt_id    = s[0].strip()
	plc_type  = s[1].strip()
	plc_id    = s[2].strip()

	if mkt_id == '' or mkt_id == '\N':
	    continue

	if plc_id == '' or plc_id == '\N':
	    continue

	if plc_type == '02':   #plc_id 컬럼이 taid이므로 taid를 바로 넣는다.
	    taid   = plc_id
	    mid = ''
	elif plc_type == '01': #plc_id 컬럼이 mid이므로 mid로 taid를 찾는다
	    taid   =  dic_taid.get(plc_id, '') 
	    mid = plc_id
            if taid == '':
                continue

        else:
	    continue

        print '\t'.join([mkt_id, taid, mid])

if 'SS3_D_SLTN_CUST_ASST_SRC_STA' in src:
    print_mbr()
elif 'mkt_use_plc' in src:
    print_mkt()


