#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import datetime
# 회원 정보
# tb_evs_ods_m_mb_mem
# hdfs://skpds/data_db/11st/raw/evs_mb_mem/2017/05/18	



# 구매 정보
# tb_evs_ods_f_tr_ord_prd
# hdfs://skpds/data_bis/11st/raw/evs_tr_ord_prd/2017/05/17  
# 회원번호 - 구매 번호 - 배송지 일련번호 - CI



# 구매 테이블에서 최근 주문 기록만 남긴다.
# test 
# cat 11st_tr.dat | map_demo_11st_tr_last_order.py | sort > 11st_tr_mapped.dat

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'evs_mb_mem'
    src = 'hdfs://skpds/data_db/11st/raw/evs_mo_app_push_info'
    src = 'hdfs://skpds/product_bis/svc_cdpf/USPF/EVS/LFSTG/USPF_LFSTG_EVS_BUY'
#    src = 'dmp_pi_id_pool'
	

def proc_11st_tr():

    i_mem_no        = 108
    i_ord_no        = 0
    i_delvplace_seq = 3
    two_years_ago   = (datetime.datetime.now() - datetime.timedelta(days = 365 * 2)).strftime('%Y%m%d')

    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')

        try:
            #ord_no 가 크면 최근이다.
            ord_no        = s[i_ord_no].strip()
            delvplace_seq = s[i_delvplace_seq].strip()

	    if two_years_ago > ord_no[0:8]:
                continue

            mem_no = s[i_mem_no].strip()
        except:
            continue

	if mem_no == '-1' or mem_no == '' or mem_no == '0':
	    continue


        print '\t'.join([ mem_no, ord_no, delvplace_seq, 'order' ])

def proc_11st_agree():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')
        
        mem_no = s[6].strip()
        mem_ag = s[17].strip()

        if mem_no == '-1' or mem_no == '' or mem_no == '0':
            continue

        print '\t'.join([ mem_no, mem_ag, 'agree' ])

def proc_11st_os():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')

        mem_no = s[0].strip()
        os_str = s[1].strip()
        update_dt = s[2].strip()

        if mem_no == '-1' or mem_no == '' or mem_no == '0':
            continue

        if 'ANDROID' in os_str.upper():
            os_put = 'Android'
        else:
            os_put = 'IOS'

        print '\t'.join([ mem_no, os_put, update_dt, '', 'os' ])

def proc_11st_mem():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        ci        = s[0]
        mbr_id    = s[3]

        if mbr_id == '' or mbr_id == '' or mbr_id == '0':
            continue

        print '\t'.join([ mbr_id, ci ])

if 'dmp_pi_id_pool' in src:
    proc_11st_mem()
elif 'evs_mo_app_push_info' in src:
    proc_11st_agree()
elif 'elev_os' in src:
    proc_11st_os()
else:
    proc_11st_tr()

