#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'hdfs://skpds/data_bis/11st/raw/evs_sd_ord_detl_rslt/2017/05/23/33333.gz'
#    src = 'hdfs://skpds/data_db/11st/raw/evs_tr_bckt/2017/05/23'
#    src = 'evs_pd_prd'
    pass

target_day = sys.argv[1]
## 주문의 경우 무조건 D-2의 날짜가 들어온다.
#target_day   = ''.join(src.split('/')[-4:-1])

def print_order():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')

	prd_no       = s[4].strip()
	member_no    = s[7].strip()
	cate         = s[5].strip()
	dt           = s[151].strip()
	dttm         = s[13].strip()
        pay_trns_amt = s[63].strip()
        pay_seller_dscnt_amt_basic = s[125].strip()

        if pay_seller_dscnt_amt_basic == '':
            pay_seller_dscnt_amt_basic = 0

        if pay_trns_amt == '':
            pay_trns_amt = 0

	if dt != target_day:
	    continue

        if int(pay_trns_amt) - int(pay_seller_dscnt_amt_basic) <= 0:
            continue


	if len(dttm) != 19:
	    dt_hour = ''
	else:
	    dt_hour = dttm[11:13]

	if member_no == '-1' or member_no == '' or member_no == '\N':
	    continue

	print '\t'.join([ prd_no, 'buy', member_no, cate, dt, dt_hour ])

def print_basket():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')
        
	prd_no       = s[1].strip()
	cate         = s[22].strip()
	member_no    = s[12].strip()
	dt           = s[11].strip()
	
	if len(dt) != 21:
	    dt_date = ''
	    dt_hour = ''
	else:
	    dt_date = dt.replace('-','')[0:8]
	    dt_hour = dt[11:13]

	if member_no == '-1' or member_no == '' or member_no == '\N':
	    continue

        if dt_date != target_day:
            continue

	print '\t'.join( [prd_no, 'keep', member_no, cate, dt_date, dt_hour ])

def print_product():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')

	prd_no       = s[0].strip()
        prd_nm       = s[5].strip().replace('\t', ' ')

	if prd_no == '' or prd_no == '\N':
	    continue
	
	print '\t'.join([ prd_no, prd_nm ])


if 'dmp_11st_pre' in src:
    print_order()
elif 'evs_tr_bckt' in src:
    print_basket()
elif 'evs_pd_prd' in src:
    print_product()





