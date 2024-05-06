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
        try:

            if int(dt) < 20160601:
	        continue
        except:
            pass

        if int(pay_trns_amt) - int(pay_seller_dscnt_amt_basic) <= 0:
            continue

	if member_no == '-1' or member_no == '' or member_no == '\N':
	    continue

	print line,

print_order()





