#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import json
import datetime

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'PROD_BLE_BLEKEY_MAPPING'
    src = 'hdfs://skpds/data/newtech/raw/server/2017/05/23/10/rddd.raw'
    src = 'hdfs://skpds/data/newtech/raw/server/2017/12/13/17/raw-1.log'

    pass

def print_mdn ():

    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')
	ble = s[0].strip()
	mdn = s[1].strip()

	print '\t'.join([ble, mdn])

def print_zone_data():

    f = open('m2zone.dat', 'r')
    lines = f.readlines()
    f.close()

    dic_zone = dict()
    dic_zone_name = dict()
    for line in lines:
	s = line.rstrip('\n').split('\001')
	if s[10] != '':
	    dic_zone[s[10]] = s[0]
	# tech id -> zone id
	dic_zone_name[s[0]] = s[2]

    lines = None

    dic_mid = dict()
    dic_mid2zone = dict()

    f = open('m2_merch_zone.dat', 'r')
    lines = f.readlines()
    f.close()
    for line in lines:
	s = line.rstrip('\n').split('\001')
	dic_mid[s[1]] = max(dic_mid.get(s[1], ''), s[0])
        dic_mid2zone[s[0]] = s[1]
#	dic_mid[s[1]] = s[0]
	# zone id -> mid
    lines = None

    dic_addr = dict()
    f = open('mid_addr.dat')
    lines = f.readlines()
    f.close()
    for line in lines:
        s = line.rstrip('\n').split('\t')
        dic_addr[s[0]] = [ s[1], s[2] ]
	# mid - addr - addrcode

    lines = None

    dict_lc = dict()
    dict_mc = dict()
    dict_sc = dict()

    f = open('ocb_code.dat', 'r')
    lines = f.readlines()
    f.close()
    for line in lines:
        s = line.rstrip('\n').split('\t')

        cate = s[0].strip()
  
        if cate == 'HJD_LGRP_CD':
            dict_lc[s[1]] = s[6].strip()

        elif cate == 'HJD_MGRP_CD':
            dict_mc[s[1]] = s[6].strip()

        elif cate == 'HJD_SGRP_CD':
            dict_sc[s[1]] = s[6].strip()
    
    lines = None    

    weeks = ( 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' )

    dt = src.split('/')[-5:-1]
    dt_date = ''.join(dt[:3])
    dt_hour = dt[3]

    try:
	c_week = weeks[datetime.date(int(dt_date[0:4]), int(dt_date[4:6]), int(dt_date[6:8])).weekday()]
    except:
	c_week = ''

    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        try:

            ble    = s[3].strip()
            log_id = s[19].strip()
            body   = s[27].strip()
            mid    = s[14].strip()
            tech_id = ''
            zone_id = ''
        except:
            continue
        

        if ble == '' or ble == '\N':
	    continue

        if log_id == 'PXG0101': 
	    type_ch = '01'
            try:
                tech_id = str(json.loads(body)['tech_id'])
            except:
                tech_id = ''

            zone_id = dic_zone.get(tech_id, '')
            mid = dic_mid.get(zone_id, '')
            zone_name = dic_zone_name.get(zone_id, '')

	elif log_id in [ 'PXB0101', 'PXW0101' ]:
	    type_ch = '02'
#            zone_id = dic_mid2zone.get(mid, '')
	else:
	    continue

        mid_info = dic_addr.get(mid, ['',''])
	mid_name = mid_info[0]
	mid_addr = mid_info[1]

	lm_dic = dict_lc.get(mid_addr[0:2], '')
	mm_dic = dict_mc.get(mid_addr[0:5], '')

        lm = lm_dic
	if mm_dic != '' and lm_dic != '':
	    mm = '%s %s' % (lm_dic, mm_dic)
	else:
	    mm = ''

        km = dict_sc.get(mid_addr + '00', '')
        if km == '':
            km = dict_sc.get(mid_addr, '')

        if km == '':
            km = dict_sc.get(mid_addr[:-1] + '0', '')

        if km == '':
            km = dict_sc.get(mid_addr[:-1] + '000', '')

        if km == '':
            km = dict_sc.get(mid_addr[:-3] + '000', '')

	if km != '' and mm_dic != '' and lm_dic != '':
	    sm = '%s %s %s' % (lm_dic, mm_dic, km)
	else:
	    sm = ''

        if type_ch == '01' and zone_id != '':
            print '\t'.join([ble, dt_date, dt_hour, c_week, type_ch, zone_id, zone_name, lm.strip(), mm.strip(), sm.strip()])
        if type_ch == '02' and mid != '':
            print '\t'.join([ble, dt_date, dt_hour, c_week, type_ch, mid, mid_name, lm.strip(), mm.strip(), sm.strip()])


	
if 'PROD_BLE_BLEKEY_MAPPING' in src:
    print_mdn()
elif 'newtech' in src:
    print_zone_data()




