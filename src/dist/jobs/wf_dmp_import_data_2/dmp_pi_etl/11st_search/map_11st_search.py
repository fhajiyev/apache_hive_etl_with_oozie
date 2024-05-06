#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import datetime

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'hdfs://skpds/user/hive/warehouse/svc_cdpf.db/intgt_srch_keyword_mbr'
#    src = 'hdfs://skpds/data_bis/11st/dm/log_mobile_keywords/2017/05/23/000.gz'
#    src = 'hdfs://skpds/data_bis/11st/dm/log_web_keywords/2017/05/23/000.gz'
#    src = 'dmp_pi_id_pool'
    pass

weeks     = ( 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' )


now_day = sys.argv[1]

for line in sys.stdin:
    if 'dmp_pi_id_pool' in src:
        s = line.rstrip('\n').split('\t')
        member_no = s[3].strip()
        ci        = s[0].strip()
        
        if member_no == '-1' or member_no == '' or member_no == '\N':
	    continue

	if ci == '' or ci == '\N':
	    continue

        print '\t'.join([ member_no, ci ])

    else:

        s = line.rstrip('\n').split('\001')
	try:
            member_no = s[0].strip()
            kword     = s[1].strip()
            dt_date   = s[2].strip()

            if dt_date != now_day:
#                print dt_date, now_day
                continue
           
            dt_year  = dt_date[0:4]
            dt_month = dt_date[4:6]
            dt_day   = dt_date[6:8]
            target_day = datetime.date(int(dt_year), int(dt_month), int(dt_day))
            c_week = weeks[target_day.weekday()]

	except:
	    continue

        if member_no == '-1' or member_no == '' or member_no == '\N':
            continue

	if kword == '' or kword == '\N':
	    continue
  
        print '\t'.join([ member_no, dt_date, '', c_week, 'search', kword ])


