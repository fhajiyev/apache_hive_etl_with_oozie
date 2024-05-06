#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import datetime

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = ''
#    src = 'dmp_pi_id_pool'
    pass


weeks = ( 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' )


for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    if 'dmp_pi_id_pool' in src:
	ci        = s[0]
        member_no = s[3]

        if s[3] == '':
            continue

	print '\t'.join([member_no, ci])
    else:
	
	member_no = s[2]
	action    = s[1]
	dt_date   = s[3]
	dt_hour   = s[4]
        prd_id    = s[0]
#        try:
        prd_nm    = s[6]
#        except:
#         print s
#             continue
        
        cate      = s[7]
        if cate == '0' or cate == '':
            cate      = s[5]
            cate1     = ''
            cate2     = ''
            cate3     = ''
            cate4     = ''
        else:
            cate1     = s[8]
            cate2     = s[9]
            cate3     = s[10]
            cate4     = s[11]

        try:
            dt_year  = dt_date[0:4]
            dt_month = dt_date[4:6]
            dt_day   = dt_date[6:8]
            target_day = datetime.date(int(dt_year), int(dt_month), int(dt_day))
            c_week = weeks[target_day.weekday()]

        except:
            c_week = ''

        print '\t'.join([ member_no, dt_date, dt_hour, c_week, action, cate, cate1, cate2, cate3, cate4, prd_id, prd_nm ])







