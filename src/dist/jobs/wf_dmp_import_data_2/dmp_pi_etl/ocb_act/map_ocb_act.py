#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import datetime

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'hdfs://skpds/product/BIS_SERVICES/OCB/INTG/OCB_D_INTG_LOG/poc=01/dt='
    src = 'hdfs://skpds/data_bis/ocb/MART/APP/mart_app_push_rctn_ctnt/2017/07/31'
    src = 'hdfs://skpds/data_bis/ocb/MART/APP/mart_app_feed_clk_ctnt/2017/07/10'
    src = 'dmp_pi_id_pool'
    pass


weeks     = ( 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' )

i_mbr_id          = 5
i_cdc_base_time   = 54
i_page_id         = 28
i_actn_id         = 29

for line in sys.stdin:

    if 'OCB_D_INTG_LOG' in src:
        s = line.rstrip('\n').split('\t')

	try:
            mbr_id          = s[i_mbr_id].strip()
            dt              = s[i_cdc_base_time].strip()
            page_id         = s[i_page_id].strip().lower()
            actn_id         = s[i_actn_id].strip().lower()
            filter_id       = page_id + actn_id
	except:
	    continue

        if mbr_id == '' or mbr_id == '\N':
            continue

        try:
            dt_year  = dt[0:4]
            dt_month = dt[4:6]
            dt_day   = dt[6:8]
#            dt_time  = dt[8:10]
            dt_date  = dt[0:8]
            target_day = datetime.date(int(dt_year), int(dt_month), int(dt_day))
            c_week = weeks[target_day.weekday()]

        except:
            continue
            pass

        if filter_id not in ['unknownpush_receive',
            'unknownapp_call',
            '/discoverunknown',
            '/mobileleafletunknown',
            '/checkinunknown',
            'j0100unknown',
            '/pushnoti/indicatorunknown',
            '/pushnoti/popupunknown',
            'j0101unknown',
            'unknownissue_coupon_result',
            'unknownapp_call',
            '/marketingpushunknown',
            '/locker/maindrag.unlock',
            '/explaincriticalpermissionunknown',
            'unknownlock_content_receive',
            'unknownpermission_status','unknownmarketingpush'] and page_id not in ['/locker/weather', '/locker/weatherponginstall']:

            print '\t'.join([ mbr_id, dt_date, '00', c_week ])

        else:
            continue
    elif 'ocb_push' in src:
        s = line.rstrip('\n').split('\001')

        mbr_id  = s[0].strip()
        push_id = s[1].strip()
        dt      = s[2].strip()

        try:
            dt_year  = dt[0:4]
            dt_month = dt[4:6]
            dt_day   = dt[6:8]
#            dt_time  = dt[8:10]
            dt_date  = dt[0:8]
            target_day = datetime.date(int(dt_year), int(dt_month), int(dt_day))
            c_week = weeks[target_day.weekday()]

        except:
            continue
            pass

        print '\t'.join([ mbr_id, dt_date, '01', c_week, push_id] )



    elif 'ocb_feed' in src:

        s = line.rstrip('\n').split('\001')

        mbr_id  = s[0].strip()
        feed_id = s[1].strip()
        dt      = s[2].strip()

        try:
            dt_year  = dt[0:4]
            dt_month = dt[4:6]
            dt_day   = dt[6:8]
#            dt_time  = dt[8:10]
            dt_date  = dt[0:8]
            target_day = datetime.date(int(dt_year), int(dt_month), int(dt_day))
            c_week = weeks[target_day.weekday()]

        except:
            continue
            pass


        print '\t'.join([ mbr_id, dt_date, '02', c_week, '', feed_id] )

    else:

        s = line.rstrip('\n').split('\t')
        member_no = s[1].strip()
        ci        = s[0].strip()
        
        if member_no == '-1' or member_no == '' or member_no == '\N':
	    continue

	if ci == '' or ci == '\N':
	    continue

        print '\t'.join([ member_no, ci ])

