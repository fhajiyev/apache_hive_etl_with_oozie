#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import datetime

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'hdfs://skpds/data/BIS_SERVICES/SMW/LOG/smw_client_log/dt=$LAST_JOB_DATE_NOSLA'
#    src = 'dmp_pi_id_pool'
    pass


weeks     = ( 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' )

i_mbr_id         = 4
i_base_time      = 0
i_current_page   = 29
i_action_id      = 28
i_push_id        = 60

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    if 'smw_client_log' in src:
        try:
            mbr_id          = s[i_mbr_id].strip()
            dt              = s[i_base_time].strip()
            page_id         = s[i_current_page].strip().lower()
            action_id       = s[i_action_id].strip().lower()
            filter_id       = page_id + action_id
            push_id         = s[i_push_id].strip()
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


        if page_id in ['/push/noti','/push/coin', '/push_device/noti']:
            if action_id == '' or action_id == '\N':
                if push_id != '': 
                    # push 인 경우
            
                    print '\t'.join([ mbr_id, dt_date, '01', c_week, push_id])
                    continue



        if page_id == '/appexec' or page_id == '' or page_id == '\N':
            continue

        if action_id in [ 'event_receive', 'server_comm',
                'background.display', 'background.display_native', 'background.dispaly_native' ]:
            continue

        if filter_id in [ '/start',
                '/start/guide',
                '/ble/icon', 'ble icon', '/ble/network/error', '/ble/noti', '/ble/iconble_tap.delete',
                '/preordericon', '/preordericonorder_tap.delete',
                '/pushnoti/popup', '/pushnoti/indicator', '/push/noti', '/pushnoti/indicatorindicator_tap.notidel',
                '/pushnoti/indicator_bluetooth',
                '/push/ticket',
                '/push/coin', '/push/coinlongpress.coin', '/wifi/noti', '/push/generalticket', '/push_device/noti',
                '/permission/main', '/noti/guide', '/welcome', '/noti/guidepushlist_tap.close', '/welcometap.close',
                '/shoppingbackground.display', '/newsbackground.display', '/guidelockbackground.display',
                '/guidein/noti', '/guideout/noti', '/guidewelcome', '/guidewelcometap.close',
                '/main/couponbackground.couponlist', '/btpopup', '/btpopupbottom_tap.cancelbtn']:
            continue

        print '\t'.join([ mbr_id, dt_date, '00', c_week])

    else:

        member_no = s[2].strip()
        ci        = s[0].strip()
        
        if member_no == '-1' or member_no == '' or member_no == '\N':
	    continue

	if ci == '' or ci == '\N':
	    continue

        print '\t'.join([ member_no, ci ])

