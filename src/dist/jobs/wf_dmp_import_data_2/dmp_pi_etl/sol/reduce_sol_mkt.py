#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
import datetime

prev_mkt_id = ''
prev_taid   = ''
prev_mid    = ''
prev_data   = list()

weeks = ( 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' )

dic_taid = dict()
f = open('taid_dic.dat', 'r')
lines = f.readlines()
f.close()

for line in lines:
    s = line.rstrip('\n').split('\001')

    _taid = s[0].strip()
    _name = s[2].strip()
    _cate = s[4].strip()

    dic_taid[_taid] = { 'n' : _name, 'c' : _cate }

lines = None

dic_mkt = dict()
f = open('mkt_name.dat', 'r')
lines = f.readlines()
f.close()

for line in lines:
    s = line.rstrip('\n').split('\t')

    _mkt_cd   = s[0].strip()
    _mkt_name = s[1].strip()
    dic_mkt[_mkt_cd] = _mkt_name
lines = None

dic_cate = dict()
f = open('sol_cate.dat', 'r')
lines = f.readlines()
f.close()

for line in lines:
    s = line.rstrip('\n').split('\t')

    _cate_code = s[0].strip()
    _cate_name = s[1].strip()

    dic_cate[_cate_code] = _cate_name
lines = None

dic_ocb_merch = dict()
f = open('sid2ocb_merch_name.dat', 'r')
lines = f.readlines()
f.close()

for line in lines:
    s = line.rstrip('\n').split('\t')

    _sid   = s[0].strip()
    _bname = s[1].strip()

    dic_ocb_merch[_sid] = _bname
lines = None

dic_mid_merch = dict()
f = open('mid2merch_name.dat', 'r')
lines = f.readlines()
f.close()

for line in lines:
    s = line.rstrip('\n').split('\t')

    _mid   = s[0].strip()
    _bname = s[1].strip()

    dic_mid_merch[_mid] = _bname

lines = None

def print_data(pdata):

    for d in pdata:
#		out = [ mkt_id, '', -->std_tm, mbr_id, action, channel, sid, svc_sltn_id ]
        std_tm = d[0]
        mbr_id = d[1]
	act    = d[2]
	ch     = d[3]
	sid    = d[4]
	sol    = d[5]
        try:

	    dt_date = std_tm[0:8]
	    dt_hour = std_tm[8:10]
	    c_week  = weeks[datetime.date(int(std_tm[0:4]), int(std_tm[4:6]), int(std_tm[6:8])).weekday()]
	except:
	    dt_date = ''
	    dt_hour = ''
	    c_week  = ''

              
        mkt_name = dic_mkt.get(prev_mkt_id, '')
        dic_taid_item = dic_taid.get(prev_taid, {'n' : '', 'c' :''})
        aname = dic_taid_item['n']
        cate_cd = dic_taid_item['c']

        lc = cate_cd[0:4]
        mc = cate_cd[0:2]
        sc = cate_cd

        lm = dic_cate.get(lc, '')
        mm = dic_cate.get(mc, '')
        sm = dic_cate.get(sc, '')

        if sid != '':
            bcode = sid
            bname = dic_ocb_merch.get(bcode, '') # 매장 이름
        else:
            bcode = prev_mid
            bname = dic_mid_merch.get(bcode, '')

#               즉 미리줌 액션은 피카소에 무조건 마케팅 이름이 있다.
#		if act in [ '01', '02', '03' ] and mkt_name == '':
#               taid 는 자연스럽게 마지막 것이 남음. 세컨더리 소팅에 의해 prev_taid가 최대값이고 최후값임

        if 1:

            print '\t'.join([ mbr_id, 
                              dt_date, dt_hour, c_week, act, ch,
                              prev_mkt_id, mkt_name,
      	                      prev_taid, aname,
                              lc, mc, sc, lm, mm, sm, '||'.join([lm,mm,sm]),
                              bcode, bname
                            ])

 
for line in sys.stdin:
    s = line.strip('\n').split('\t')

    mkt_id  = s[0]

    if len(s) == 3:
	taid  = s[1]
	mid   = s[2]
    else: 
        data  = s[2:]

    if prev_mkt_id == '':
        prev_mkt_id = mkt_id

	if len(s) == 3:
	    prev_taid = taid
	    if mid != '':
                prev_mid  = mid
	else:
	    prev_data = list()
            prev_data.append(data)
	
    if prev_mkt_id != mkt_id:
	if len(prev_data) > 0:
	    print_data(prev_data)

        prev_data = list()

    prev_mkt_id  = mkt_id
    
    if len(s) == 3:
        prev_taid = taid
	if mid != '':
            prev_mid  = mid
    else:
        prev_data.append(data)

if prev_mkt_id == mkt_id:
    if len(prev_data) > 0:
	print_data(prev_data)


