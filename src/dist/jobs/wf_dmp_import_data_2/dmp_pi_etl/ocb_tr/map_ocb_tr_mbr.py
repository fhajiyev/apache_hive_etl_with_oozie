#!/usr/bin/python
#-*- coding: utf-8 -*-


## mart_anal_sale_info
# hdfs://skpds/data_bis/ocb/MART/BIL/mart_anal_sale_info

import sys
import datetime

try:
    day_delta = int(sys.argv[1])
except:
    day_delta = 1

target_day = (datetime.datetime.now() - datetime.timedelta(days = day_delta))
target_day_str = target_day.strftime('%Y%m%d')

weeks = ( 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' )

dict_acode = dict()
dict_mcode = dict()
dict_cpncd = dict()

f = open('ocb_info.dat', 'r')
lines = f.readlines()
for line in lines:
    s = line.rstrip('\n').split('\t')
    # 제휴사 코드 - A - 제휴사 이름 - 대 - 중 - 소

    if s[1] == 'A':
	dict_acode[s[0]] = { 'aname' : s[2], 'll' : s[3], 'mm' : s[4], 'ss' : s[5], 'lc' : s[6], 'mc' : s[7], 'sc' : s[8] }


for line in lines:
    s = line.rstrip('\n').split('\t')

    # 제휴사 코드 - M - 가맹점 코드 - 가맹점 이름
    if s[1] == 'M':
	try:
            dict_mcode[s[2]] = { 'acode' : dict_acode[s[0]], 'mname' : s[3] }
	except:
            pass

f.close()


f = open('ocb_coupon.dat', 'r')
lines = f.readlines()
for line in lines:
    line = line.rstrip('\n').split('\t', 1)
    s = line[1][1:-1].split(',')

    cpn_cd = s[1].strip()
    cpn_nm = s[2].strip()

    dict_cpncd[cpn_cd] = cpn_nm
    



i_mbr_id          = 5
i_rcv_dt          = 0
i_slip_cd         = 4
i_cpn_prd_qty     = 34 
i_occ_pnt         = 30 
i_avl_pnt         = 31
i_stlmt_alcmpn_cd = 16 
i_cncl_slip_tp_cd = 7
i_stlmt_mcnt_cd   = 14
i_cpn_prd_cd      = 12

for line in sys.stdin: 
    line = line.rstrip('\n').split('\t', 1)
    s = line[1][1:-1].split(',')


    mbr_id          = s[i_mbr_id].strip()
    rcv_dt          = s[i_rcv_dt].strip()
    slip_cd         = s[i_slip_cd].strip()
    cpn_prd_qty     = s[i_cpn_prd_qty].strip()
    occ_pnt         = s[i_occ_pnt].strip()
    avl_pnt         = s[i_avl_pnt].strip()
    stlmt_alcmpn_cd = s[i_stlmt_alcmpn_cd].strip()
    cncl_slip_tp_cd = s[i_cncl_slip_tp_cd].strip()
    stlmt_mcnt_cd   = s[i_stlmt_mcnt_cd].strip()
    cpn_prd_cd      = s[i_cpn_prd_cd].strip()

    if rcv_dt != target_day_str:
	continue

    c_week = weeks[target_day.weekday()]

    # mbr_id - 날짜 - 시간대 ('') - 요일 - 행태 종류 - 제휴사 코드 - 제휴사 이름...
    acode = stlmt_alcmpn_cd
    mcode = stlmt_mcnt_cd
    aname = dict_acode.get(acode, {'aname' : '', 'll':'', 'mm':'', 'ss' : '', 'lc' : '', 'mc' : '', 'sc' : '' })
    mname = dict_mcode.get(mcode, {'mname' : '' })

    anames = [''] * 3
    anames[0] = aname['ll']
    anames[1] = aname['mm']
    anames[2] = aname['ss']

    to_aname = '||'.join(anames)
    if to_aname == '||||':
        to_aname = ''

    lc = aname['lc']
    mc = aname['mc']
    sc = aname['sc']
	

    if slip_cd in ['01', '05', '15', '35', '37', '65'] and cncl_slip_tp_cd not in ['1', '2', 'C']:
	action = '01'
	pt = abs(int(occ_pnt))

    elif slip_cd == '11' and cncl_slip_tp_cd not in ['1', '2', 'C']:
	action = '02'
	pt = abs(int(avl_pnt))
    else:
	continue

    if cpn_prd_cd == 'Y':
        cpn_prd_cd = ''


    cpn_prd_nm = dict_cpncd.get(cpn_prd_cd, '')


    print '\t'.join([ mbr_id, rcv_dt, '', c_week, action, acode,
		aname['aname'], lc, mc, sc, 
		anames[0], anames[1], anames[2],
		to_aname,
		mcode, 
		mname['mname'],
		str(pt), cpn_prd_cd, cpn_prd_nm])

