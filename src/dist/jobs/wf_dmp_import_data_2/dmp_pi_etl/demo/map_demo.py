#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'temp_ocb'
    src = 'temp_11st'
#    src = 'dmp_pi_id_pool'
    pass

def print_sw():
    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        t = [''] * 101
        t[0]  = s[0]
        t[14] = s[1]
        t[22] = s[2]

        print '\t'.join(t)


def print_ocb():
# 0 dmp_uid
# 1 카드 구분
# 2 카드 이름
# 3 카드 발행일

# 0 ci
# 9 카드 구분
# 10 카드 이름
# 11 카드 일자

    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        t = [''] * 101

        t[0] = s[0]
        t[9] = s[1]
        t[10] = s[2]
        t[11] = s[3]
        if len(s) >= 5:
            t[14] = s[5]
            t[15] = s[4]

        if len(s) >= 7:
            t[21] = s[6]

        print '\t'.join(t)

def print_11st():
# 0 dmp_uid
# 1 시
# 2 군
# 3 동
    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

	t = [''] * 101

        t[0] = s[0]
        t[6] = s[1]
        t[7] = s[2]
        t[8] = s[3]
        t[23] = s[5]
        t[25] = s[4]

	print '\t'.join(t)

def print_id_pool():
## id_pool
# 0 ci
# 5 age
# 6 sex
# 7 laddr
# 8 maddr
# 9 saddr


# 0 dmp_uid
# 1 age
# 2 sex
# 3 ocb  si
# 4 addr gu
# 5 addr dong
    for line in sys.stdin:
        s = line.rstrip('\n').split('\t')

        t = [''] * 101

        t[0] = s[0]
        t[1] = s[5]
        t[2] = s[6]
        t[3] = s[7]
        t[4] = s[8]
        t[5] = s[9]
        ocb_id  = s[1]
        sw_id   = s[2]
        elev_id = s[3]


        if ocb_id != '' and sw_id != '' and elev_id != '':
            t[13] = '06'
        elif ocb_id != '' and sw_id != '' and elev_id == '':
            t[13] = '07'
        elif elev_id != '' and sw_id != '' and ocb_id == '':
            t[13] = '04'
        elif elev_id != '' and ocb_id != '' and sw_id == '':
            t[13] = '05'
        elif elev_id != '' and ocb_id == '' and sw_id == '':
            t[13] = '01'
        elif sw_id != '' and ocb_id == '' and elev_id == '':
            t[13] = '02'
        elif ocb_id != '' and sw_id == '' and elev_id == '':
            t[13] = '03'
    
        print '\t'.join(t) 

if 'temp_ocb' in src:
    print_ocb()
elif 'temp_sw' in src:
    print_sw()
elif 'temp_11st' in src:
    print_11st()
elif 'dmp_pi_id_pool' in src:
    print_id_pool()


