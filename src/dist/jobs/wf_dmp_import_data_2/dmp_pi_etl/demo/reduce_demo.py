#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os

prev_dmp_uid = ''
prev_id_pool = [''] * 5
prev_st_addr = [''] * 3
id_pool      = [''] * 5
st_addr      = [''] * 3
prev_pt      = ''
prev_mbr_ct  = ''
prev_push    = 0
prev_ocb_os  = ''
prev_sw_os   = ''
prev_11st_os = ''
prev_11st_agree = ''

# ocb, sw, 11

mbr_cate = { 
                '01' : [ 'N', 'N', 'Y'], 
                '02' : [ 'N', 'Y', 'N'], 
                '03' : [ 'Y', 'N', 'N'], 
                '04' : [ 'N', 'Y', 'Y'], 
                '05' : [ 'Y', 'N', 'Y'], 
                '06' : [ 'Y', 'Y', 'Y'], 
                '07' : [ 'Y', 'Y', 'N'] } 


def print_result():
    if 1:
        t = [''] * 101
        t[0]    = prev_dmp_uid

        t[1:6]  = prev_id_pool
        t[6:9]  = prev_st_addr
        t[13]   = prev_mbr_ct
        t[15]   = prev_pt
        t[21]   = prev_ocb_os
        t[22]   = prev_sw_os
        t[23]   = prev_11st_os
        t[25]   = prev_11st_agree

        # ocb : 2
        # sw  : 1 --> 비트 플래그

        t[16:19] = mbr_cate.get(prev_mbr_ct, ['N', 'N', 'N'])

        if prev_push == 3:
            t[19] = 'Y'
            t[20] = 'Y'
            t[14] = '03'
        elif prev_push == 2:
            t[14] = '02'
            t[19] = 'Y'
            t[20] = 'N'
        elif prev_push == 1:
            t[14] = '01'
            t[19] = 'N'
            t[20] = 'Y'
        else:
            t[14] = '04'
            t[19] = 'N'
            t[20] = 'N'

        print '\001'.join(t)

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    dmp_uid   = s[0]
    id_pool   = s[1:6]
    st_addr   = s[6:9]
    pt        = s[15]
    mbr_ct    = s[13]
    elev_agree = s[25]
    ocb_os    = s[21]
    sw_os     = s[22]
    elev_os   = s[23]

    if s[14]  == '':
        push = 0
    else:
        push  = int(s[14])

    if prev_dmp_uid == '':
        prev_dmp_uid   = dmp_uid

	if id_pool[0] != '':
            prev_id_pool   = id_pool
	if st_addr[0] != '':
            prev_st_addr   = st_addr
        if pt != '':
            prev_pt        = pt
        if mbr_ct != '':
            prev_mbr_ct    = mbr_ct
        if elev_agree != '':
            prev_11st_agree = elev_agree
        if ocb_os != '':
            prev_ocb_os    = ocb_os
        if sw_os  != '':
            prev_sw_os     = sw_os
        if elev_os != '':
            prev_11st_os   = elev_os

        prev_push = prev_push | push

	continue
	
    if prev_dmp_uid != dmp_uid:
        print_result()


        prev_id_pool = [''] * 5
        prev_st_addr = [''] * 3
        prev_pt   = ''
        prev_mbr_ct = ''
        prev_push = 0
        prev_11st_agree = ''
        prev_ocb_os = ''
        prev_sw_os = ''
        prev_11st_os = ''
    

    prev_dmp_uid  = dmp_uid

    if id_pool[0] != '':
        prev_id_pool   = id_pool
    if st_addr[0] != '':
        prev_st_addr   = st_addr
 
    if pt != '':
        prev_pt = pt
    if mbr_ct != '':
        prev_mbr_ct = mbr_ct

    if elev_agree != '':
        prev_11st_agree = elev_agree

    if ocb_os != '':
        prev_ocb_os = ocb_os
    if sw_os != '':
        prev_sw_os  = sw_os
    if elev_os != '':
        prev_11st_os = elev_os

    prev_push = prev_push | push


if prev_dmp_uid == dmp_uid:
    print_result()
