#!/usr/bin/python
#-*- coding: utf-8 -*-


# mart_mbr_mst
# hdfs://skpds/data_bis/ocb/MART/MBR/mart_mbr_mst

# mart_card_mst
# hdfs://skpds/data_bis/ocb/MART/CTR/mart_card_mst/

#dw_mbr_pnt_info
#hdfs://skpds/data_bis/ocb/DW/NXM_BIL/dw_mbr_pnt_info

#mart_tot_agrmt_mgmt_mst
#-input hdfs://skpds/data_bis/ocb/MART/MBR/mart_tot_agrmt_mgmt_mst \

# 앱회원 마스터
#hdfs://skpds/data_bis/ocb/MART/APP/mart_app_mbr_mst


import sys
import os

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'mart_mbr_mst'
    src = 'hdfs://skpds/data_bis/ocb/MART/APP/mart_app_mbr_mst'

    pass

def print_ocb_mbr():
    i_mbr_id = 0
    i_ci     = 2

    for line in sys.stdin:
        line = line.rstrip('\n').split('\t', 1)

        s = line[1][1:-1].split(',')

        dmp_uid = s[i_ci].strip()

        if dmp_uid == '' or dmp_uid == '\N':
            continue

        mbr_id = s[i_mbr_id].strip()
        print '\t'.join([ mbr_id, dmp_uid])

def print_ocb_pt():
    i_mbr_id   = 1
    i_avl_pnt  = 4
    i_mbrshp_pgm_id = 0
    i_pnt_knd_cd = 2

    for line in sys.stdin:
        line = line.rstrip('\n').split('\t', 1)

        s = line[1][1:-1].split(',')

        mbr_id = s[i_mbr_id].strip()
       
        if mbr_id == '' or mbr_id == '\N' or mbr_id == '-1':
            continue

        pgm_id = s[i_mbrshp_pgm_id].strip()
        if pgm_id != 'A':
            continue

        pnt_knd_cd = s[i_pnt_knd_cd].strip()
        if pnt_knd_cd not in ['H11','H15','O11','O12','O13','O14','O15',
                          'O16','O17','O18','O21','O22','O23','O24',
                          'O25','O26','O27','O28','O29','O31','O32',
                          'O33','O35','O41','O4A','O4B','O4H','O4I',
                          'O4S','O4T','O4X','Y']:
            continue

        avl_pnt = s[i_avl_pnt].strip()
        if avl_pnt == '' or avl_pnt == '\N' or avl_pnt == '0':
            continue

        print '\t'.join([ mbr_id, avl_pnt, 'pt'])

def print_ocb_os():
    for line in sys.stdin:
        line = line.rstrip('\n').split('\t', 1)

        s = line[1][1:-1].split(',')
        
        mbr_id = s[0].strip()
        mbr_os = s[5].strip()


        if mbr_id == '' or mbr_id == '\N' or mbr_id == '-1':
            continue

        if mbr_os == '42':
            os_str = 'Android'
        elif mbr_os == '43':
            os_str = 'IOS'
        else:
            continue

 
        print '\t'.join([ mbr_id, os_str, '', '', '', 'os'])





def print_ocb_agree():
    i_mbr_id   = 0
    i_ocb_mktng_agrmt_yn  = 1
    i_push_rcv_agrmt_yn = 14
    i_bnft_mlf_push_agrmt_yn = 16

    for line in sys.stdin:
        line = line.rstrip('\n').split('\t', 1)

        s = line[1][1:-1].split(',')

        mbr_id = s[i_mbr_id].strip()

        if mbr_id == '' or mbr_id == '\N' or mbr_id == '-1':
            continue

        ag1 = s[i_ocb_mktng_agrmt_yn].strip()
        ag2 = s[i_push_rcv_agrmt_yn].strip()
        ag3 = s[i_bnft_mlf_push_agrmt_yn].strip()

        if ag1 == '1' and ag2 == '1' and ag3 == '1':
            print '\t'.join([ mbr_id, '', '', '', '2'])


if 'mart_mbr_mst' in src:
    print_ocb_mbr()
elif 'dw_mbr_pnt_info' in src:
    print_ocb_pt()
elif 'mart_tot_agrmt_mgmt_mst' in src:
    print_ocb_agree()
elif 'mart_app_mbr_mst' in src:
    print_ocb_os()


 

