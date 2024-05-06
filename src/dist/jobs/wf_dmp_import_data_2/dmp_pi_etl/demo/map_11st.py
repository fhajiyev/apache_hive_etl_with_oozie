#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os
# tb_evs_ods_f_tr_ord_clm_delvplace
# hdfs://skpds/data_db/11st/raw/evs_tr_ord_clm_delvplace_ogg/
# 배송지 일련번호 - 빈칸 - 시도 - 군구 - 동

try:
    src = os.environ['mapreduce_map_input_file']
except:
    src = 'evs_tr_ord_clm_delvplace_ogg'
#    src = ''

    pass

def proc_11st_tr_loc():
    i_delvplace_seq = 0
    i_rcvr_mail_no  = 4
    zip_code = dict()

    f = open('zip_code.dat', 'r')
    lines = f.readlines()
    f.close()

    i_zip_code = 0
    i_sido     = 2
    i_sigungu  = 3
    i_dong     = 4

    for line in lines:
	s = line.rstrip('\n').split('\001')
	zip_code[s[i_zip_code]] = [ s[i_sido], s[i_sigungu], s[i_dong] ]

    lines = None

    for line in sys.stdin:
        s = line.rstrip('\n').split('\001')

	try:

            delvplace_seq = s[i_delvplace_seq].strip()
            rcvr_mail_no  = s[i_rcvr_mail_no].strip()
	except:
	    continue


	if zip_code.has_key(rcvr_mail_no):

	    lad_dic = zip_code[rcvr_mail_no][0]
	    mad_dic = zip_code[rcvr_mail_no][1]
	    sad_dic = zip_code[rcvr_mail_no][2]
            lad = lad_dic
	    if mad_dic != '' and lad_dic != '':
		mad = '%s %s' % (lad_dic, mad_dic)
	    else:
		mad = ''
	    if sad_dic != '' and mad_dic != '' and lad_dic != '':
		sad = '%s %s %s' % (lad_dic, mad_dic, sad_dic)
	    else:
		sad = ''
            print '\t'.join([ delvplace_seq, lad, mad, sad ])

if 'evs_tr_ord_clm_delvplace_ogg' in src:
    proc_11st_tr_loc()
else:
    for line in sys.stdin:
	print line.rstrip('\n')
	

	
