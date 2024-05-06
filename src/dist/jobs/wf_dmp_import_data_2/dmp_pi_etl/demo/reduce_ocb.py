#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os


prev_mbr_id = ''
prev_card = list()
pt_sum = 0
prev_agree = '0'
prev_dmp_uid = ''
prev_os_str = ''

for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    mbr_id         = s[0]

    card_gubun = ''
    card_name  = ''
    card_issue = ''

    if len(s) == 4:
	card_gubun = s[1]
	card_name  = s[2]
	card_issue = s[3]
    elif len(s) == 2:
	dmp_uid    = s[1]
    elif len(s) == 3:
        pt         = s[1]
    elif len(s) == 6:
        os_str     = s[1]
    else:
        agree      = '2' 
	
    if prev_mbr_id == '':
        prev_mbr_id = mbr_id
	if len(s) == 2:
	    prev_dmp_uid = dmp_uid
	elif len(s) == 4:
            prev_card = list()
	    prev_card.append({'gubun' : card_gubun, 'name' : card_name, 'issue' : card_issue})
        elif len(s) == 3:
 	    pt_sum = int(pt)
        elif len(s) == 6:
            prev_os_str = os_str
        else:
            prev_agree = agree
            
        continue

	
    if prev_mbr_id != mbr_id:
        if prev_dmp_uid != '':
            print '\t'.join([prev_dmp_uid, '', '', '', str(pt_sum), prev_agree, prev_os_str])
            if len(prev_card) > 0:
                for card in prev_card:
                    print '\t'.join([prev_dmp_uid, card['gubun'], card['name'], card['issue']])

        prev_card = list() 
        pt_sum = 0
        prev_agree = '0'
        prev_dmp_uid = ''
        prev_os_str = ''
    

    prev_mbr_id  = mbr_id
    
    if len(s) == 4:
        prev_card.append({'gubun' : card_gubun, 'name' : card_name, 'issue' : card_issue})
    elif len(s) == 2:
        prev_dmp_uid = dmp_uid
    elif len(s) == 3:
        pt_sum += int(pt)
    elif len(s) == 6:
        prev_os_str = os_str
    else:
        prev_agree = '2'
	
if prev_mbr_id == mbr_id:
    if prev_dmp_uid != '':
        print '\t'.join([prev_dmp_uid, '', '', '', str(pt_sum), prev_agree, prev_os_str])

        if len(prev_card) > 0:
            for card in prev_card:
                print '\t'.join([prev_dmp_uid, card['gubun'], card['name'], card['issue']])
