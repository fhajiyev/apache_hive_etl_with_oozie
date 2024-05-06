#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys

dict_cate = dict()
f = open('11st_disp_ct_dic.dat', 'r')
lines = f.readlines()
f.close()

for line in lines:
    s = line.rstrip('\n').split('\001')
    dict_cate[s[0]] = dict() 
    dict_cate[s[0]]['lm'] = s[20]
    dict_cate[s[0]]['mm'] = s[19]
    dict_cate[s[0]]['sm'] = s[18]
    dict_cate[s[0]]['lc'] = s[6]
    dict_cate[s[0]]['mc'] = s[5]
    dict_cate[s[0]]['sc'] = s[4]
    dict_cate[s[0]]['name'] = s[2]
    dict_cate[s[0]]['code'] = s[0]

lines = None

## mapper :         print '%s\t%s' % (member_no, ci)
## mapper :         print '%s\t%s\t%s\t%s\t%s\t%s' % (member_no, dt_date, dt_hour, action, c_week, brand_cd, cate)

prev_mbr_id = ''
prev_order = list()
order = list()
prev_ci = ''
ci = ''

def print_result():
#    if prev_ci == '++1dw56Ch/VytmloS7DdDpI48XkAU3cuGELE9HrXaTA+3wuB1nU36vJ9FnZKVnvbUPQmii1ZAhiEpNv0wNOE6Q==':
#        print '------'
#        print prev_ci
#        print prev_mbr_id, len(prev_order)
#        print prev_order
#        print '------'
    if 1:
       if len(prev_order) > 0:
            for tr in prev_order:

                if 1:
#                try:
                    t = [''] * 101
                    t[0] = prev_ci
                    t[1] = tr[0]
                    t[2] = tr[1]
                    t[3] = tr[2]
                    t[4] = tr[3]

#                    if tr[5] != '0':
                    if 0:    
                        t[5] = tr[5]
                        t[6] = tr[6]
                        t[7] = tr[7]
                        t[8] = tr[8]
                        t[9]  = dict_cate.get(t[5], {'name':''})['name']
                        t[10]  = dict_cate.get(t[6], {'name':''})['name']
                        t[11]  = dict_cate.get(t[7], {'name':''})['name']

                    else:
                        t[7] = tr[4]
                        t[5] = dict_cate.get(t[7], {'lc':''})['lc']
                        t[6] = dict_cate.get(t[7], {'mc':''})['mc']
                        t[8] = ''

                        t[9]   = dict_cate.get(t[7], {'lm':''})['lm']
                        t[10]  = dict_cate.get(t[7], {'mm':''})['mm']
                        t[11]  = dict_cate.get(t[7], {'sm':''})['sm']

#                    t[12]  = dict_cate.get(t[8], {'name':''})['name']
                    t[13]  = '||'.join(t[9:12])
#                    t[13]  = '||'.join(t[9:13])

#                    if t[8] == '0':
#                        t[8] = ''

                    t[16] = tr[9]
                    t[17] = tr[10]

                    print '\001'.join(t)
#               except:
#                   pass


for line in sys.stdin:
    s = line.rstrip('\n').split('\t')

    mbr_id = s[0]

    if len(s) == 2:
        ci = s[1]
    else:
        order = s[1:]

    if prev_mbr_id == '':
        prev_mbr_id = mbr_id
        if len(s) == 2:
            prev_ci     = ci
	else:
            prev_order = list()
            prev_order.append(order)

    if prev_mbr_id != mbr_id:
        if  prev_ci != '':
            print_result()
        prev_order = list()
        prev_ci = ''

    prev_mbr_id = mbr_id
    
    if len(s) == 2:
        prev_ci = ci
    else:
        prev_order.append(order)

if prev_mbr_id == mbr_id and prev_ci != '':
    print_result()

