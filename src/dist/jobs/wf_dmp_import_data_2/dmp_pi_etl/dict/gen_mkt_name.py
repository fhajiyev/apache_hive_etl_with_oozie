#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys

dic_mkt_name= dict()
f = open('sol_svcsol_mktname.dat', 'r')
lines = f.readlines()
for line in lines:
    s = line.rstrip('\n').split('\t')
    dic_mkt_name[s[0]] = s[1]
f.close()

f = open('sol_mkt_svcsol.dat', 'r')
lines = f.readlines()
for line in lines:
    s = line.rstrip('\n').split('\t')
    mkt_name = dic_mkt_name.get(s[1], '')
    if mkt_name != '':
        print '\t'.join([s[0], mkt_name])
f.close()

