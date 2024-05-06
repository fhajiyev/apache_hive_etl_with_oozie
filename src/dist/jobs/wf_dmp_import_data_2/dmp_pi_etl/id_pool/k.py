#!/usr/bin/python
#-*- coding: utf-8 -*-


# mart_card_mst
# hdfs://skpds/data_bis/ocb/MART/CTR/mart_card_mst/

import sys
import os


laddr = dict()
maddr = dict()
saddr = dict()

f = open('ocb_code.dat', 'r')
lines = f.readlines()
f.close()

for line in lines:
    s = line.rstrip('\n').split('\t')

    cate  = s[0].strip()
    ak    = s[1].strip()
    av    = s[6].strip()
    pa    = s[9].strip()

    if cate == 'HJD_LGRP_CD':
        laddr[ak] = av

    elif cate == 'HJD_MGRP_CD':
        maddr[ak] = [None] * 2
        maddr[ak][0] = av 
        maddr[ak][1] = pa 

    elif cate == 'HJD_SGRP_CD':
        saddr[ak] = [None] * 2
        saddr[ak][0] = av
        saddr[ak][1] = pa


for s in saddr:
#    print maddr[m][1]
    try:
        print laddr[maddr[saddr[s][1]][1]], maddr[saddr[s][1]][0], saddr[s][0]
    except:
        print '---------------'


