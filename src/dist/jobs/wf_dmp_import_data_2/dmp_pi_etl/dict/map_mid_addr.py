#!/usr/bin/python
#-*- coding: utf-8 -*-

import sys
import os

for line in sys.stdin:
    s = line.rstrip('\n').split('\001')

    mid = s[0].strip()
    exp = s[6].strip()
    acode = s[17].strip()

    print '\t'.join([mid, exp, acode])







