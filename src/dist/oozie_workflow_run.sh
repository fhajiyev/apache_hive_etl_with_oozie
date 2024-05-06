#!/bin/sh

jobPropertiesPath=$1

oozie job -oozie http://172.22.224.34:11200/oozie -config $jobPropertiesPath -run
