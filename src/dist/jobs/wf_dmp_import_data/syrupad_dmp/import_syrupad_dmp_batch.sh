#!/bin/sh

export LANG=ko_KR.UTF-8

DMP_PREPARE_HOME=/app/dmp/dmp/jobs/wf_dmp_import_data/syrupad_dmp

cd $DMP_PREPARE_HOME

mkdir -p logs

DAYS=14

find $DMP_PREPARE_HOME/logs/ -name "*.log" -mtime +$DAYS -type f -exec rm -f {} \;

$DMP_PREPARE_HOME/import_syrupad_dmp.sh 1> $DMP_PREPARE_HOME/logs/import_syrupad_dmp.`date '+%Y%m%d%H%M'`.log 2>&1
