#!/bin/sh


LAST_JOB_DAY=2
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`


cd /app/dmp_pi/dmp_pi_etl/cim_seg


/app/di/hive/bin/hive --hivevar day2before="$TARGET_JOB_DATE" < import_additional_aud.hql

sleep 5

exit


