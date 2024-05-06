#!/bin/sh


#LAST_JOB_DAY=2
#TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`


cd /app/dmp_pi/dmp_pi_etl/uid_lifetime

./drop_old_partition.sh

sleep 5

/app/di/script/run_hivetl.sh -f uid_life.hql
/app/di/script/run_hivetl.sh -f common_uid_life.hql

sleep 5

exit


