#!/bin/bash
mysql -h 172.22.136.50 --default-character-set=utf8 --user=dmpuser --password=`cat /app/dmp_pi/.mysql_password_pi` -s -N -e \
'select id from dmp.site_data_source where (max_recency > 1 and data_source_type="NORMAL") or (data_source_type="UID_UPLOAD");' > /app/dmp_pi/dmp_pi_etl/uid_lifetime/srcs.dat

/app/yarn_etl/bin/hadoop fs -put -f /app/dmp_pi/dmp_pi_etl/uid_lifetime/srcs.dat hdfs://skpds/user/dmp_pi/uid_life_srcs


