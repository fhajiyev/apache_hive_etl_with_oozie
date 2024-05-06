#!/bin/bash


DD=`date +%Y%m%d00 -d "5 day ago"`

/app/di/script/run_hivetl.sh -e  "alter table dmp_pi.prod_uid_life drop partition (part_hour < $DD);"
/app/di/script/run_hivetl.sh -e  "alter table dmp_pi.prod_data_source_store drop partition (part_hour < $DD, data_source_id=23);"

