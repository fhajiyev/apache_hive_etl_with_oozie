#!/bin/bash
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_etl/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_solution\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
#    $HDD fs -put proc_$CTIME.log hdfs://skpds/user/dmp_pi/dmp_pi_etl_stat/
}

die() {
    etl_stat 'FAIL';
    echo >&2 -e "\nERROR: $@\n"; exit 1;
}
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }
print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }
print_sstr() { printf "\e[93m$@\e[0m\n"; }


LAST_JOB_DAY=`expr 2 + $1`
LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`


DROP_DAYS=`expr 92 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

echo "
set mapreduce.job.reduces=256;
set mapre.job.queue.name=COMMON;
insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='13')


select distinct ci, spot_name, dwelltime,
'','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','',''

from (

select
    V.ci as ci
   ,T.tag_name as spot_name
   ,cast( ( UNIX_TIMESTAMP(V.exittime, 'yyyyMMddHHmmss')-UNIX_TIMESTAMP(V.entertime, 'yyyyMMddHHmmss') ) / 60 as int )  as dwelltime
from svc_cim.prod_daily_visit V 
join svc_cim.meta_tag_seq T on V.tag_seq=T.tag_seq
where v.parent_seq is not null and part_dt='$TARGET_JOB_DATE'


) cim;
" > cim_${LAST_JOB_DATE_NOSLA}.hql


run /app/di/script/run_hivetl.sh -f cim_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='13');"

etl_stat 'DONE'

rm -f cim_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log

