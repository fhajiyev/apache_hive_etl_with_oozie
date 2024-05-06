#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_11st_search\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`

TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

DROP_DAYS=`expr 182 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

etl_stat 'DOING'
print_bline
print_bstr "11번가 액티비티 = 9"


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

add jar hdfs://skpds/app/hive/udf/common-udf-1.0.jar;
CREATE TEMPORARY FUNCTION di_memnodecrypt AS 'com.di.hive.udf.MemnoDecrypt';

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${TARGET_JOB_DATE}00', data_source_id='9')
select distinct 
  u.ci                                                      as uid, 
  a.dt                                                      as col001, 
  ''                                                        as col002, 
  case from_unixtime(unix_timestamp(a.dt, 'yyyyMMdd'), 'u')
    when 1 then 'mon'
    when 2 then 'tue'
    when 3 then 'wed' 
    when 4 then 'thu'
    when 5 then 'fri'
    when 6 then 'sat' 
    else 'sun'
  end                                                       as col003, 
  ''                                                        as col004,
  a.channel                                                 as col005, 
  a.carrier_name,
  a.manufacturer,
  a.device_model,
  a.os_name,
  a.os_version,
  a.browser_name,
  a.browser_version,

  
  '','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','',''

from (
select distinct 
    memno, 
    dt as dt,
    '00' as channel,
    '' as carrier_name,
    '' as manufacturer,
    '' as device_model,
    '' as os_name,
    '' as os_version,
    '' as browser_name,
    '' as browser_version
 
    from SVC_CDPF.INTGT_LOG_BASE_EVS_MBL  where dt = '${TARGET_JOB_DATE}' and memno <> ''
union all 
select distinct 
    memno, 
    dt as dt, 
    '01' as channel,
    '' as carrier_name,
    '' as manufacturer,
    '' as device_model,
    '' as os_name,
    '' as os_version,
    '' as browser_name,
    '' as browser_version
 
    from SVC_CDPF.INTGT_LOG_BASE_EVS_WEB  where dt = '${TARGET_JOB_DATE}' and memno <> ''
union all
select distinct 
   di_memnodecrypt(member_no)                        as memno, 
   '${TARGET_JOB_DATE}'                              as dt,
   CASE WHEN poc_clf = 'app' THEN '00' ELSE '01' END as channel,
   carrier_name                                      as carrier_name,
   manufacturer                                      as manufacturer,
   device_model                                      as device_model,
   os_name                                           as os_name,
   os_version                                        as os_version,
   browser_name                                      as browser_name,
   browser_version                                   as browser_version

   FROM
   11st.log_client_live
   WHERE
   PART_HOUR BETWEEN '${TARGET_JOB_DATE}00' and '${TARGET_JOB_DATE}24'
   and member_no is not null and member_no <> ''
   and poc_clf is not null and poc_clf <> ''

) a join dmp_pi.id_pool u on (u.elev_id = a.memno);
" > 11st_act_${LAST_JOB_DATE_NOSLA}.hql


run /app/di/script/run_hivetl.sh -f 11st_act_${LAST_JOB_DATE_NOSLA}.hql
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='9');"

etl_stat 'DONE'

rm -f 11st_act_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log

