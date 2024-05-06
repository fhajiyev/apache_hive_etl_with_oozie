#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_dic/bin/hadoop
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

HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"



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

OUT_ETL_11ST_SEARCH=$WORKDIR/etl_11st_search
etl_stat 'DOING'
print_bline
print_bstr "11번가 검색 = 2"


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='2')

select
   AA.ci                          as uid
 , BB.dt                          as dateval
 , ''                             as timeval 
 , CASE from_unixtime(unix_timestamp(BB.dt, 'yyyyMMdd'), 'u')
     WHEN 1 THEN 'mon'
     WHEN 2 THEN 'tue'
     WHEN 3 THEN 'wed'
     WHEN 4 THEN 'thu'
     WHEN 5 THEN 'fri'
     WHEN 6 THEN 'sat'
     ELSE 'sun'
   END as weekday
 , BB.action
 , BB.word,

   '','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','',''


 from
 (

    select
    ci,
    elev_id
    from
    dmp_pi.id_pool
    where part_date = '${TODAY_JOB_DATE}'

 ) AA

join

 (

    select
    mbr_id,
    word,
    dt,
    'search' as action
 
    from
    svc_cdpf.intgt_srch_keyword_mbr   
    where
    dt = '${LAST_JOB_DATE_NOSLA}'
    and mbr_id not in ('-1', '', '\n', '0')
    and word not in ('', '\n')

 ) BB

 ON AA.elev_id = BB.mbr_id

;" > 11st_search_${LAST_JOB_DATE_NOSLA}.hql


/app/di/qcshell/bin/qcshell -b eda-hive2 -f 11st_search_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='2');"

etl_stat 'DONE'

rm -f 11st_search_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log


