#!/bin/sh
HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_ocb_act\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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



LAST_JOB_DAY=2
LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`

DROP_DAYS=5
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

OUT_ETL_OCB_ACT=$WORKDIR/etl_ocb_act
etl_stat 'DOING'
print_bline
print_bstr "Demographics SW = 20"

# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_push_rctn_ctnt/2017/07/30	푸쉬
# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_feed_clk_ctnt/2017/07/31	피드


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store_alter partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='20')


SELECT DISTINCT
   

   aa.ci,
   'Y',
   aa.join_dt,
   aa.push_yn,
   aa.age,
   aa.sex,
   aa.nationality,
   '',

   aa.sido,
   CASE
      WHEN aa.sido <> '' AND aa.sigungu <> '' THEN CONCAT(aa.sido, ' ', aa.sigungu)
      ELSE ''
   END,

   CASE
      WHEN aa.sido <> '' AND aa.sigungu <> '' AND aa.dong <> '' THEN CONCAT(aa.sido, ' ', aa.sigungu, ' ', aa.dong)
      ELSE ''
   END,
   aa.building_name,




   '','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','',''

   FROM

   (



   SELECT

   a.ci,
   SUBSTR(a.join_dt, 1, 8) as join_dt,

   if (c.member_id is null, 'N', 'Y') as push_yn,
   
   CASE
           WHEN a.birth_day is null THEN ''
           WHEN length(a.birth_day)=8 THEN CAST(substr('${TODAY_JOB_DATE}', 1, 4)-substr(a.birth_day, 1, 4) as int)+1
           ELSE ''
   END AS age,

   if (a.sex is null, '', a.sex) as sex,

   CASE
           WHEN a.nationality is null THEN ''
           WHEN a.nationality = 'K' THEN 'L'
           WHEN a.nationality = 'A' THEN 'F'
           ELSE ''
   END AS nationality,

   if (b.sido          is null,    '', b.sido)          as sido,
   if (b.sigungu       is null,    '', b.sigungu)       as sigungu,
   if (b.dong          is null,    '', b.dong)          as dong,
   if (b.building_name is null,    '', b.building_name) as building_name


   FROM
   (

    SELECT

    a1.member_id,  
    a1.ci,
    a1.join_dt,
    a1.birth_day, 
    a1.sex,
    a1.nationality,
    a1.zip_code   

    FROM

    smartwallet.mt3_member a1
    
    WHERE
    a1.ci IS NOT NULL AND a1.ci <> '' AND a1.ci not like '%\u0001%'
    AND wallet_accept = 1 and wallet_accept1 = 1 and wallet_accept2 = 1 and vm_state_cd = '9' and length(last_auth_dt) = 14
  

   ) a

   
   LEFT JOIN

   (
    
     SELECT 
     m.member_id
     FROM
     smartwallet.mt3_member m
     JOIN 
     smartwallet.mt3_device_list d 
     ON 
     m.device_model = d.device_model
     WHERE
     m.wallet_accept is not null
     AND m.wallet_accept3 = 1
     AND m.vm_state_cd = '9'
     AND m.vm_ver >= '1301'
     AND m.os_version is not null
     AND m.token is not null
     AND m.push_server_type in ('3', '4', '5')
     AND m.noti_use_yn = 'Y' 

   ) c

   ON a.member_id = c.member_id

   LEFT JOIN
  
   (

     select
     zip_code,
     sido,
     sigungu,
     dong,
     building_name

     from
     smartwallet.dbm_mt3_zip_code
     where
     part_date = '${TODAY_JOB_DATE}'

   ) b
   
   ON a.zip_code = b.zip_code

   ) aa


;" > demo_sw_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -f demo_sw_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store_alter drop partition (part_hour < '${DROP_DATE}00', data_source_id='20');"

etl_stat 'DONE'

rm -f demo_*.hql

rm -f *.log

