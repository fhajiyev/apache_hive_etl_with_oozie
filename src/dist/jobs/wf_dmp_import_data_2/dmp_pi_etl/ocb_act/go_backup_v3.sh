#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi

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



LAST_JOB_DAY=`expr 2 + $1`
LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`

DROP_DAYS=`expr 182 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

OUT_ETL_OCB_ACT=$WORKDIR/etl_ocb_act
etl_stat 'DOING'
print_bline
print_bstr "OCB 액티비티 = 4"

# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_push_rctn_ctnt/2017/07/30	푸쉬
# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_feed_clk_ctnt/2017/07/31	피드


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='4')


SELECT
   mb.ci_no,
   ma.dt,
   '',
   CASE from_unixtime(unix_timestamp(ma.dt, 'yyyyMMdd'), 'u')
      WHEN 1 THEN 'mon'
      WHEN 2 THEN 'tue'
      WHEN 3 THEN 'wed'
      WHEN 4 THEN 'thu'
      WHEN 5 THEN 'fri'
      WHEN 6 THEN 'sat'
      ELSE 'sun'
   END,
   '',
   ma.push_type,
   ma.push_id,
   ma.feed_id,
   ma.push_name,
   ma.feed_name,
  
   '',
   '','','','','','','','','','',
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

SELECT DISTINCT
  mbr_id                    as member_id,
  SUBSTR(cdc_base_time,1,8) as dt,
  '00'                      as push_type,  
  ''                        as push_id,
  ''                        as feed_id,
  ''                        as push_name,
  ''                        as feed_name 


FROM
       OCB.OCB_D_INTG_LOG 

WHERE  

       POC = '01'
       AND DT ='${LAST_JOB_DATE_NOSLA}'
       AND LOWER(CONCAT(TRIM(PAGE_ID),TRIM(ACTN_ID))) NOT IN ('unknownpush_receive'       ,'unknownapp_call'        ,'/discoverunknown'
                                                             ,'/mobileleafletunknown'     ,'/checkinunknown'        ,'j0100unknown'
                                                             ,'/pushnoti/indicatorunknown','/pushnoti/popupunknown' ,'j0101unknown'
                                                             ,'unknownissue_coupon_result','unknownapp_call'
                                                             ,'/marketingpushunknown'
                                                             ,'/locker/maindrag.unlock','/explaincriticalpermissionunknown'
                                                             ,'unknownlock_content_receive'
                                                             ,'unknownpermission_status'
                                                             ,'unknownmarketingpush'
                                                             )
       AND LOWER(PAGE_ID) NOT IN ('/locker/weather', '/locker/weatherponginstall')
       AND SUBSTR(cdc_base_time,1,8) = '${LAST_JOB_DATE_NOSLA}'

UNION ALL


SELECT DISTINCT
  mbr_id       as member_id,
  base_dt      as dt,
  '01'         as push_type,
  tgt_push_id  as push_id,
  ''           as feed_id,
  ''           as push_name,
  ''           as feed_name

FROM
       OCB.MART_APP_PUSH_RCTN_CTNT

WHERE
       base_dt     = '${LAST_JOB_DATE_NOSLA}'
       AND tgt_push_id <> ''
       AND push_rcv_yn = '1'


UNION ALL

SELECT DISTINCT
  mbr_id  as member_id,
  base_dt as dt,
  '02'    as push_type,
  ''      as push_id,
  feed_id as feed_id,
  ''      as push_name,
  feed_nm as feed_name

FROM
       OCB.MART_APP_FEED_CLK_CTNT

WHERE
       base_dt    = '${LAST_JOB_DATE_NOSLA}'
       AND feed_clk_yn = '1'


UNION ALL

SELECT DISTINCT
  mbr_id       as member_id,
  base_dt      as dt,
  '03'         as push_type,
  tgt_push_id  as push_id,
  ''           as feed_id,
  ''           as push_name,
  ''           as feed_name

FROM
       OCB.MART_APP_PUSH_RCTN_CTNT
WHERE

       base_dt     = '${LAST_JOB_DATE_NOSLA}'
       AND tgt_push_id <> ''
       AND push_clk_yn = '1'


UNION ALL

SELECT DISTINCT
  mbr_id       as member_id,
  base_dt      as dt,
  '04'         as push_type,
  ''           as push_id,
  feed_id      as feed_id,
  ''           as push_name,
  feed_nm      as feed_name

FROM
       OCB.MART_APP_FEED_CLK_CTNT
WHERE

       base_dt     = '${LAST_JOB_DATE_NOSLA}'
       AND feed_exps_yn = '1'



) ma


JOIN 

ocb.mart_mbr_mst mb

ON 

ma.member_id = mb.mbr_id

WHERE mb.ci_no IS NOT NULL AND length(ci_no)>0
;" > ocb_act_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -f ocb_act_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='4');"

etl_stat 'DONE'

rm -f ocb_act_*.hql

rm -f *.log

