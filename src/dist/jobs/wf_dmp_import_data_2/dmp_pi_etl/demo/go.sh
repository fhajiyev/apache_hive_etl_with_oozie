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
print_bstr "Demographics = 1"

# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_push_rctn_ctnt/2017/07/30	푸쉬
# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_feed_clk_ctnt/2017/07/31	피드


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='1')


SELECT
   b.uid,
   CASE 
       WHEN b.maxage > 0 THEN b.maxage
       ELSE ''
   END,
   CASE
       WHEN b.maxgen = 101 OR b.maxgen = 11 OR b.maxgen = 1 THEN 'M'
       WHEN b.maxgen = 102 OR b.maxgen = 12 OR b.maxgen = 2 THEN 'F'
       ELSE ''
   END,
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


   FROM
   (

   SELECT
   a.uid           as uid,
   MAX(a.age)      as maxage,
   MAX(a.genclass) as maxgen
   FROM

   (

      SELECT
      ci_no                               as uid,
      CASE 
           WHEN age is null THEN 0
           WHEN age > 0 AND bthdt IS NOT NULL AND bthdt <> '99990101' THEN age
           ELSE 0
      END                                 as age, 
      CASE 
           WHEN gndr_fg_cd is null THEN 0
           WHEN gndr_fg_cd = '1' THEN 101             -- for M
           WHEN gndr_fg_cd = '2' THEN 102             -- for F
           ELSE 0
      END                                 as genclass -- values 1 2 X Y
      FROM
      ocb.mart_mbr_mst
      WHERE ci_no IS NOT NULL AND length(ci_no)>0 AND ci_no not like '%\u0001%' AND mbr_sts_cd = 'A'
      AND ((age IS NOT NULL AND bthdt IS NOT NULL AND bthdt <> '99990101')OR(gndr_fg_cd IS NOT NULL AND gndr_fg_cd IN ('1','2'))) -- age or gender present


      UNION ALL

      SELECT
      ci                                                                        as uid,
      CASE
           WHEN birth_day is null THEN 0 
           WHEN length(birth_day)=8 THEN CAST(substr('${LAST_JOB_DATE_NOSLA}', 1, 4)-substr(birth_day, 1, 4) as int)+1 
           ELSE 0
      END                                                                       as age,
      CASE
           WHEN sex is null THEN 0 
           WHEN sex = 'M' THEN 11                                                           -- for M
           WHEN sex = 'F' THEN 12                                                           -- for F                                                                   
           ELSE 0 
      END                                                                       as genclass -- values M F NULL      
      FROM
      smartwallet.mt3_member
      WHERE ci IS NOT NULL AND length(ci)>0 AND ci not like '%\u0001%' AND wallet_accept = 1 and wallet_accept1 = 1 and wallet_accept2 = 1 and vm_state_cd = '9' and length(last_auth_dt) = 14
      AND ((birth_day IS NOT NULL AND length(birth_day)=8)OR(sex IS NOT NULL AND sex IN ('M','F'))) -- age or gender present


      UNION ALL

      SELECT
      mem_ci                      as uid,
      CASE 
           WHEN age is null THEN 0            -- can use age value 
           WHEN age <> '' THEN CAST(age as int)
           ELSE 0
      END                         as age,
      CASE
           WHEN gndr is null THEN 0 
           WHEN gndr = 'M' THEN 1             -- for M
           WHEN gndr = 'F' THEN 2             -- for F
           ELSE 0
      END                         as genclass -- values M F N NULL
      FROM
      11st.tb_evs_base_m_mb_mem
      WHERE part_date='${LAST_JOB_DATE_NOSLA}'
      AND mem_ci IS NOT NULL AND length(mem_ci)>0 AND mem_ci not like '%\u0001%' AND mem_clf = '01' AND mem_typ_cd = '01' AND mem_stat_cd = '01'
      AND ((age IS NOT NULL AND age <> '')OR(gndr IS NOT NULL AND gndr IN ('M','F'))) -- age or gender present



   ) a
   group by a.uid

   ) b
   WHERE b.maxage > 0 OR b.maxgen > 0
;" > demo_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -f demo_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='1');"

etl_stat 'DONE'

rm -f demo_*.hql

rm -f *.log

