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
print_bstr "Demographics OCB = 19"

# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_push_rctn_ctnt/2017/07/30	푸쉬
# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_feed_clk_ctnt/2017/07/31	피드


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='19')


SELECT DISTINCT

   aa.ci_no,
   'Y', 
   aa.app_mbr_yn,
   aa.lckr_use_yn,
   aa.ocb_join_dt,
   aa.app_fst_cnct_dt,
   aa.lckr_join_dt,
   aa.push_yn,
   aa.avl_pnt,
   aa.age,
   aa.gndr_fg_cd,
   aa.frng_yn,
   aa.mrrg_yn,

   aa.sido_hm,
   REGEXP_REPLACE(CASE
      WHEN aa.sido_hm <> '' AND aa.sigungu_hm <> '' THEN CONCAT(aa.sido_hm, ' ', aa.sigungu_hm)
      ELSE ''
   END, '세종 .', '세종 세종시'),

   REGEXP_REPLACE(CASE
      WHEN aa.sido_hm <> '' AND aa.sigungu_hm <> '' AND aa.dong_hm <> '' THEN CONCAT(aa.sido_hm, ' ', aa.sigungu_hm, ' ', aa.dong_hm)
      ELSE ''
   END, '세종 .', '세종 세종시'),

   aa.sido_job,
   REGEXP_REPLACE(CASE
      WHEN aa.sido_job <> '' AND aa.sigungu_job <> '' THEN CONCAT(aa.sido_job, ' ', aa.sigungu_job)
      ELSE ''
   END, '세종 .', '세종 세종시'),

   REGEXP_REPLACE(CASE
      WHEN aa.sido_job <> '' AND aa.sigungu_job <> '' AND aa.dong_job <> '' THEN CONCAT(aa.sido_job, ' ', aa.sigungu_job, ' ', aa.dong_job)
      ELSE ''
   END, '세종 .', '세종 세종시'),




   aa.jobp_nm,
   aa.dept_nm,
   aa.car_poss_yn,
   aa.carknd_nm,
   aa.car_mdl, 

   '','','','','','','',
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

   a.ci_no,
   CASE 
      WHEN a.app_mbr_yn IS NULL THEN 'N'
      WHEN a.app_mbr_yn = '1' THEN 'Y'
      ELSE 'N'
   END AS app_mbr_yn,
  
   CASE
      WHEN b.lckr_use_yn IS NULL THEN 'N'
      WHEN b.lckr_use_yn IN ('1', 'Y') THEN 'Y'
      ELSE 'N'
   END AS lckr_use_yn,

   if (a.ocb_join_dt is null,       '',     a.ocb_join_dt) as ocb_join_dt,   
   if (a.app_fst_cnct_dt is null,   '', a.app_fst_cnct_dt) as app_fst_cnct_dt,
   if (a.lckr_join_dt is null OR a.lckr_join_dt = '99990101',      '',    a.lckr_join_dt) as lckr_join_dt,
   
   if (c.mbr_id is null, 'N', 'Y') as push_yn,

   a.avl_pnt as avl_pnt,
   
   CASE
      WHEN a.bthdt IS NOT NULL AND a.bthdt <> '99990101' AND age > 0 THEN age
      ELSE ''
   END AS age,

   CASE
      WHEN a.gndr_fg_cd IS NULL THEN ''
      WHEN a.gndr_fg_cd = '1' THEN 'M'
      WHEN a.gndr_fg_cd = '2' THEN 'F'
      ELSE ''
   END AS gndr_fg_cd,   

   CASE
      WHEN a.frgn_yn IS NULL THEN ''
      WHEN a.frgn_yn IN ('1', 'Y') THEN 'L'
      WHEN a.frgn_yn IN ('2', 'X') THEN 'F'
      ELSE ''
   END AS frng_yn,

   CASE
      WHEN a.mrrg_yn IS NULL THEN 'N'
      WHEN a.mrrg_yn IN ('1', 'Y') THEN 'Y'
      ELSE 'N'
   END AS mrrg_yn,


   if (sido_hm.dtl_cd_nm    is null OR sido_hm.dtl_cd_nm    = '미입력',       '',        sido_hm.dtl_cd_nm) as sido_hm, 
   if (sigungu_hm.dtl_cd_nm is null OR sigungu_hm.dtl_cd_nm = '미입력',       '',     sigungu_hm.dtl_cd_nm) as sigungu_hm,
   if (dong_hm.dtl_cd_nm    is null OR dong_hm.dtl_cd_nm    = '미입력',       '',        dong_hm.dtl_cd_nm) as dong_hm,

   if (sido_job.dtl_cd_nm    is null OR sido_job.dtl_cd_nm    = '미입력',       '',        sido_job.dtl_cd_nm) as sido_job,
   if (sigungu_job.dtl_cd_nm is null OR sigungu_job.dtl_cd_nm = '미입력',       '',     sigungu_job.dtl_cd_nm) as sigungu_job,
   if (dong_job.dtl_cd_nm    is null OR dong_job.dtl_cd_nm    = '미입력',       '',        dong_job.dtl_cd_nm) as dong_job,

   if (a.jobp_nm is null,       '', a.jobp_nm) as jobp_nm,
   if (a.dept_nm is null,       '', a.dept_nm) as dept_nm,

   CASE
       WHEN a.car_poss_yn IS NULL THEN 'N'
       WHEN a.car_poss_yn IN ('1', 'Y') THEN 'Y'
       ELSE 'N'
   END AS car_poss_yn,


   if (d.carknd_nm is null,  '', d.carknd_nm) as carknd_nm,

   if (a.car_mdl is null,    '', a.car_mdl) as car_mdl
  
   FROM
   (

    SELECT

    a1.mbr_id,
    a1.ci_no,
    a1.app_mbr_yn,
    a1.ocb_join_dt,
    a1.app_fst_cnct_dt,
    a1.lckr_join_dt,
    a1.avl_pnt,
    a1.age,
    a1.bthdt,
    a1.gndr_fg_cd,
    a1.frgn_yn,
    a1.mrrg_yn,
    a1.home_hjd_sgrp_cd,
    a1.jobp_hjd_sgrp_cd,
    a1.jobp_nm,
    a1.dept_nm,
    a1.car_poss_yn,
    a1.carknd_cd,
    a1.car_mdl

    FROM

    ocb.mart_mbr_mst a1
    
    WHERE
    a1.ci_no IS NOT NULL AND a1.ci_no <> '' AND a1.ci_no not like '%\u0001%'
    AND mbr_sts_cd = 'A'

   ) a

   LEFT JOIN

   (

     SELECT
     mbr_id,
     lckr_use_yn
     FROM
     ocb.mart_app_mbr_mst
     WHERE
     base_dt = '${TODAY_JOB_DATE}'

   ) b

   ON a.mbr_id = b.mbr_id
  
   LEFT JOIN

   (
    
     SELECT
     mbr_id
     FROM
     ocb.mart_tot_agrmt_mgmt_mst
     WHERE ocb_mktng_agrmt_yn = '1' AND push_rcv_agrmt_yn = '1' AND bnft_mlf_push_agrmt_yn = '1'

   ) c

   ON a.mbr_id = c.mbr_id

   LEFT JOIN
   
   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_LGRP_CD'

   ) sido_hm

   ON SUBSTR(a.home_hjd_sgrp_cd, 1, 2) = sido_hm.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_MGRP_CD'

   ) sigungu_hm

   ON SUBSTR(a.home_hjd_sgrp_cd, 1, 5) = sigungu_hm.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) dong_hm

   ON a.home_hjd_sgrp_cd = dong_hm.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_LGRP_CD'

   ) sido_job

   ON SUBSTR(a.jobp_hjd_sgrp_cd, 1, 2) = sido_job.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_MGRP_CD'

   ) sigungu_job

   ON SUBSTR(a.jobp_hjd_sgrp_cd, 1, 5) = sigungu_job.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) dong_job

   ON a.jobp_hjd_sgrp_cd = dong_job.dtl_cd

   LEFT JOIN

   ocb.dw_carknd_cd d

   ON

   a.carknd_cd = d.carknd_cd

   ) aa


;" > demo_ocb_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -f demo_ocb_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='19');"

etl_stat 'DONE'

rm -f demo_*.hql

rm -f *.log

