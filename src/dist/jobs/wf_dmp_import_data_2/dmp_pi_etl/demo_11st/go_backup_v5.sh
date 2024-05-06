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
TODAY_JOB_DATE_730_AGO=`date +%Y%m%d -d "730 day ago"`
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`

DROP_DAYS=5
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

OUT_ETL_OCB_ACT=$WORKDIR/etl_ocb_act
etl_stat 'DOING'
print_bline
print_bstr "Demographics 11st = 18"

# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_push_rctn_ctnt/2017/07/30	푸쉬
# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_feed_clk_ctnt/2017/07/31	피드


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='18')



   SELECT

   aa.mem_ci,
   'Y',
   aa.bgn_ent_dy,
   aa.noti_shp_evt_bnft_inst_yn,
   aa.cmlt_pnt,
   aa.thm_pchsr_grd_nm,
   aa.age,
   aa.gndr,
   aa.natv_yn,
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

   aa.bldng,


   '','','','','','','',
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

   a.mem_ci,

   if (c.bgn_ent_dy                is null,  '',   REGEXP_REPLACE(SUBSTR(c.bgn_ent_dy,1,10),'-','')) as bgn_ent_dy,  
   if (e.noti_shp_evt_bnft_inst_yn is null, 'N',   e.noti_shp_evt_bnft_inst_yn) as noti_shp_evt_bnft_inst_yn,
   if (g.cmlt_pnt                  is null,  '',   g.cmlt_pnt) as cmlt_pnt,
   if (d.thm_pchsr_grd_nm          is null,  '',   REGEXP_REPLACE(d.thm_pchsr_grd_nm, '등급', '')) as thm_pchsr_grd_nm, 
   if (a.age                       is null,  '',   REGEXP_REPLACE(a.age, '.0', '')) as age,

   CASE
        WHEN a.gndr is null OR a.gndr NOT IN ('M','F') THEN ''
        ELSE a.gndr
   END as gndr,

   CASE
        WHEN d.natv_yn is null THEN ''
        WHEN d.natv_yn = 'Y' THEN 'L'
        WHEN d.natv_yn = 'N' THEN 'F'
        ELSE ''
   END as natv_yn,

   if (d.mrrg_yn                    is null,  '',   d.mrrg_yn)  as mrrg_yn,
   if (GG.sido                      is null,  '',   GG.sido)    as sido,
   if (GG.sigungu                   is null,  '',   GG.sigungu) as sigungu,
   if (GG.dong                      is null,  '',   GG.dong)    as dong,
   if (GG.bldng                     is null,  '',   GG.bldng)   as bldng

   FROM

   (

     SELECT DISTINCT

     mem_no,
     age,
     gndr,
     mem_ci

     FROM
     11st.tb_evs_base_m_mb_mem 
     WHERE 
     part_date = '${TODAY_JOB_DATE}'
     AND mem_ci IS NOT NULL AND mem_ci <> '' AND mem_ci not like '%\u0001%'
     AND mem_clf = '01' AND mem_typ_cd = '01' AND mem_stat_cd = '01'

   ) a

   INNER JOIN

   (

    SELECT DISTINCT

    mem_no,
    bgn_ent_dy    

    FROM
    11st.tb_evs_ods_m_mb_mem
    WHERE
    part_date = '${TODAY_JOB_DATE}'

   ) c
   
   ON a.mem_no = c.mem_no
  
   INNER JOIN

   (

    SELECT DISTINCT
   
    mbr_no,   
    thm_pchsr_grd_nm,
    natv_yn,
    mrrg_yn
 
    FROM
    11st.tb_evs_dw_m_mbr_info 
    WHERE
    p_yyyymmdd = '${TODAY_JOB_DATE}'

   ) d

   ON a.mem_no = d.mbr_no

   LEFT JOIN

   (
      
     SELECT DISTINCT
     
     mem_no,
     cmlt_pnt

     FROM
     11st.tb_evs_ods_f_mt_pnt
     WHERE
     part_date = '${TODAY_JOB_DATE}'
     AND
     pnt_kd_cd IS NOT NULL AND pnt_kd_cd = '07'     

   ) g

   ON a.mem_no = g.mem_no

   LEFT JOIN

   (
     SELECT DISTINCT

     mem_no,
     noti_shp_evt_bnft_inst_yn

     FROM
     11st.tb_evs_ods_m_mo_app_push_info
     WHERE
     part_date = '${TODAY_JOB_DATE}' 

   ) e

   ON a.mem_no = e.mem_no

   LEFT JOIN

   (
      SELECT DISTINCT
      DD.buy_mem_no_de,
      FF.sido_nm          as sido,
      FF.sigungu_nm       as sigungu,
      FF.ueupmyon_nm      as dong,
      FF.sigungu_build_nm as bldng
      from

      (
            select
            buy_mem_no_de,
            max(delvplace_seq) as delvplace_seq_max
            from
            11st.tb_evs_ods_f_tr_ord_prd
            where
            part_date = '${TODAY_JOB_DATE}' and SUBSTR(ord_no,1,8)>='${TODAY_JOB_DATE_730_AGO}' and buy_mem_no_de not in ('','-1','0')
            group by buy_mem_no_de
      )
      DD

      join
      (
         select
         delvplace_seq,
         rcvr_mail_no
         from
         11st.tb_evs_ods_f_tr_ord_clm_delvplace
         where
         part_date = '${TODAY_JOB_DATE}'
      )
      EE
      on
      DD.delvplace_seq_max = EE.delvplace_seq

      join
      (

         select
         CASE
              WHEN part_date = '${TODAY_JOB_DATE}' THEN area_no
              WHEN part_date = '20171029' THEN mail_no
         END as zcode,
         sido_nm,
         sigungu_nm,
         ueupmyon_nm,
         sigungu_build_nm
         from
         11st.tb_evs_ods_m_sy_road_addr
         where ((part_date = '${TODAY_JOB_DATE}' and length(area_no)=5)or(part_date = '20171029' and length(mail_no)=6))

      )
      FF
      on
      EE.rcvr_mail_no = FF.zcode

   )GG

   ON
   c.mem_no = GG.buy_mem_no_de


   ) aa


;" > demo_11st_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -f demo_11st_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='18');"

etl_stat 'DONE'

rm -f demo_*.hql

rm -f *.log

