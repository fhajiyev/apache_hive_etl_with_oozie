#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_ocb_tr\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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
LAST_JOB_MONTH_NOSLA=`date +%Y%m -d "$LAST_JOB_DAY day ago"`
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

DROP_DAYS=`expr 367 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE
ID_POOL=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE

run cp $DMP_PI_LOCAL_HOME/ocb_info.dat $DMP_PI_LOCAL_HOME/ocb_tr/
run cp $DMP_PI_LOCAL_HOME/ocb_coupon.dat $DMP_PI_LOCAL_HOME/ocb_tr/

etl_stat 'DOING'
print_bline
print_bstr "OCB TR = 5"


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='5')

select 


 AA.ci,
 BB.rcv_dt,
 '',
 CASE from_unixtime(unix_timestamp(BB.rcv_dt, 'yyyyMMdd'), 'u')
     WHEN 1 THEN 'mon'
     WHEN 2 THEN 'tue'
     WHEN 3 THEN 'wed'
     WHEN 4 THEN 'thu'
     WHEN 5 THEN 'fri'
     WHEN 6 THEN 'sat'
     ELSE 'sun'
 END,

 BB.action,
 BB.acode,
 if(DD.alcmpn_nm IS NULL, '', DD.alcmpn_nm),

 if(DD.lc IS NULL, '', DD.lc),
 if(DD.mc IS NULL, '', DD.mc),
 if(DD.sc IS NULL, '', DD.sc),

 if(DD.lm IS NULL, '', DD.lm),
 if(DD.mm IS NULL, '', DD.mm),
 if(DD.sm IS NULL, '', DD.sm),

 TRIM
 (
 CONCAT
  (
     CASE WHEN DD.lm IS NULL OR DD.lm IN (' ','') THEN ''
     ELSE DD.lm
     END,
     CASE WHEN DD.mm IS NULL OR DD.mm IN (' ','') THEN ''
     ELSE CONCAT('\||',DD.mm)
     END,
     CASE WHEN DD.sm IS NULL OR DD.sm IN (' ','') THEN ''
     ELSE CONCAT('\||',DD.sm)
     END
  )
 ),

 BB.mcode,
 if(CC.mcnt_nm IS NULL, '', CC.mcnt_nm),
 
 BB.pntval,

 BB.cpn_cd,
 if(EE.prd_nm IS NULL, '', EE.prd_nm),


   '','',
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
    ocb_id
    from
    dmp_pi.id_pool
    where part_date = '${TODAY_JOB_DATE}'

 ) AA

 JOIN

 (
   select

   mbr_id,
   rcv_dt,
   CASE
      WHEN slip_cd = '11' THEN '02'
      ELSE '01'
   END as action,

   CASE
      WHEN slip_cd = '11' THEN abs(avl_pnt)
      ELSE abs(occ_pnt)
   END as pntval,

   slip_cd,
   cpn_prd_qty,
   occ_pnt,
   avl_pnt,
   stlmt_alcmpn_cd as acode,
   cncl_slip_tp_cd,
   stlmt_mcnt_cd   as mcode,
   if(cpn_prd_cd is null or cpn_prd_cd = 'Y', '', cpn_prd_cd) as cpn_cd

   from
   ocb.mart_anal_sale_info
   where
   rcv_ym = '${LAST_JOB_MONTH_NOSLA}'
   and
   rcv_dt = '${LAST_JOB_DATE_NOSLA}'
   and
   cncl_slip_tp_cd not in ('1', '2', 'C')
   and
   slip_cd in ('01', '05', '15', '35', '37', '65', '11')
   


 ) BB

 ON
 AA.ocb_id = BB.mbr_id



 LEFT JOIN

 (
  
  select

  alcmpn_cd,
  mcnt_cd,
  mcnt_nm

  from

  ocb.mart_mcnt_mst

 ) CC

 ON 
 BB.mcode = CC.mcnt_cd

 LEFT JOIN

 (

  select

  a.alcmpn_cd,
  a.alcmpn_nm,

  if(loc_l.dtl_cd_nm is null or loc_l.dtl_cd_nm = '미입력', '', loc_l.dtl_cd_nm)          as lm,
  if(loc_m.dtl_cd_nm is null or loc_m.dtl_cd_nm = '미입력', '', loc_m.dtl_cd_nm)          as mm,
  if(loc_s.dtl_cd_nm is null or loc_s.dtl_cd_nm = '미입력', '', loc_s.dtl_cd_nm)          as sm,
  if(loc_l.dtl_cd    is null or loc_l.dtl_cd = 'Y',         '', loc_l.dtl_cd)          as lc,
  if(loc_m.dtl_cd    is null or loc_m.dtl_cd = 'Y',         '', loc_m.dtl_cd)          as mc,
  if(loc_s.dtl_cd    is null or loc_s.dtl_cd = 'Y',         '', loc_s.dtl_cd)          as sc

  from

  ocb.mart_alcmpn_mst a

  left join

  (

  select
  dtl_cd,
  dtl_cd_nm

  from
  ocb.dw_ocb_intg_cd
  where domn_id = 'OCB_BIZTP_LGRP_CD'

  ) loc_l

  ON SUBSTR(a.ocb_biztp_sgrp_cd, 1, 2) = loc_l.dtl_cd

  left join

  (

  select
  dtl_cd,
  dtl_cd_nm

  from
  ocb.dw_ocb_intg_cd
  where domn_id = 'OCB_BIZTP_MGRP_CD'

  ) loc_m

  ON SUBSTR(a.ocb_biztp_sgrp_cd, 1, 4) = loc_m.dtl_cd

  left join

  (

  select
  dtl_cd,
  dtl_cd_nm

  from
  ocb.dw_ocb_intg_cd
  where domn_id = 'OCB_BIZTP_SGRP_CD'

  ) loc_s

  ON a.ocb_biztp_sgrp_cd = loc_s.dtl_cd

 ) DD

 ON
 BB.acode = DD.alcmpn_cd


 LEFT JOIN
 
 ocb.dw_cpn_prd_mst EE

 ON 
 BB.cpn_cd = EE.cpn_prd_cd





;" > ocb_transact_${LAST_JOB_DATE_NOSLA}.hql


run /app/di/script/run_hivetl.sh -f ocb_transact_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='5');"

etl_stat 'DONE'

rm -f ocb_transact_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log


