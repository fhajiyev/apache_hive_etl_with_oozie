#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_etl/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_zone\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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
ID_POOL=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE


DROP_DAYS=`expr 92 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

run cp $DMP_PI_LOCAL_HOME/m2_merch_zone.dat .
run cp $DMP_PI_LOCAL_HOME/m2zone.dat .
run cp $DMP_PI_LOCAL_HOME/mid_addr.dat .
run cp $DMP_PI_LOCAL_HOME/ocb_code.dat .

OUT_BLE_TR=$WORKDIR/temp_ble_tr


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='8')

select distinct

t.ci,
t.dateval,
t.timeval,
t.weekday,
t.type_ch,
t.loc_code,
t.loc_name,

t.lm,

CASE
   WHEN t.lm <> '' AND t.mm <> '' THEN CONCAT(t.lm, ' ', t.mm)
   ELSE ''
END,

CASE
   WHEN t.lm <> '' AND t.mm <> '' AND t.sm <> '' THEN CONCAT(t.lm, ' ', t.mm, ' ', t.sm)
   ELSE ''
END,


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


from

(




select



AA.ci,

CC.dateval,
CC.timeval,
CASE from_unixtime(unix_timestamp(CC.dateval, 'yyyyMMdd'), 'u')
     WHEN 1 THEN 'mon'
     WHEN 2 THEN 'tue'
     WHEN 3 THEN 'wed'
     WHEN 4 THEN 'thu'
     WHEN 5 THEN 'fri'
     WHEN 6 THEN 'sat'
     ELSE 'sun'
END as weekday,

CC.type_ch,
CASE WHEN CC.type_ch = '01' THEN CC.zone_id   ELSE CC.mid_id                                END as loc_code,
CASE WHEN CC.type_ch = '01' THEN CC.zone_name ELSE if(DD.mid_name is null, '', DD.mid_name) END as loc_name,

if(DD.lm is null, '', DD.lm) as lm,
if(DD.mm is null, '', DD.mm) as mm,
if(DD.sm is null, '', DD.sm) as sm


from
(
    select
    ci,
    mdn
    from
    dmp_pi.id_pool
    where part_date = '${TODAY_JOB_DATE}'

) AA

JOIN

(


   SELECT


   BB.dateval,
   BB.timeval,
 
   CASE 
      WHEN BB.type_ch = '01' THEN m2zone2mid.maxmid
      ELSE BB.mid
   END as mid_id,
  
   BB.type_ch,
   BB.mdn,
   if(m2zone.zone_id is null, '', m2zone.zone_id) as zone_id,  
   if(m2zone.name    is null, '', m2zone.name   ) as zone_name

   FROM 

   (
      select
     
      substr(a.part_hour, 1,8) as dateval,
      substr(a.part_hour, 9,2) as timeval,
      a.mid,
      b.mdn,

      CASE WHEN a.log_id = 'PXG0101' THEN '01' ELSE '02' END as type_ch,
  

      CASE WHEN a.log_id = 'PXG0101' THEN if(get_json_object(a.body, '$.tech_id') is null, '', get_json_object(a.body, '$.tech_id')) ELSE '' END as tech_id



      from
      proximity.log_server_newtech a
      join
      istore.prod_ble_blekey_map b
      on
      a.key = b.ble_key 

      where
      a.part_hour between '${LAST_JOB_DATE_NOSLA}00' and '${LAST_JOB_DATE_NOSLA}24'
      and
      a.key NOT IN ('', '\n')
      and
      a.log_id IN ('PXG0101', 'PXB0101', 'PXW0101')


   ) BB


  left join

   (

      select

      tech_id,
      zone_id,
      name

      from

      imc.dbm_imc_m2_zone
      where
      part_date = '${LAST_JOB_DATE_NOSLA}'
      and
      tech_id IS NOT NULL and tech_id <> ''


   ) m2zone

   on
   BB.tech_id = m2zone.tech_id


  left join

   (

      select
  
      zone_id,
      max(mid) as maxmid

      from
      imc.dbm_imc_m2_merch_zone
      where
      part_date = '${LAST_JOB_DATE_NOSLA}'

      group by zone_id

   ) m2zone2mid

   on
   m2zone.zone_id = m2zone2mid.zone_id



) CC

ON
AA.mdn = CC.mdn

left join
(

   SELECT   
  
   mid_addr.mid as mid_id,
   mid_addr.exp_name as mid_name,
   if(loc_l.dtl_cd_nm IS NULL, '', loc_l.dtl_cd_nm) as lm,
   if(loc_m.dtl_cd_nm IS NULL, '', loc_m.dtl_cd_nm) as mm,
   CASE
      WHEN loc_s1.dtl_cd_nm IS NOT NULL AND loc_s1.dtl_cd_nm <> '' THEN loc_s1.dtl_cd_nm
      WHEN loc_s2.dtl_cd_nm IS NOT NULL AND loc_s2.dtl_cd_nm <> '' THEN loc_s2.dtl_cd_nm
      WHEN loc_s3.dtl_cd_nm IS NOT NULL AND loc_s3.dtl_cd_nm <> '' THEN loc_s3.dtl_cd_nm
      WHEN loc_s4.dtl_cd_nm IS NOT NULL AND loc_s4.dtl_cd_nm <> '' THEN loc_s4.dtl_cd_nm
      WHEN loc_s5.dtl_cd_nm IS NOT NULL AND loc_s5.dtl_cd_nm <> '' THEN loc_s5.dtl_cd_nm
      ELSE ''
   END as sm

   FROM
   (
  
     SELECT

     mid,
     exp_name,
     address_code

     from
     imc.dbm_imc_imc_store_basic
     where
     part_date = '${LAST_JOB_DATE_NOSLA}'

   ) mid_addr


   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_LGRP_CD'

   ) loc_l

   ON SUBSTR(mid_addr.address_code, 1, 2) = loc_l.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_MGRP_CD'

   ) loc_m

   ON SUBSTR(mid_addr.address_code, 1, 5) = loc_m.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s1

   ON (
       CONCAT(mid_addr.address_code, '00')                                              = loc_s1.dtl_cd
      )

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s2

   ON (
       mid_addr.address_code                                                            = loc_s2.dtl_cd 
      )

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s3

   ON (
       CONCAT(SUBSTR(mid_addr.address_code, 1, length(mid_addr.address_code)-1), '0')   = loc_s3.dtl_cd
      )

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s4

   ON (
       CONCAT(SUBSTR(mid_addr.address_code, 1, length(mid_addr.address_code)-1), '000') = loc_s4.dtl_cd
      )

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s5

   ON (
       CONCAT(SUBSTR(mid_addr.address_code, 1, length(mid_addr.address_code)-3), '000') = loc_s5.dtl_cd
      )



)DD
ON
CC.mid_id = DD.mid_id

  where
  (CC.type_ch = '01' and CC.zone_id is not null and CC.zone_id <> '')
  or
  (CC.type_ch = '02' and CC.mid_id  is not null and CC.mid_id  <> '')


)t
;" > prox_${LAST_JOB_DATE_NOSLA}.hql


/app/di/qcshell/bin/qcshell -b eda-hive2 -f prox_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='8');"

etl_stat 'DONE'

rm -f prox_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log


