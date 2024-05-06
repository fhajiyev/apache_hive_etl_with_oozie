
set hivevar:day2before;
set hivevar:day5before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='50')

   SELECT

   AA.ci,
   BB.REQUEST_DT,
   BB.REQUEST_TIME,
   BB.WEEKDAY,
   BB.evm_trd_knd_cd,
   BB.mktng_id,
   BB.evnt_id,
   BB.ctrc_id,
   BB.alcmpn_cd,
   BB.alcmpn_nm,
   BB.trd_mcnt_cd,
   BB.trd_mcnt_nm,
   BB.iss_dutum_poc_cd_nm,
   BB.pntval,
   BB.iss_dutum_poc_cd,

   '','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '${hivevar:day2before}00',
   '${hivevar:day2before}00'

    FROM

 (
    select
    ci,
    ocb_id
    from
    dmp_pi.id_pool
    where part_date = '${hivevar:day2before}'

 ) AA

 INNER JOIN

    (

      SELECT

      mbr_id,
      trd_dt as REQUEST_DT,
      SUBSTR(trd_time,1,2) as REQUEST_TIME,
      CASE from_unixtime(unix_timestamp(trd_dt, 'yyyyMMdd'), 'u')
          WHEN 1 THEN 'mon'
          WHEN 2 THEN 'tue'
          WHEN 3 THEN 'wed'
          WHEN 4 THEN 'thu'
          WHEN 5 THEN 'fri'
          WHEN 6 THEN 'sat'
          ELSE 'sun'
      END as weekday,
      evm_trd_knd_cd,
      mktng_id,
      evnt_id,
      ctrc_id,
      alcmpn_cd,
      alcmpn_nm,
      if(trd_mcnt_cd = 'Y', '', trd_mcnt_cd) as trd_mcnt_cd,
      trd_mcnt_nm,
      iss_dutum_poc_cd_nm,

      if(evm_trd_knd_cd IN ('SC01','UC01'), (-1)*incdec_pnt, incdec_pnt) as pntval,
      iss_dutum_poc_cd

      FROM
      ocb.mart_anal_evm_sale_info

      WHERE
      trd_dt = '${hivevar:day2before}'

    ) BB

       ON
       AA.ocb_id = BB.mbr_id;



