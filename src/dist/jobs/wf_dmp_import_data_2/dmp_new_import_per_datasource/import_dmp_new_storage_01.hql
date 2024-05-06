



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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='1')


SELECT
   b.uid,
   b.age,
   b.sex_cd,

   if(c.int_cross_agr_yn IS NULL, '', c.int_cross_agr_yn),
   if(d.telco_nm         IS NULL, '',         d.telco_nm),
   if(d.mfact_nm         IS NULL, '',         d.mfact_nm),
   if(d.last_dur         IS NULL, '',         d.last_dur),

   if(d.ocb_card_avg_inco_prsm_amt IS NULL, '',         CAST(d.ocb_card_avg_inco_prsm_amt AS DECIMAL)),
   CASE
      WHEN d.ocb_card_avg_inco_seg_cd IS NULL THEN ''
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P10'  THEN '1'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P20'  THEN '2'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P30'  THEN '3'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P40'  THEN '4'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P50'  THEN '5'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P60'  THEN '6'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P70'  THEN '7'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P80'  THEN '8'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P90'  THEN '9'
      WHEN d.ocb_card_avg_inco_seg_cd like '%_P100' THEN '10'
      ELSE ''
   END,
   if(d.evs_avg_consum_m12         IS NULL, '',         CAST(d.evs_avg_consum_m12 AS DECIMAL)),
   CASE
      WHEN d.evs_avg_consum_m12_seg_cd IS NULL THEN ''
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P10'  THEN '1'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P20'  THEN '2'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P30'  THEN '3'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P40'  THEN '4'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P50'  THEN '5'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P60'  THEN '6'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P70'  THEN '7'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P80'  THEN '8'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P90'  THEN '9'
      WHEN d.evs_avg_consum_m12_seg_cd like '%_P100' THEN '10'
      ELSE ''
   END,
   '','','','','','','','','','',
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

   SELECT
   t2.uid,
   t2.age,
   t2.sex_cd

   FROM
   (

   SELECT
   t1.uid           as uid,
   t1.age,
   t1.sex_cd,
   ROW_NUMBER() OVER( PARTITION BY t1.UID ORDER BY t1.DSTYPE ) AS RN
   FROM

   (

      SELECT
      b1.ci as uid,
      a1.age,
      a1.sex_cd,
      2 as DSTYPE
      FROM

      (

       SELECT
       mbr_id,
       age,
       sex_cd
       FROM

       svc_custpfdb.pdb_ocb_mbr
       WHERE
       mbr_id IS NOT NULL AND mbr_id <> ''
       AND ((age IS NOT NULL AND age <> '') AND (sex_cd IS NOT NULL AND sex_cd IN ('M','F'))) -- valid age and valid gender present

      ) a1

      inner join

      (

       SELECT
       ci,
       rep_ocb_mbr_id
       FROM
       svc_custpfdb.PDB_CI_MBR
       WHERE
       ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'
       AND
       rep_ocb_mbr_id IS NOT NULL AND rep_ocb_mbr_id <> ''

      ) b1

      ON a1.mbr_id = b1.rep_ocb_mbr_id

      UNION ALL

      SELECT
      b2.ci as uid,
      a2.age,
      a2.sex_cd,
      1 as DSTYPE
      FROM

      (

       SELECT
       mbr_id,
       age,
       sex_cd
       FROM

       svc_custpfdb.pdb_syr_mbr
       WHERE
       mbr_id IS NOT NULL AND mbr_id <> ''
       AND ((age IS NOT NULL AND age <> '') AND (sex_cd IS NOT NULL AND sex_cd IN ('M','F'))) -- valid age and valid gender present

      ) a2

      inner join

      (

       SELECT
       ci,
       rep_syr_mbr_id
       FROM
       svc_custpfdb.PDB_CI_MBR
       WHERE
       ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'
       AND
       rep_syr_mbr_id IS NOT NULL AND rep_syr_mbr_id <> ''

      ) b2

      ON a2.mbr_id = b2.rep_syr_mbr_id

      UNION ALL

      SELECT
      b3.ci as uid,
      a3.age,
      a3.sex_cd,
      3 as DSTYPE
      FROM

      (

       SELECT
       mbr_id,
       age,
       sex_cd
       FROM

       svc_custpfdb.pdb_evs_mbr
       WHERE
       mbr_id IS NOT NULL AND mbr_id <> ''
       AND ((age IS NOT NULL AND age <> '') AND (sex_cd IS NOT NULL AND sex_cd IN ('M','F'))) -- valid age and valid gender present

      ) a3

      inner join

      (

       SELECT
       ci,
       rep_evs_mbr_id
       FROM
       svc_custpfdb.PDB_CI_MBR
       WHERE
       ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'
       AND
       rep_evs_mbr_id IS NOT NULL AND rep_evs_mbr_id <> ''

      ) b3

      ON a3.mbr_id = b3.rep_evs_mbr_id


   ) t1

   ) t2
   WHERE t2.RN = 1

   ) b

   LEFT JOIN

   svc_custpfdb.pdb_ci_mbr c

   ON
   b.uid = c.ci

   LEFT JOIN

   svc_custpfdb.pdb_mp_com_f_info d

   ON
   b.uid = d.ci

;

