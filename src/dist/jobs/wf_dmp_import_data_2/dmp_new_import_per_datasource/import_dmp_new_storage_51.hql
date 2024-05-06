




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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='51')


SELECT DISTINCT

   a1.ci,

   if(c.sel_11         IS NULL OR c.sel_11='', '',         CAST(c.sel_11 AS DECIMAL)),
   CASE
         WHEN c.sel_12 IS NULL THEN ''
         WHEN c.sel_12 like '%_P10'  THEN '1'
         WHEN c.sel_12 like '%_P20'  THEN '2'
         WHEN c.sel_12 like '%_P30'  THEN '3'
         WHEN c.sel_12 like '%_P40'  THEN '4'
         WHEN c.sel_12 like '%_P50'  THEN '5'
         WHEN c.sel_12 like '%_P60'  THEN '6'
         WHEN c.sel_12 like '%_P70'  THEN '7'
         WHEN c.sel_12 like '%_P80'  THEN '8'
         WHEN c.sel_12 like '%_P90'  THEN '9'
         WHEN c.sel_12 like '%_P100' THEN '10'
         ELSE ''
   END,

   if(c.sel_21         IS NULL OR c.sel_21='', '',         CAST(c.sel_21 AS DECIMAL)),
   CASE
         WHEN c.sel_22 IS NULL THEN ''
         WHEN c.sel_22 like '%_P10'  THEN '1'
         WHEN c.sel_22 like '%_P20'  THEN '2'
         WHEN c.sel_22 like '%_P30'  THEN '3'
         WHEN c.sel_22 like '%_P40'  THEN '4'
         WHEN c.sel_22 like '%_P50'  THEN '5'
         WHEN c.sel_22 like '%_P60'  THEN '6'
         WHEN c.sel_22 like '%_P70'  THEN '7'
         WHEN c.sel_22 like '%_P80'  THEN '8'
         WHEN c.sel_22 like '%_P90'  THEN '9'
         WHEN c.sel_22 like '%_P100' THEN '10'
         ELSE ''
   END,

   if(c.sel_31         IS NULL OR c.sel_31='', '',         CAST(c.sel_31 AS DECIMAL)),
   CASE
         WHEN c.sel_32 IS NULL THEN ''
         WHEN c.sel_32 like '%_P10'  THEN '1'
         WHEN c.sel_32 like '%_P20'  THEN '2'
         WHEN c.sel_32 like '%_P30'  THEN '3'
         WHEN c.sel_32 like '%_P40'  THEN '4'
         WHEN c.sel_32 like '%_P50'  THEN '5'
         WHEN c.sel_32 like '%_P60'  THEN '6'
         WHEN c.sel_32 like '%_P70'  THEN '7'
         WHEN c.sel_32 like '%_P80'  THEN '8'
         WHEN c.sel_32 like '%_P90'  THEN '9'
         WHEN c.sel_32 like '%_P100' THEN '10'
         ELSE ''
   END,

   if(c.sel_41         IS NULL OR c.sel_41='', '',         CAST(c.sel_41 AS DECIMAL)),
   CASE
         WHEN c.sel_42 IS NULL THEN ''
         WHEN c.sel_42 like '%_P10'  THEN '1'
         WHEN c.sel_42 like '%_P20'  THEN '2'
         WHEN c.sel_42 like '%_P30'  THEN '3'
         WHEN c.sel_42 like '%_P40'  THEN '4'
         WHEN c.sel_42 like '%_P50'  THEN '5'
         WHEN c.sel_42 like '%_P60'  THEN '6'
         WHEN c.sel_42 like '%_P70'  THEN '7'
         WHEN c.sel_42 like '%_P80'  THEN '8'
         WHEN c.sel_42 like '%_P90'  THEN '9'
         WHEN c.sel_42 like '%_P100' THEN '10'
         ELSE ''
   END,


   '','',
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

    mbr_id,
    sex_cd,
    age,
    svc_mkt_agr_yn,
    push_agr_yn,
    mbr_fg_cd

    FROM

    svc_custpfdb.PDB_OCB_MBR

    WHERE
    mbr_id IS NOT NULL AND mbr_id <> ''

   ) a

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

         ) a1

         ON a.mbr_id = a1.rep_ocb_mbr_id

   LEFT JOIN

   (

      SELECT

      ci,

      CASE WHEN biztp_fg_nm IS NOT NULL AND TRIM(biztp_fg_nm) = 'MART' THEN mth12_avg_amt ELSE '' END   as sel_11,
      CASE WHEN biztp_fg_nm IS NOT NULL AND TRIM(biztp_fg_nm) = 'MART' THEN avg_amt_seg_cd ELSE '' END  as sel_12,

      CASE WHEN biztp_fg_nm IS NOT NULL AND TRIM(biztp_fg_nm) = 'SUPER' THEN mth12_avg_amt ELSE '' END  as sel_21,
      CASE WHEN biztp_fg_nm IS NOT NULL AND TRIM(biztp_fg_nm) = 'SUPER' THEN avg_amt_seg_cd ELSE '' END as sel_22,

      CASE WHEN biztp_fg_nm IS NOT NULL AND TRIM(biztp_fg_nm) = 'CONV' THEN mth12_avg_amt ELSE '' END   as sel_31,
      CASE WHEN biztp_fg_nm IS NOT NULL AND TRIM(biztp_fg_nm) = 'CONV' THEN avg_amt_seg_cd ELSE '' END  as sel_32,

      CASE WHEN biztp_fg_nm IS NOT NULL AND TRIM(biztp_fg_nm) = 'REFL' THEN mth12_avg_amt ELSE '' END   as sel_41,
      CASE WHEN biztp_fg_nm IS NOT NULL AND TRIM(biztp_fg_nm) = 'REFL' THEN avg_amt_seg_cd ELSE '' END  as sel_42

      FROM
      svc_custpfdb.pdb_mp_com_h_biztp_cust_sale
      WHERE
      ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'

   ) c

   ON a1.ci = c.ci




;


