




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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='18')


SELECT DISTINCT

   a1.ci,
   'Y',
   if(b.evs_join_dt         is null, '', b.evs_join_dt),
   if(a.push_agr_yn         is null, '', a.push_agr_yn),
   if(a.mem_typ_cd          is null, '', a.mem_typ_cd),
   if(b.evs_mbr_buy_grd_cd  is null, '', b.evs_mbr_buy_grd_cd),
   if(a.age                 is null, '', a.age),
   if(a.sex_cd              is null, '', a.sex_cd),
   if(b.evs_frgn_yn         is null, '', b.evs_frgn_yn),
   if(b.evs_marry_yn        is null, '', b.evs_marry_yn),
   if(a.mem_clf             is null, '', a.mem_clf),
   if(a.svc_mkt_agr_yn      is null, '', a.svc_mkt_agr_yn),
   if(b.evs_app_acvt_per_cd is null, '', b.evs_app_acvt_per_cd),
   if(b.evs_buy_fcst_scr    is null, '', cast(b.evs_buy_fcst_scr as decimal)),
   if(b.evs_plus_fcst_scr   is null, '', cast(b.evs_plus_fcst_scr as decimal)),
   if(b.evs_ltv             is null, '', cast(b.evs_ltv as decimal)),


   '','','','','',
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
    mem_typ_cd,
    mem_clf

    FROM

    svc_custpfdb.PDB_EVS_MBR

    WHERE
    mbr_id IS NOT NULL AND mbr_id <> ''

   ) a

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

         ) a1

         ON a.mbr_id = a1.rep_evs_mbr_id

   LEFT JOIN

   (

     SELECT

     ci,
     evs_join_dt,
     evs_mbr_buy_grd_cd,
     CASE
         WHEN evs_frgn_yn IS NULL THEN ''
         WHEN evs_frgn_yn = 'N'    THEN 'L'
         WHEN evs_frgn_yn = 'Y'    THEN 'F'
         ELSE ''
     END as evs_frgn_yn,
     evs_app_acvt_per_cd,
     ROUND(evs_buy_fcst_scr, 4) as evs_buy_fcst_scr,
     ROUND(evs_plus_fcst_scr, 4) as evs_plus_fcst_scr,
     ROUND(evs_ltv, 4) as evs_ltv,
     evs_marry_yn


     FROM
     svc_custpfdb.pdb_mp_com_f_info
     WHERE
     ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'

   ) b

   ON a1.ci = b.ci

;


