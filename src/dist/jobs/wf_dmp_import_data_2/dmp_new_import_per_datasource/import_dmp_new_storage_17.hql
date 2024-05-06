

set hivevar:day2before;
set hivevar:day2before_hyp;
set hivevar:day271before_hyp;
set hivevar:day367before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='17')

select
   AA.ci                          as uid
 , BB.create_dt                   as dateval
 , BB.create_time                 as time
 , CASE from_unixtime(unix_timestamp(BB.create_dt, 'yyyyMMdd'), 'u')
     WHEN 1 THEN 'mon'
     WHEN 2 THEN 'tue'
     WHEN 3 THEN 'wed'
     WHEN 4 THEN 'thu'
     WHEN 5 THEN 'fri'
     WHEN 6 THEN 'sat'
     ELSE 'sun'
   END as weekday
 , BB.action
 , ''
 , ''
 , ''
 , ''
 , CASE WHEN CC.disp_ctgr_ctgr1_nm IS NULL THEN '' ELSE TRIM(CC.disp_ctgr_ctgr1_nm) END
 , CASE WHEN CC.disp_ctgr_ctgr2_nm IS NULL THEN '' ELSE TRIM(CC.disp_ctgr_ctgr2_nm) END
 , CASE WHEN CC.disp_ctgr_ctrg3_nm IS NULL THEN '' ELSE TRIM(CC.disp_ctgr_ctrg3_nm) END
 , ''
 , TRIM
 (
 CONCAT
 (
     CASE WHEN CC.disp_ctgr_ctgr1_nm IS NULL OR TRIM(CC.disp_ctgr_ctgr1_nm) IN ('') THEN ''
     ELSE TRIM(CC.disp_ctgr_ctgr1_nm)
     END,
     CASE WHEN CC.disp_ctgr_ctgr2_nm IS NULL OR TRIM(CC.disp_ctgr_ctgr2_nm) IN ('') THEN ''
     ELSE CONCAT('\||',TRIM(CC.disp_ctgr_ctgr2_nm))
     END,
     CASE WHEN CC.disp_ctgr_ctrg3_nm IS NULL OR TRIM(CC.disp_ctgr_ctrg3_nm) IN ('') THEN ''
     ELSE CONCAT('\||',TRIM(CC.disp_ctgr_ctrg3_nm))
     END
 )
 ) AS cat_nm
 , ''
 , ''
 , BB.prd_no
 , BB.prd_nm


 , IF(DD.sel_prc  is null, '', DD.sel_prc )
 , IF(HH.opt_name is null, '', HH.opt_name)

 , '',   
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









 from
 (
    select
    ci,
    elev_id
    from
    dmp_pi.id_pool
    where part_date = '${hivevar:day2before}'

 ) AA
 join
 (
 select  regexp_replace(substr(A.create_dt,1,10),'-','') as create_dt
       , substr(A.create_dt,12,2) as create_time
       , A.create_no
       , A.bckt_seq 
       , A.prd_no
       , B.prd_nm
       , 'keep' as action
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN A.disp_ctgr_no ELSE B.disp_ctgr_no_de  END as cat_id
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr1_no_de END as cat1_id
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr2_no_de END as cat2_id
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr3_no_de END as cat3_id
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr4_no_de END as cat4_id

 from
 ELEVENST_SKP.evs_skp_ods_f_tr_bckt A
 join
 ELEVENST_SKP.evs_skp_ods_m_pd_prd B
 on A.prd_no = B.prd_no
 
 where A.part_date = '${hivevar:day2before}'
   and B.part_date = '${hivevar:day2before}'
   and regexp_replace(substr(A.create_dt,1,10),'-','') = '${hivevar:day2before}'
   and A.create_no not in ('-1', '', '\n', '0')
 ) BB on AA.elev_id = BB.create_no

 
 left join
 (
    select
    bckt_seq,
    slct_prd_opt_nm as opt_name
    from
    ELEVENST_SKP.evs_skp_ods_f_tr_slct_prd_opt
    where
    part_date = '${hivevar:day2before}'
 )
 HH
 on
 HH.bckt_seq = BB.bckt_seq

 left join
 (
    select
    disp_ctgr_no,
    disp_ctgr_ctgr1_no,
    disp_ctgr_ctgr2_no,
    disp_ctgr_ctrg3_no,
    disp_ctgr_ctgr1_nm,
    disp_ctgr_ctgr2_nm,
    disp_ctgr_ctrg3_nm
    from
    ELEVENST_SKP.evs_skp_ods_m_dd_disp_ctgr
    where
    part_date = '${hivevar:day2before}'
 )
 CC
 on
 BB.cat_id = CC.disp_ctgr_no

 left join
 (
    select
    prd_no,
    sel_prc

    from
    ELEVENST_SKP.evs_skp_ods_m_pd_prd_prc
    where
    part_date = '${hivevar:day2before}'
    and
    substr(create_dt,1,10) >= '${hivevar:day271before_hyp}' and substr(create_dt,1,10) <= '${hivevar:day2before_hyp}'

 ) DD on BB.prd_no = DD.prd_no

;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day367before}00', data_source_id='17');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day367before}00', data_source_id='17');

