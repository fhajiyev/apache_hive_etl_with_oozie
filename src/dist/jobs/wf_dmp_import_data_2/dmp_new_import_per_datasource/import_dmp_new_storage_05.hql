

set hivevar:day2before;
set hivevar:day2before_yyyymm;
set hivevar:day367before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='5')

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
   '','','','','','','','','','',
   '${hivevar:day2before}00',
   '${hivevar:day2before}00'


 from
 (
    select
    ci,
    ocb_id
    from
    dmp_pi.id_pool
    where part_date = '${hivevar:day2before}'

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
   rcv_ym = '${hivevar:day2before_yyyymm}'
   and
   rcv_dt = '${hivevar:day2before}'
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





;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day367before}00', data_source_id='5');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day367before}00', data_source_id='5');




