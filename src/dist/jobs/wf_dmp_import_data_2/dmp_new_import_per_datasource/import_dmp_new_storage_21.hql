

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


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='21')

select distinct
       
       mall.uid,
       mall.card_type,
       mall.issue_date,

       CASE WHEN mall.card_type = '01' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '01' THEN mall.card_name ELSE '' END,

       CASE WHEN mall.card_type = '02' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '02' THEN mall.card_name ELSE '' END,

       CASE WHEN mall.card_type = '03' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '03' THEN mall.card_name ELSE '' END,

       CASE WHEN mall.card_type = '04' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '04' THEN mall.card_name ELSE '' END,

       CASE WHEN mall.card_type = '05' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '05' THEN mall.card_name ELSE '' END,


'','','','','','','','',
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

     ma.ci_no           as uid,
     ma.card_gubun      as card_type,
     ma.card_dtl_grp_cd as card_code,
     ma.dtl_cd_nm       as card_name, 
     ma.card_iss_dt     as issue_date

from (
select  d.ci_no, a.mbr_id, a.card_sts_cd, a.card_dtl_grp_cd, a.iss_alcmpn_cd, a.card_iss_dt,
         (case when a.crdt_card_yn = '1' then '02'
               when a.chkcrd_yn    = '1' then '03' else '01' end)  card_gubun,
        c.dtl_cd_nm ,b.alcmpn_nm
  from ( select mbr_id, card_sts_cd, card_dtl_grp_cd, iss_alcmpn_cd, card_iss_dt, crdt_card_yn, chkcrd_yn
           from ocb.mart_card_mst
          where card_sts_cd = 'A'  ) a
  join ocb.mart_alcmpn_mst  b
    on (a.iss_alcmpn_cd = b.alcmpn_cd)
  join (select dtl_cd , dtl_cd_nm
          from ocb.dw_ocb_intg_cd
         where domn_id  = 'CARD_DTL_GRP_CD') c
     on (a.card_dtl_grp_cd = c.dtl_cd)
   join  ocb.mart_mbr_mst d
     on (a.mbr_id = d.mbr_id)
   where d.ci_no is not null and d.ci_no <> '' and d.ci_no not like '%\u0001%'
) ma
union all
select

     mb.ci        as uid, 
     mb.card_gubn as card_type, 
     mb.card_id   as card_code, 
     mb.card_name as card_name, 
     mb.issue_dt  as issue_date

from (
select
  aa.ci, aa.member_id, bb.card_id, cc.card_name, substr(bb.issue_dt,1,8) as issue_dt, substr(bb.issue_dt,9,2) as issue_time,
  case
    when cc.card_type_cd = '28' then '05'
    else '04'
  end as card_gubn
  from smartwallet.mt3_member aa
  join smartwallet.mt3_mem_card bb on aa.member_id = bb.member_id
  join smartwallet.mt3_card cc on bb.card_id = cc.card_id
  where bb.status_cd in ('3','6') and aa.ci is not null and aa.ci <> ''

) mb

) mall;


