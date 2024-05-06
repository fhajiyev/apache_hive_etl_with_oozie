

set hivevar:day2before;
set hivevar:day2before_hyp;
set hivevar:day16before_hyp;
set hivevar:day730before;
set hivevar:day367before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='16')


select

  t.ci,
  t.pay_dt,
  t.pay_time,
  t.weekday,
  t.action,
  '',
  '',
  '',
  '',
  t.disp_ctgr_ctgr1_nm,
  t.disp_ctgr_ctgr2_nm,
  t.disp_ctgr_ctrg3_nm,
  '',
  t.cat_nm,
  '',
  '',
  t.prd_no,
  t.prd_nm,
  t.amount,
  t.opt_name,
  '',
  t.stl_mns_clf_cd,
  t.dtls_com_nm,

  t.sido,

  CASE
     WHEN t.sido <> '' AND t.sigungu <> '' THEN CONCAT(t.sido, ' ', t.sigungu)
     ELSE ''
  END,

  CASE
     WHEN t.sido <> '' AND t.sigungu <> '' AND t.dong <> '' THEN CONCAT(t.sido, ' ', t.sigungu, ' ', t.dong)
     ELSE ''
  END,

  t.bldng,

  t.card_type,

  '','','',
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
  AA.ci
, BB.pay_dt
, BB.pay_time
, CASE from_unixtime(unix_timestamp(BB.pay_dt, 'yyyyMMdd'), 'u')
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
, if(CC.disp_ctgr_ctgr1_nm is null, '', TRIM(CC.disp_ctgr_ctgr1_nm)) as disp_ctgr_ctgr1_nm
, if(CC.disp_ctgr_ctgr2_nm is null, '', TRIM(CC.disp_ctgr_ctgr2_nm)) as disp_ctgr_ctgr2_nm
, if(CC.disp_ctgr_ctrg3_nm is null, '', TRIM(CC.disp_ctgr_ctrg3_nm)) as disp_ctgr_ctrg3_nm
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
, BB.prd_no
, BB.prd_nm


, if(BB.amount         is null, '', BB.amount        ) as amount
, if(HH.opt_name       is null, '', HH.opt_name      ) as opt_name
, if(LL.stl_mns_clf_cd is null, '', LL.stl_mns_clf_cd) as stl_mns_clf_cd
, if(KK.dtls_com_nm    is null, '', KK.dtls_com_nm   ) as dtls_com_nm
, if(KK.crd_typ        is null, '', KK.crd_typ       ) as card_type

, CASE
      WHEN GG.sido is null THEN ''

      WHEN GG.sido like '서울%'        THEN '서울'

      WHEN GG.sido like '인천%'        THEN '인천'
      WHEN GG.sido like '울산%'        THEN '울산'
      WHEN GG.sido like '부산%'        THEN '부산'
      WHEN GG.sido like '대전%'        THEN '대전'
      WHEN GG.sido like '대구%'        THEN '대구'
      WHEN GG.sido like '광주%'        THEN '광주'

      WHEN GG.sido like '세종%'        THEN '세종'
      WHEN GG.sido like '제주%'        THEN '제주'

      WHEN GG.sido like '강원%'        THEN '강원'
      WHEN GG.sido like '경기%'        THEN '경기'

      WHEN GG.sido = '전라남도'        THEN '전남'
      WHEN GG.sido = '전라북도'        THEN '전북'
      WHEN GG.sido = '충청남도'        THEN '충남'
      WHEN GG.sido = '충청북도'        THEN '충북'
      WHEN GG.sido = '경상남도'        THEN '경남'
      WHEN GG.sido = '경상북도'        THEN '경북'
  END as sido

, if (GG.sigungu                   is null,  '',   GG.sigungu) as sigungu
, if (GG.dong                      is null,  '',   GG.dong)    as dong
, if (GG.bldng                     is null,  '',   GG.bldng)   as bldng

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
select  A.pay_dt
      , substr(A.pay_dttm,12,2) as pay_time
      , A.pur_mbr_no
      , A.ord_no
      , A.prd_no
      , B.prd_nm
      , A.ord_amt as amount
      , 'buy' as action
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN A.disp_ctgr_no ELSE B.disp_ctgr_no_de  END as cat_id
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr1_no_de END as cat1_id
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr2_no_de END as cat2_id
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr3_no_de END as cat3_id
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr4_no_de END as cat4_id

from
ELEVENST_SKP.evs_skp_ods_f_sd_ord_detl_rslt A
join
ELEVENST_SKP.evs_skp_ods_m_pd_prd B
on A.prd_no = B.prd_no

where A.part_date = '${hivevar:day2before}'
  and B.part_date = '${hivevar:day2before}'
  and A.pay_dt = '${hivevar:day2before}'
  and A.pay_trns_amt - A.pay_seller_dscnt_amt_basic > 0
  and A.pur_mbr_no not in ('-1', '', '\n', '0')
) BB on AA.elev_id = BB.pur_mbr_no



left join
(
  select
  ord_no,
  slct_prd_opt_nm as opt_name
  from
  ELEVENST_SKP.evs_skp_ods_f_tr_slct_prd_opt
  where
  part_date = '${hivevar:day2before}'
)
HH
on
HH.ord_no = BB.ord_no



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

  select distinct
  ord_no,
  dtls_com_nm,
  crd_typ

  from

  (

  select
  ord_no,
  crden_cd,
  crd_typ

  from
  ELEVENST_SKP.EVS_SKP_ODS_F_TR_CRDT_STL
  where
  part_date = '${hivevar:day2before}'

  ) A

  inner join

  (

  select
  dtls_cd,
  dtls_com_nm

  from
  ELEVENST_SKP.EVS_SKP_ODS_OM_SY_CO_DETAIL
  where
  part_date = '${hivevar:day2before}'
  and grp_cd='TR223'

  ) B

  on
  A.crden_cd = B.dtls_cd


) KK
on
KK.ord_no = BB.ord_no


left join

(
  select distinct
  ord_no,
  stl_mns_clf_cd

  from
  ELEVENST_SKP.EVS_SKP_ODS_F_TR_ORD_STL_RFND
  where
  part_date = '${hivevar:day2before}'

) LL
on
LL.ord_no = BB.ord_no


left join

(
select distinct
DD.buy_mem_no_de,
EE.sido_nm          as sido,
EE.sigungu_nm       as sigungu,
EE.ueupmyon_val     as dong,
EE.build_nm as bldng

from


(

            select
            buy_mem_no_de,
            max(delvplace_seq) as delvplace_seq_max
            from
            ELEVENST_SKP.evs_skp_ods_f_tr_ord_prd
            where
            part_date = '${hivevar:day2before}' and (SUBSTR(ord_no,1,8) between '${hivevar:day730before}' and '${hivevar:day2before}') and buy_mem_no_de not in ('','-1','0')
            group by buy_mem_no_de

)
DD

join
(


      select
      delvplace_seq,
      sido_nm,
      sigungu_nm,
      ueupmyon_val,
      sigungu_build_nm as build_nm

      from

      (
         select
         delvplace_seq,
         funcs.SecuAesDecrypt(rcvr_mail_no) as rcvr_mail_no,
         rcvr_build_mng_no
         from
         ELEVENST_SKP.evs_skp_ods_f_tr_ord_clm_delvplace
         where
         part_date = '${hivevar:day2before}' and (SUBSTR(create_dt,1,10) between '${hivevar:day16before_hyp}' and '${hivevar:day2before_hyp}') and rcvr_build_mng_no <> ''
      ) tb11

      join
      (

         select
         area_no as zcode,
         sido_nm,
         sigungu_nm,
         hjdong_nm as ueupmyon_val,
         build_mng_no,
         sigungu_build_nm
         from
         ELEVENST_SKP.evs_skp_ods_m_sy_road_addr
         where (part_date = '${hivevar:day2before}' and length(area_no)=5)

      ) tb12

      on
      tb11.rcvr_mail_no = tb12.zcode

)
EE
on
DD.delvplace_seq_max = EE.delvplace_seq


)GG
on
BB.pur_mbr_no = GG.buy_mem_no_de

) t
;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day367before}00', data_source_id='16');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day367before}00', data_source_id='16');

