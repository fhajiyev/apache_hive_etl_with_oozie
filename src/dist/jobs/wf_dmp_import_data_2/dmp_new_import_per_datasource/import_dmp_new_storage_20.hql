




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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='20')


SELECT DISTINCT

   a1.ci,
   'Y',
   if(b.syr_join_dt  is null, '', b.syr_join_dt),
   if(a.push_agr_yn  is null, '', a.push_agr_yn),
   if(a.age          is null, '', a.age),
   if(a.sex_cd       is null, '', a.sex_cd),
   if(b.syr_frgn_yn  is null, '', b.syr_frgn_yn),
   if(b.syr_marry_yn is null, '', b.syr_marry_yn),
   b.syr_home_sido_nm,
   b.syr_home_sigungu_nm,
   b.syr_home_upmyundong_nm,
   '',
   '',
   if(a.svc_mkt_agr_yn            is null, '', a.svc_mkt_agr_yn),
   if(b.syr_app_acvt_per_cd       is null, '', b.syr_app_acvt_per_cd),
   if(b.syr_cpn_last_ract_dt      is null, '', b.syr_cpn_last_ract_dt),
   if(b.syr_cpn_ract_day_cnt      is null, '', b.syr_cpn_ract_day_cnt),
   if(b.syr_cpn_acvt_per_cd       is null, '', b.syr_cpn_acvt_per_cd),
   if(b.syr_push_ract_clk_rt      is null, '', cast(b.syr_push_ract_clk_rt as decimal)),
   if(b.syr_push_ract_acvt_per_cd is null, '', b.syr_push_ract_acvt_per_cd),

   if(c.seg_cd is null, '', c.seg_cd),

   if(d.scr        is null, '', d.scr),
   if(d.cls_100_gr is null, '', d.cls_100_gr),

   CASE WHEN a.token_valid_yn IS NULL OR a.token_valid_yn = '' THEN 'UNKNOWN' ELSE a.token_valid_yn END,

   if(a.birth_md is null, '', a.birth_md),

   '','','','','','',
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
    push_agr_yn,
    sex_cd,
    age,
    svc_mkt_agr_yn,
    token_valid_yn,
    birth_md

    FROM

    svc_custpfdb.PDB_SYR_MBR

    WHERE
    mbr_id IS NOT NULL AND mbr_id <> ''

   ) a

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

         ) a1

         ON a.mbr_id = a1.rep_syr_mbr_id

   LEFT JOIN

   (

     SELECT

     ci,
     syr_join_dt,
     syr_marry_yn,
     syr_app_acvt_per_cd,
     syr_cpn_last_ract_dt,
     syr_cpn_ract_day_cnt,
     syr_cpn_acvt_per_cd,
     syr_push_ract_clk_rt,
     syr_push_ract_acvt_per_cd,

     CASE
         WHEN syr_frgn_yn IS NULL THEN ''
         WHEN syr_frgn_yn = 'N'    THEN 'L'
         WHEN syr_frgn_yn = 'Y'    THEN 'F'
         ELSE ''
     END as syr_frgn_yn,

   CASE
         WHEN syr_home_sido_nm is null THEN ''

         WHEN syr_home_sido_nm like '서울%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'서울특별시','서울')

         WHEN syr_home_sido_nm like '인천%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'인천광역시','인천')
         WHEN syr_home_sido_nm like '울산%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'울산광역시','울산')
         WHEN syr_home_sido_nm like '부산%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'부산광역시','부산')
         WHEN syr_home_sido_nm like '대전%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'대전광역시','대전')
         WHEN syr_home_sido_nm like '대구%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'대구광역시','대구')
         WHEN syr_home_sido_nm like '광주%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'광주광역시','광주')

         WHEN syr_home_sido_nm like '세종%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'세종특별자치시','세종')
         WHEN syr_home_sido_nm like '제주%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'제주특별자치도','제주')

         WHEN syr_home_sido_nm like '강원%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'강원도','강원')
         WHEN syr_home_sido_nm like '경기%'        THEN REGEXP_REPLACE(syr_home_sido_nm,'경기도','경기')

         WHEN syr_home_sido_nm = '전라남도'        THEN REGEXP_REPLACE(syr_home_sido_nm,'전라남도','전남')
         WHEN syr_home_sido_nm = '전라북도'        THEN REGEXP_REPLACE(syr_home_sido_nm,'전라북도','전북')
         WHEN syr_home_sido_nm = '충청남도'        THEN REGEXP_REPLACE(syr_home_sido_nm,'충청남도','충남')
         WHEN syr_home_sido_nm = '충청북도'        THEN REGEXP_REPLACE(syr_home_sido_nm,'충청북도','충북')
         WHEN syr_home_sido_nm = '경상남도'        THEN REGEXP_REPLACE(syr_home_sido_nm,'경상남도','경남')
         WHEN syr_home_sido_nm = '경상북도'        THEN REGEXP_REPLACE(syr_home_sido_nm,'경상북도','경북')
         ELSE ''
   END
   as syr_home_sido_nm,

   CASE
         WHEN syr_home_sigungu_nm is null THEN ''

         WHEN syr_home_sigungu_nm like '서울%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'서울특별시','서울')

         WHEN syr_home_sigungu_nm like '인천%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'인천광역시','인천')
         WHEN syr_home_sigungu_nm like '울산%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'울산광역시','울산')
         WHEN syr_home_sigungu_nm like '부산%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'부산광역시','부산')
         WHEN syr_home_sigungu_nm like '대전%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'대전광역시','대전')
         WHEN syr_home_sigungu_nm like '대구%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'대구광역시','대구')
         WHEN syr_home_sigungu_nm like '광주%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'광주광역시','광주')

         WHEN syr_home_sigungu_nm like '세종%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'세종특별자치시','세종')
         WHEN syr_home_sigungu_nm like '제주%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'제주특별자치도','제주')

         WHEN syr_home_sigungu_nm like '강원%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'강원도','강원')
         WHEN syr_home_sigungu_nm like '경기%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'경기도','경기')

         WHEN syr_home_sigungu_nm like '전라남도%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'전라남도','전남')
         WHEN syr_home_sigungu_nm like '전라북도%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'전라북도','전북')
         WHEN syr_home_sigungu_nm like '충청남도%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'충청남도','충남')
         WHEN syr_home_sigungu_nm like '충청북도%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'충청북도','충북')
         WHEN syr_home_sigungu_nm like '경상남도%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'경상남도','경남')
         WHEN syr_home_sigungu_nm like '경상북도%'        THEN REGEXP_REPLACE(syr_home_sigungu_nm,'경상북도','경북')
         ELSE ''
      END
      as syr_home_sigungu_nm,


      CASE
         WHEN syr_home_upmyundong_nm is null THEN ''

         WHEN syr_home_upmyundong_nm like '서울%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'서울특별시','서울')

         WHEN syr_home_upmyundong_nm like '인천%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'인천광역시','인천')
         WHEN syr_home_upmyundong_nm like '울산%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'울산광역시','울산')
         WHEN syr_home_upmyundong_nm like '부산%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'부산광역시','부산')
         WHEN syr_home_upmyundong_nm like '대전%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'대전광역시','대전')
         WHEN syr_home_upmyundong_nm like '대구%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'대구광역시','대구')
         WHEN syr_home_upmyundong_nm like '광주%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'광주광역시','광주')

         WHEN syr_home_upmyundong_nm like '세종%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'세종특별자치시','세종')
         WHEN syr_home_upmyundong_nm like '제주%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'제주특별자치도','제주')

         WHEN syr_home_upmyundong_nm like '강원%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'강원도','강원')
         WHEN syr_home_upmyundong_nm like '경기%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'경기도','경기')

         WHEN syr_home_upmyundong_nm like '전라남도%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'전라남도','전남')
         WHEN syr_home_upmyundong_nm like '전라북도%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'전라북도','전북')
         WHEN syr_home_upmyundong_nm like '충청남도%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'충청남도','충남')
         WHEN syr_home_upmyundong_nm like '충청북도%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'충청북도','충북')
         WHEN syr_home_upmyundong_nm like '경상남도%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'경상남도','경남')
         WHEN syr_home_upmyundong_nm like '경상북도%'        THEN REGEXP_REPLACE(syr_home_upmyundong_nm,'경상북도','경북')
         ELSE ''
      END
      as syr_home_upmyundong_nm


     FROM
     svc_custpfdb.pdb_mp_com_f_info
     WHERE
     ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'

   ) b

   ON a1.ci = b.ci

   LEFT JOIN

   svc_custpfdb.pdb_mp_com_m_syr_segm c

   ON a1.ci = c.ci

   LEFT JOIN

   (
     SELECT
     ci,
     cast(score * 100 as decimal) as scr,
     cls_100_gr
     FROM
     svc_custpfdb.pdb_mp_com_syr_chun_score
   ) d

   ON a1.ci = d.ci
;


