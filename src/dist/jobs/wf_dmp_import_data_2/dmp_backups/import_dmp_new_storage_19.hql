




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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='19')


SELECT DISTINCT

   a1.ci,
   'Y',
   if(b.ocb_app_mbr_yn   is null, '', b.ocb_app_mbr_yn),
   if(b.ocb_lock_mbr_yn  is null, '', b.ocb_lock_mbr_yn),
   if(b.ocb_join_dt      is null, '', b.ocb_join_dt),
   if(b.ocb_app_join_dt  is null, '', b.ocb_app_join_dt),
   if(b.ocb_lock_join_dt is null, '', b.ocb_lock_join_dt),
   if(a.push_agr_yn      is null, '', a.push_agr_yn),
   if(b.ocb_hld_pnt      is null, '', b.ocb_hld_pnt),
   if(a.age              is null, '', a.age),
   if(a.sex_cd           is null, '', a.sex_cd),
   if(b.ocb_frgn_yn      is null, '', b.ocb_frgn_yn),
   if(b.ocb_marry_yn     is null, '', b.ocb_marry_yn),

   b.ocb_home_sido_nm,
   b.ocb_home_sigungu_nm,
   b.ocb_home_upmyundong_nm,
   b.ocb_office_sido_nm,
   b.ocb_office_sigungu_nm,
   b.ocb_office_upmyundong_nm,
   '',
   '',
   '',
   '',
   '',
   if(a.mbr_fg_cd                 is null, '', a.mbr_fg_cd),
   if(a.svc_mkt_agr_yn            is null, '', a.svc_mkt_agr_yn),
   if(b.ocb_tr_grd                is null, '', b.ocb_tr_grd),
   if(b.ocb_app_acvt_per_cd       is null, '', b.ocb_app_acvt_per_cd),
   if(b.ocb_cpn_last_ract_dt      is null, '', b.ocb_cpn_last_ract_dt),
   if(b.ocb_cpn_ract_day_cnt      is null, '', b.ocb_cpn_ract_day_cnt),
   if(b.ocb_cpn_acvt_per_cd       is null, '', b.ocb_cpn_acvt_per_cd),
   if(b.ocb_push_ract_clk_rt      is null, '', cast(b.ocb_push_ract_clk_rt as decimal)),
   if(b.ocb_push_ract_acvt_per_cd is null, '', b.ocb_push_ract_acvt_per_cd),


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
     ocb_app_mbr_yn,
     ocb_lock_mbr_yn,
     ocb_join_dt,
     ocb_app_join_dt,
     ocb_lock_join_dt,
     ocb_hld_pnt,
     CASE
         WHEN ocb_frgn_yn IS NULL THEN ''
         WHEN ocb_frgn_yn = 'N'    THEN 'L'
         WHEN ocb_frgn_yn = 'Y'    THEN 'F'
         ELSE ''
     END as ocb_frgn_yn,
     ocb_marry_yn,


   CASE
         WHEN ocb_home_sido_nm is null THEN ''

         WHEN ocb_home_sido_nm like '서울%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'서울특별시','서울')

         WHEN ocb_home_sido_nm like '인천%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'인천광역시','인천')
         WHEN ocb_home_sido_nm like '울산%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'울산광역시','울산')
         WHEN ocb_home_sido_nm like '부산%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'부산광역시','부산')
         WHEN ocb_home_sido_nm like '대전%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'대전광역시','대전')
         WHEN ocb_home_sido_nm like '대구%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'대구광역시','대구')
         WHEN ocb_home_sido_nm like '광주%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'광주광역시','광주')

         WHEN ocb_home_sido_nm like '세종%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'세종특별자치시','세종')
         WHEN ocb_home_sido_nm like '제주%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'제주특별자치도','제주')

         WHEN ocb_home_sido_nm like '강원%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'강원도','강원')
         WHEN ocb_home_sido_nm like '경기%'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'경기도','경기')

         WHEN ocb_home_sido_nm = '전라남도'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'전라남도','전남')
         WHEN ocb_home_sido_nm = '전라북도'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'전라북도','전북')
         WHEN ocb_home_sido_nm = '충청남도'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'충청남도','충남')
         WHEN ocb_home_sido_nm = '충청북도'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'충청북도','충북')
         WHEN ocb_home_sido_nm = '경상남도'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'경상남도','경남')
         WHEN ocb_home_sido_nm = '경상북도'        THEN REGEXP_REPLACE(ocb_home_sido_nm,'경상북도','경북')
         ELSE ''
   END
   as ocb_home_sido_nm,

   CASE
         WHEN ocb_home_sigungu_nm is null THEN ''

         WHEN ocb_home_sigungu_nm like '서울%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'서울특별시','서울')

         WHEN ocb_home_sigungu_nm like '인천%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'인천광역시','인천')
         WHEN ocb_home_sigungu_nm like '울산%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'울산광역시','울산')
         WHEN ocb_home_sigungu_nm like '부산%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'부산광역시','부산')
         WHEN ocb_home_sigungu_nm like '대전%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'대전광역시','대전')
         WHEN ocb_home_sigungu_nm like '대구%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'대구광역시','대구')
         WHEN ocb_home_sigungu_nm like '광주%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'광주광역시','광주')

         WHEN ocb_home_sigungu_nm like '세종%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'세종특별자치시','세종')
         WHEN ocb_home_sigungu_nm like '제주%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'제주특별자치도','제주')

         WHEN ocb_home_sigungu_nm like '강원%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'강원도','강원')
         WHEN ocb_home_sigungu_nm like '경기%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'경기도','경기')

         WHEN ocb_home_sigungu_nm like '전라남도%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'전라남도','전남')
         WHEN ocb_home_sigungu_nm like '전라북도%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'전라북도','전북')
         WHEN ocb_home_sigungu_nm like '충청남도%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'충청남도','충남')
         WHEN ocb_home_sigungu_nm like '충청북도%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'충청북도','충북')
         WHEN ocb_home_sigungu_nm like '경상남도%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'경상남도','경남')
         WHEN ocb_home_sigungu_nm like '경상북도%'        THEN REGEXP_REPLACE(ocb_home_sigungu_nm,'경상북도','경북')
         ELSE ''
      END
      as ocb_home_sigungu_nm,


      CASE
         WHEN ocb_home_upmyundong_nm is null THEN ''

         WHEN ocb_home_upmyundong_nm like '서울%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'서울특별시','서울')

         WHEN ocb_home_upmyundong_nm like '인천%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'인천광역시','인천')
         WHEN ocb_home_upmyundong_nm like '울산%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'울산광역시','울산')
         WHEN ocb_home_upmyundong_nm like '부산%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'부산광역시','부산')
         WHEN ocb_home_upmyundong_nm like '대전%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'대전광역시','대전')
         WHEN ocb_home_upmyundong_nm like '대구%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'대구광역시','대구')
         WHEN ocb_home_upmyundong_nm like '광주%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'광주광역시','광주')

         WHEN ocb_home_upmyundong_nm like '세종%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'세종특별자치시','세종')
         WHEN ocb_home_upmyundong_nm like '제주%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'제주특별자치도','제주')

         WHEN ocb_home_upmyundong_nm like '강원%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'강원도','강원')
         WHEN ocb_home_upmyundong_nm like '경기%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'경기도','경기')

         WHEN ocb_home_upmyundong_nm like '전라남도%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'전라남도','전남')
         WHEN ocb_home_upmyundong_nm like '전라북도%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'전라북도','전북')
         WHEN ocb_home_upmyundong_nm like '충청남도%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'충청남도','충남')
         WHEN ocb_home_upmyundong_nm like '충청북도%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'충청북도','충북')
         WHEN ocb_home_upmyundong_nm like '경상남도%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'경상남도','경남')
         WHEN ocb_home_upmyundong_nm like '경상북도%'        THEN REGEXP_REPLACE(ocb_home_upmyundong_nm,'경상북도','경북')
         ELSE ''
      END
      as ocb_home_upmyundong_nm,

      CASE
         WHEN ocb_office_sido_nm is null THEN ''

         WHEN ocb_office_sido_nm like '서울%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'서울특별시','서울')

         WHEN ocb_office_sido_nm like '인천%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'인천광역시','인천')
         WHEN ocb_office_sido_nm like '울산%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'울산광역시','울산')
         WHEN ocb_office_sido_nm like '부산%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'부산광역시','부산')
         WHEN ocb_office_sido_nm like '대전%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'대전광역시','대전')
         WHEN ocb_office_sido_nm like '대구%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'대구광역시','대구')
         WHEN ocb_office_sido_nm like '광주%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'광주광역시','광주')

         WHEN ocb_office_sido_nm like '세종%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'세종특별자치시','세종')
         WHEN ocb_office_sido_nm like '제주%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'제주특별자치도','제주')

         WHEN ocb_office_sido_nm like '강원%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'강원도','강원')
         WHEN ocb_office_sido_nm like '경기%'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'경기도','경기')

         WHEN ocb_office_sido_nm = '전라남도'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'전라남도','전남')
         WHEN ocb_office_sido_nm = '전라북도'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'전라북도','전북')
         WHEN ocb_office_sido_nm = '충청남도'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'충청남도','충남')
         WHEN ocb_office_sido_nm = '충청북도'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'충청북도','충북')
         WHEN ocb_office_sido_nm = '경상남도'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'경상남도','경남')
         WHEN ocb_office_sido_nm = '경상북도'        THEN REGEXP_REPLACE(ocb_office_sido_nm,'경상북도','경북')
         ELSE ''
      END
      as ocb_office_sido_nm,


      CASE
         WHEN ocb_office_sigungu_nm is null THEN ''

         WHEN ocb_office_sigungu_nm like '서울%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'서울특별시','서울')

         WHEN ocb_office_sigungu_nm like '인천%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'인천광역시','인천')
         WHEN ocb_office_sigungu_nm like '울산%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'울산광역시','울산')
         WHEN ocb_office_sigungu_nm like '부산%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'부산광역시','부산')
         WHEN ocb_office_sigungu_nm like '대전%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'대전광역시','대전')
         WHEN ocb_office_sigungu_nm like '대구%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'대구광역시','대구')
         WHEN ocb_office_sigungu_nm like '광주%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'광주광역시','광주')

         WHEN ocb_office_sigungu_nm like '세종%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'세종특별자치시','세종')
         WHEN ocb_office_sigungu_nm like '제주%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'제주특별자치도','제주')

         WHEN ocb_office_sigungu_nm like '강원%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'강원도','강원')
         WHEN ocb_office_sigungu_nm like '경기%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'경기도','경기')

         WHEN ocb_office_sigungu_nm like '전라남도%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'전라남도','전남')
         WHEN ocb_office_sigungu_nm like '전라북도%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'전라북도','전북')
         WHEN ocb_office_sigungu_nm like '충청남도%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'충청남도','충남')
         WHEN ocb_office_sigungu_nm like '충청북도%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'충청북도','충북')
         WHEN ocb_office_sigungu_nm like '경상남도%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'경상남도','경남')
         WHEN ocb_office_sigungu_nm like '경상북도%'        THEN REGEXP_REPLACE(ocb_office_sigungu_nm,'경상북도','경북')
         ELSE ''
      END
      as ocb_office_sigungu_nm,

      CASE
         WHEN ocb_office_upmyundong_nm is null THEN ''

         WHEN ocb_office_upmyundong_nm like '서울%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'서울특별시','서울')

         WHEN ocb_office_upmyundong_nm like '인천%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'인천광역시','인천')
         WHEN ocb_office_upmyundong_nm like '울산%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'울산광역시','울산')
         WHEN ocb_office_upmyundong_nm like '부산%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'부산광역시','부산')
         WHEN ocb_office_upmyundong_nm like '대전%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'대전광역시','대전')
         WHEN ocb_office_upmyundong_nm like '대구%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'대구광역시','대구')
         WHEN ocb_office_upmyundong_nm like '광주%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'광주광역시','광주')

         WHEN ocb_office_upmyundong_nm like '세종%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'세종특별자치시','세종')
         WHEN ocb_office_upmyundong_nm like '제주%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'제주특별자치도','제주')

         WHEN ocb_office_upmyundong_nm like '강원%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'강원도','강원')
         WHEN ocb_office_upmyundong_nm like '경기%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'경기도','경기')

         WHEN ocb_office_upmyundong_nm like '전라남도%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'전라남도','전남')
         WHEN ocb_office_upmyundong_nm like '전라북도%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'전라북도','전북')
         WHEN ocb_office_upmyundong_nm like '충청남도%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'충청남도','충남')
         WHEN ocb_office_upmyundong_nm like '충청북도%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'충청북도','충북')
         WHEN ocb_office_upmyundong_nm like '경상남도%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'경상남도','경남')
         WHEN ocb_office_upmyundong_nm like '경상북도%'        THEN REGEXP_REPLACE(ocb_office_upmyundong_nm,'경상북도','경북')
         ELSE ''
      END
      as ocb_office_upmyundong_nm,


     ocb_tr_grd,
     ocb_app_acvt_per_cd,
     ocb_cpn_last_ract_dt,
     ocb_cpn_ract_day_cnt,
     ocb_cpn_acvt_per_cd,
     ocb_push_ract_clk_rt,
     ocb_push_ract_acvt_per_cd

     FROM
     svc_custpfdb.pdb_mp_com_f_info
     WHERE
     ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'

   ) b

   ON a1.ci = b.ci

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


