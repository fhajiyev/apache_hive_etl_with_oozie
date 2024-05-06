set hivevar:day2before;
set hivevar:day92before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.mapjoin.optimized.hashtable=false;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='45')

select
    t.ci,
    t.dt,
    t.enter_time,
    t.exit_time,
    t.weekday,
    t.dwell_time,

    t.sido,

    CASE
       WHEN t.sido <> '' AND t.sigungu <> '' THEN CONCAT(t.sido, ' ', t.sigungu)
       ELSE ''
    END,

    CASE
       WHEN t.sido <> '' AND (t.sigungu <> '' OR t.sido = '세종') AND t.dong <> '' THEN CONCAT(t.sido, ' ', t.sigungu, ' ', t.dong)
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

A.ci,
A.dt,
A.enter_time,
A.exit_time,
case A.weekday
          when 1 then 'mon'
          when 2 then 'tue'
          when 3 then 'wed'
          when 4 then 'thu'
          when 5 then 'fri'
          when 6 then 'sat'
          else 'sun'
end as weekday,
A.dwell_time,

CASE
      WHEN loc.sido is null THEN ''

      WHEN loc.sido like '서울%'        THEN '서울'

      WHEN loc.sido like '인천%'        THEN '인천'
      WHEN loc.sido like '울산%'        THEN '울산'
      WHEN loc.sido like '부산%'        THEN '부산'
      WHEN loc.sido like '대전%'        THEN '대전'
      WHEN loc.sido like '대구%'        THEN '대구'
      WHEN loc.sido like '광주%'        THEN '광주'

      WHEN loc.sido like '세종%'        THEN '세종'
      WHEN loc.sido like '제주%'        THEN '제주'

      WHEN loc.sido like '강원%'        THEN '강원'
      WHEN loc.sido like '경기%'        THEN '경기'

      WHEN loc.sido = '전라남도'        THEN '전남'
      WHEN loc.sido = '전라북도'        THEN '전북'
      WHEN loc.sido = '충청남도'        THEN '충남'
      WHEN loc.sido = '충청북도'        THEN '충북'
      WHEN loc.sido = '경상남도'        THEN '경남'
      WHEN loc.sido = '경상북도'        THEN '경북'
  END as sido,
if(loc.sigungu is null, '', loc.sigungu) as sigungu,
if(loc.dong    is null, '',    loc.dong) as dong

from
(
    select
        ci,
        SUBSTR(geo_code,2,8) as geo_code,
        substr(enter_time, 9,2) as enter_time,
        substr(exit_time,  9,2) as exit_time,
        dwell_time,
        part_date as dt,
        from_unixtime(unix_timestamp(part_date, 'yyyyMMdd'), 'u') as weekday
    from
        svc_cim.prod_daily_visit_per_dong
    where
        part_date = '${hivevar:day2before}'
) A

 left join

 (
          select
          geo_code,
          max(addr_1) as sido,
          max(addr_2) as sigungu,
          max(addr_3) as dong

          from
          svc_context_ano.meta_geo_code_info
          group by geo_code
 ) loc

 on SUBSTR(loc.geo_code, 2, 8) = A.geo_code

-- left join
--
-- (
--           select
--           local_cd as hc_sido,
--           REGEXP_REPLACE(local_nm,  '세종시','세종') as local_nm
--           from
--           imc.dbm_m2_merch_local_a_v
--           where
--           length(local_cd) = 2
-- ) sido_loc
--
-- on sido_loc.hc_sido = SUBSTR(A.geo_code, 1, 2)
--
-- left join
--
-- (
--           select
--           local_cd as hc_sigungu,
--           local_nm
--           from
--           imc.dbm_m2_merch_local_a_v
--           where
--           length(local_cd) = 5
-- ) sigungu_loc
--
-- on sigungu_loc.hc_sigungu = SUBSTR(A.geo_code, 1, 5)
--
-- left join
--
-- (
--           select
--           local_cd as hc_dong,
--           local_nm
--           from
--           imc.dbm_m2_merch_local_a_v
--           where
--           length(local_cd) = 8
-- ) dong_loc
--
-- on dong_loc.hc_dong = A.geo_code



) t
;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day92before}00', data_source_id='45');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day92before}00', data_source_id='45');

