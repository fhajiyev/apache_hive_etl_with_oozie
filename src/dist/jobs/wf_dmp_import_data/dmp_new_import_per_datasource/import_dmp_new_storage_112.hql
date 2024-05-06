USE dmp;


set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day3before;
set hivevar:day90before;
set hivevar:day91before;



set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

-- 오프라인 매장 방문

INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:day2before}00', data_source_id='112')

SELECT DISTINCT

t.DMP_ID,
t.dt,
t.visit_hh,
t.weekday,
t.class_id,
t.name,
t.dwell_min,

  t.sido,

  CASE
     WHEN t.sido <> '' AND t.sigungu <> '' THEN CONCAT(t.sido, ' ', t.sigungu)
     ELSE ''
  END,

  CASE
     WHEN t.sido <> '' AND (t.sigungu <> '' OR t.sido = '세종') AND t.dong <> '' THEN CONCAT(t.sido, ' ', t.sigungu, ' ', t.dong)
     ELSE ''
  END,

t.mid,
t.taid,
t.alliance_nm,
t.cate_name,

  if(SPLIT(t.NAME, ' +')[0] is null, '', TRIM(SPLIT(t.NAME, ' +')[0])),

  if(SPLIT(t.NAME, ' +')[1] is null, '', TRIM(SPLIT(t.NAME, ' +')[1])),

  if(SPLIT(t.NAME, ' +')[2] is null, '', TRIM(SPLIT(t.NAME, ' +')[2])),

  if(c.ca_name is null, '', c.ca_name),
  if(c.cb_name is null, '', c.cb_name),
  if(c.cc_name is null, '', c.cc_name),
  if(c.cd_name is null, '', c.cd_name),

  TRIM
  (
        CONCAT
        (
         CASE WHEN c.ca_name IS NULL OR TRIM(c.ca_name) IN ('', 'null') THEN ''
         ELSE TRIM(c.ca_name)
         END,
         CASE WHEN c.cb_name IS NULL OR TRIM(c.cb_name) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(c.cb_name))
         END,
         CASE WHEN c.cc_name IS NULL OR TRIM(c.cc_name) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(c.cc_name))
         END,
         CASE WHEN c.cd_name IS NULL OR TRIM(c.cd_name) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(c.cd_name))
         END
        )
  ),

  if(c.name_dmp is null, '', c.name_dmp),

  if(SPLIT(c.name_dmp, ' +')[0] is null, '', TRIM(SPLIT(c.name_dmp, ' +')[0])),

  if(SPLIT(c.name_dmp, ' +')[1] is null, '', TRIM(SPLIT(c.name_dmp, ' +')[1])),

  if(SPLIT(c.name_dmp, ' +')[2] is null, '', TRIM(SPLIT(c.name_dmp, ' +')[2])),


'','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
concat(t.dt, '00'),
t.dt



FROM

(

SELECT

A.DMP_ID,
A.dt,
case A.weekday
      when 1 then 'mon'
      when 2 then 'tue'
      when 3 then 'wed'
      when 4 then 'thu'
      when 5 then 'fri'
      when 6 then 'sat'
      else 'sun'
end as weekday,
A.name,
A.visit_hh,
A.dwell_min,

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
if(loc.dong    is null, '',    loc.dong) as dong,

A.class_id,
if(A.mid         is null, '', A.mid)         as mid,
if(A.taid        is null, '', A.taid)        as taid,
if(A.alliance_nm is null, '', A.alliance_nm) as alliance_nm,
if(A.cate_name   is null, '', A.cate_name)   as cate_name


from

(


SELECT

        CASE
            WHEN ID_TYPE = 'gaid' THEN CONCAT('(GAID)', AD_ID)
            ELSE CONCAT('(IDFA)', UPPER(AD_ID))
        END
        AS DMP_ID,


visit_dt as dt,
from_unixtime(unix_timestamp(part_date, 'yyyyMMdd'), 'u') as weekday,
name,
SUBSTR(geo_code,2,8) as geo_code,
'class1' as class_id,
mid,
taid,
alliance_nm,
REGEXP_REPLACE(cate_name,  '[\;\:]','||') as cate_name,
visit_hh,
dwell_min

from
svc_context_ano.prod_daily_visit_class1_hh
where part_date='${hivevar:day3before}'
and substr(visit_dt,1,4) <> '1970'

union all


SELECT

        CASE
            WHEN ID_TYPE = 'gaid' THEN CONCAT('(GAID)', AD_ID)
            ELSE CONCAT('(IDFA)', UPPER(AD_ID))
        END
        AS DMP_ID,


visit_dt as dt,
from_unixtime(unix_timestamp(part_date, 'yyyyMMdd'), 'u') as weekday,
name,
SUBSTR(geo_code,2,8) as geo_code,
'class2' as class_id,
mid,
taid,
alliance_nm,
REGEXP_REPLACE(cate_name,  '[\;\:]','||') as cate_name,
visit_hh,
dwell_min

from
svc_context_ano.prod_daily_visit_class2_hh
where part_date='${hivevar:day3before}'
and substr(visit_dt,1,4) <> '1970'


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

left join
(

  select
  mid,
  ca_name,
  cb_name,
  cc_name,
  cd_name,
  name_dmp
  from
  dmp.prod_imc_tmap_poi_info_v
  where mid is not null and mid <> ''

) C
on C.mid = t.mid

;


