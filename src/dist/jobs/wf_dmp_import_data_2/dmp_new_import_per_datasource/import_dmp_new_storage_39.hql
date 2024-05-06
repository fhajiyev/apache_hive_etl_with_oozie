

set hivevar:day2before;
set hivevar:day182before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='39')

SELECT DISTINCT
ci,
sch_dt,
'',
CASE from_unixtime(unix_timestamp(sch_dt, 'yyyyMMdd'), 'u')
     WHEN 1 THEN 'mon'
     WHEN 2 THEN 'tue'
     WHEN 3 THEN 'wed'
     WHEN 4 THEN 'thu'
     WHEN 5 THEN 'fri'
     WHEN 6 THEN 'sat'
     ELSE 'sun'
END,
'search',
CASE
         WHEN sido_nm is null THEN ''

         WHEN sido_nm like '서울%'        THEN REGEXP_REPLACE(sido_nm,'서울특별시','서울')

         WHEN sido_nm like '인천%'        THEN REGEXP_REPLACE(sido_nm,'인천광역시','인천')
         WHEN sido_nm like '울산%'        THEN REGEXP_REPLACE(sido_nm,'울산광역시','울산')
         WHEN sido_nm like '부산%'        THEN REGEXP_REPLACE(sido_nm,'부산광역시','부산')
         WHEN sido_nm like '대전%'        THEN REGEXP_REPLACE(sido_nm,'대전광역시','대전')
         WHEN sido_nm like '대구%'        THEN REGEXP_REPLACE(sido_nm,'대구광역시','대구')
         WHEN sido_nm like '광주%'        THEN REGEXP_REPLACE(sido_nm,'광주광역시','광주')

         WHEN sido_nm like '세종%'        THEN REGEXP_REPLACE(sido_nm,'세종특별자치시','세종')
         WHEN sido_nm like '제주%'        THEN REGEXP_REPLACE(sido_nm,'제주특별자치도','제주')

         WHEN sido_nm like '강원%'        THEN REGEXP_REPLACE(sido_nm,'강원도','강원')
         WHEN sido_nm like '경기%'        THEN REGEXP_REPLACE(sido_nm,'경기도','경기')

         WHEN sido_nm = '전라남도'        THEN REGEXP_REPLACE(sido_nm,'전라남도','전남')
         WHEN sido_nm = '전라북도'        THEN REGEXP_REPLACE(sido_nm,'전라북도','전북')
         WHEN sido_nm = '충청남도'        THEN REGEXP_REPLACE(sido_nm,'충청남도','충남')
         WHEN sido_nm = '충청북도'        THEN REGEXP_REPLACE(sido_nm,'충청북도','충북')
         WHEN sido_nm = '경상남도'        THEN REGEXP_REPLACE(sido_nm,'경상남도','경남')
         WHEN sido_nm = '경상북도'        THEN REGEXP_REPLACE(sido_nm,'경상북도','경북')
         ELSE ''
END,

CASE
         WHEN sigungu_nm is null THEN ''

         WHEN sigungu_nm like '서울%'        THEN REGEXP_REPLACE(sigungu_nm,'서울특별시','서울')

         WHEN sigungu_nm like '인천%'        THEN REGEXP_REPLACE(sigungu_nm,'인천광역시','인천')
         WHEN sigungu_nm like '울산%'        THEN REGEXP_REPLACE(sigungu_nm,'울산광역시','울산')
         WHEN sigungu_nm like '부산%'        THEN REGEXP_REPLACE(sigungu_nm,'부산광역시','부산')
         WHEN sigungu_nm like '대전%'        THEN REGEXP_REPLACE(sigungu_nm,'대전광역시','대전')
         WHEN sigungu_nm like '대구%'        THEN REGEXP_REPLACE(sigungu_nm,'대구광역시','대구')
         WHEN sigungu_nm like '광주%'        THEN REGEXP_REPLACE(sigungu_nm,'광주광역시','광주')

         WHEN sigungu_nm like '세종%'        THEN REGEXP_REPLACE(sigungu_nm,'세종특별자치시','세종')
         WHEN sigungu_nm like '제주%'        THEN REGEXP_REPLACE(sigungu_nm,'제주특별자치도','제주')

         WHEN sigungu_nm like '강원%'        THEN REGEXP_REPLACE(sigungu_nm,'강원도','강원')
         WHEN sigungu_nm like '경기%'        THEN REGEXP_REPLACE(sigungu_nm,'경기도','경기')

         WHEN sigungu_nm like '전라남도%'        THEN REGEXP_REPLACE(sigungu_nm,'전라남도','전남')
         WHEN sigungu_nm like '전라북도%'        THEN REGEXP_REPLACE(sigungu_nm,'전라북도','전북')
         WHEN sigungu_nm like '충청남도%'        THEN REGEXP_REPLACE(sigungu_nm,'충청남도','충남')
         WHEN sigungu_nm like '충청북도%'        THEN REGEXP_REPLACE(sigungu_nm,'충청북도','충북')
         WHEN sigungu_nm like '경상남도%'        THEN REGEXP_REPLACE(sigungu_nm,'경상남도','경남')
         WHEN sigungu_nm like '경상북도%'        THEN REGEXP_REPLACE(sigungu_nm,'경상북도','경북')
         ELSE ''
END,


CASE
         WHEN upmyundong_nm is null THEN ''

         WHEN upmyundong_nm like '서울%'        THEN REGEXP_REPLACE(upmyundong_nm,'서울특별시','서울')

         WHEN upmyundong_nm like '인천%'        THEN REGEXP_REPLACE(upmyundong_nm,'인천광역시','인천')
         WHEN upmyundong_nm like '울산%'        THEN REGEXP_REPLACE(upmyundong_nm,'울산광역시','울산')
         WHEN upmyundong_nm like '부산%'        THEN REGEXP_REPLACE(upmyundong_nm,'부산광역시','부산')
         WHEN upmyundong_nm like '대전%'        THEN REGEXP_REPLACE(upmyundong_nm,'대전광역시','대전')
         WHEN upmyundong_nm like '대구%'        THEN REGEXP_REPLACE(upmyundong_nm,'대구광역시','대구')
         WHEN upmyundong_nm like '광주%'        THEN REGEXP_REPLACE(upmyundong_nm,'광주광역시','광주')

         WHEN upmyundong_nm like '세종%'        THEN REGEXP_REPLACE(upmyundong_nm,'세종특별자치시','세종')
         WHEN upmyundong_nm like '제주%'        THEN REGEXP_REPLACE(upmyundong_nm,'제주특별자치도','제주')

         WHEN upmyundong_nm like '강원%'        THEN REGEXP_REPLACE(upmyundong_nm,'강원도','강원')
         WHEN upmyundong_nm like '경기%'        THEN REGEXP_REPLACE(upmyundong_nm,'경기도','경기')

         WHEN upmyundong_nm like '전라남도%'        THEN REGEXP_REPLACE(upmyundong_nm,'전라남도','전남')
         WHEN upmyundong_nm like '전라북도%'        THEN REGEXP_REPLACE(upmyundong_nm,'전라북도','전북')
         WHEN upmyundong_nm like '충청남도%'        THEN REGEXP_REPLACE(upmyundong_nm,'충청남도','충남')
         WHEN upmyundong_nm like '충청북도%'        THEN REGEXP_REPLACE(upmyundong_nm,'충청북도','충북')
         WHEN upmyundong_nm like '경상남도%'        THEN REGEXP_REPLACE(upmyundong_nm,'경상남도','경남')
         WHEN upmyundong_nm like '경상북도%'        THEN REGEXP_REPLACE(upmyundong_nm,'경상북도','경북')
         ELSE ''
END,


if(ca_name is null, '', ca_name),
if(cb_name is null, '', cb_name),
if(cc_name is null, '', cc_name),
if(cd_name is null, '', cd_name),

TRIM
 (
 CONCAT
 (
     CASE WHEN ca_name IS NULL OR TRIM(ca_name) IN ('') THEN ''
     ELSE TRIM(ca_name)
     END,
     CASE WHEN cb_name IS NULL OR TRIM(cb_name) IN ('') THEN ''
     ELSE CONCAT('\||',TRIM(cb_name))
     END,
     CASE WHEN cc_name IS NULL OR TRIM(cc_name) IN ('') THEN ''
     ELSE CONCAT('\||',TRIM(cc_name))
     END,
     CASE WHEN cd_name IS NULL OR TRIM(cd_name) IN ('') THEN ''
     ELSE CONCAT('\||',TRIM(cd_name))
     END
 )
 ),

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

FROM

svc_custpfdb.pdb_mp_base_tma_dest_sch

WHERE
dt = '${hivevar:day2before}'
;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='39');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='39');

