



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

add jar hdfs://skpds/app/hive/udf/common-udf-1.0.jar;
CREATE TEMPORARY FUNCTION di_memnodecrypt AS 'com.di.hive.udf.MemnoDecrypt';

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='9')
select distinct 
  u.ci                                                      as uid, 
  a.dt                                                      as col001, 
  ''                                                        as col002, 
  case from_unixtime(unix_timestamp(a.dt, 'yyyyMMdd'), 'u')
    when 1 then 'mon'
    when 2 then 'tue'
    when 3 then 'wed' 
    when 4 then 'thu'
    when 5 then 'fri'
    when 6 then 'sat' 
    else 'sun'
  end                                                       as col003, 
  ''                                                        as col004,
  a.channel                                                 as col005, 
  a.carrier_name,
  a.manufacturer,
  a.device_model,
  a.os_name,
  a.os_version,
  a.browser_name,
  a.browser_version,

  
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

from (
-- select distinct
--     memno,
--     dt as dt,
--     '00' as channel,
--     '' as carrier_name,
--     '' as manufacturer,
--     '' as device_model,
--     '' as os_name,
--     '' as os_version,
--     '' as browser_name,
--     '' as browser_version
--
--     from SVC_CDPF.INTGT_LOG_BASE_EVS_MBL  where dt = '${hivevar:day2before}' and memno <> ''
-- union all
-- select distinct
--     memno,
--     dt as dt,
--     '01' as channel,
--     '' as carrier_name,
--     '' as manufacturer,
--     '' as device_model,
--     '' as os_name,
--     '' as os_version,
--     '' as browser_name,
--     '' as browser_version
--
--     from SVC_CDPF.INTGT_LOG_BASE_EVS_WEB  where dt = '${hivevar:day2before}' and memno <> ''
-- union all
select distinct 
   di_memnodecrypt(member_no)                        as memno, 
   '${hivevar:day2before}'                       as dt,
   CASE WHEN poc_clf = 'app' THEN '00' ELSE '01' END as channel,
   carrier_name                                      as carrier_name,
   manufacturer                                      as manufacturer,
   device_model                                      as device_model,
   os_name                                           as os_name,
   os_version                                        as os_version,
   browser_name                                      as browser_name,
   browser_version                                   as browser_version

   FROM
   ELEVENST_SKP.EVS_SKP_LOG_CLIENT_LIVE
   WHERE
   PART_HOUR BETWEEN '${hivevar:day2before}00' and '${hivevar:day2before}24'
   and member_no is not null and member_no <> ''
   and poc_clf is not null and poc_clf <> ''

) a
join
(
   select
   ci,
   elev_id
   from
   dmp_pi.id_pool
   where
   part_date = '${hivevar:day2before}'
) u
on u.elev_id = a.memno
;


alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='9');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='9');


