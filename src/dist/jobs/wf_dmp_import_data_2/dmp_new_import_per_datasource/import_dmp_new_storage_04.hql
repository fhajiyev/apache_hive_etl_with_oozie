


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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='4')


SELECT
   mb.ci,
   ma.dt,
   '',
   CASE from_unixtime(unix_timestamp(ma.dt, 'yyyyMMdd'), 'u')
      WHEN 1 THEN 'mon'
      WHEN 2 THEN 'tue'
      WHEN 3 THEN 'wed'
      WHEN 4 THEN 'thu'
      WHEN 5 THEN 'fri'
      WHEN 6 THEN 'sat'
      ELSE 'sun'
   END,
   '',
   ma.push_type,
   ma.push_id,
   ma.feed_id,
   ma.push_name,
   ma.feed_name,

   if(mc.carrier_name is null, '', mc.carrier_name),
   if(mc.manufacturer is null, '', mc.manufacturer),
   if(mc.device_model is null, '', mc.device_model),
   if(mc.os_type      is null, '', mc.os_type     ),
   if(mc.os_version   is null, '', mc.os_version  ),
  

   '','','','','','',
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

SELECT DISTINCT
  mbr_id                    as member_id,
  SUBSTR(cdc_base_time,1,8) as dt,
  '00'                      as push_type,  
  ''                        as push_id,
  ''                        as feed_id,
  ''                        as push_name,
  ''                        as feed_name 


FROM
       OCB.OCB_D_INTG_LOG 

WHERE  

       POC = '01'
       AND DT ='${hivevar:day2before}'
       AND LOWER(CONCAT(TRIM(PAGE_ID),TRIM(ACTN_ID))) NOT IN ('unknownpush_receive'       ,'unknownapp_call'        ,'/discoverunknown'
                                                             ,'/mobileleafletunknown'     ,'/checkinunknown'        ,'j0100unknown'
                                                             ,'/pushnoti/indicatorunknown','/pushnoti/popupunknown' ,'j0101unknown'
                                                             ,'unknownissue_coupon_result','unknownapp_call'
                                                             ,'/marketingpushunknown'
                                                             ,'/locker/maindrag.unlock','/explaincriticalpermissionunknown'
                                                             ,'unknownlock_content_receive'
                                                             ,'unknownpermission_status'
                                                             ,'unknownmarketingpush'
                                                             )
       AND LOWER(PAGE_ID) NOT IN ('/locker/weather', '/locker/weatherponginstall')
       AND SUBSTR(cdc_base_time,1,8) = '${hivevar:day2before}'
       AND mbr_id IS NOT NULL AND mbr_id <> ''

UNION ALL


SELECT
  mbr_id       as member_id,
  base_dt      as dt,
  '01'         as push_type,
  tgt_push_id  as push_id,
  ''           as feed_id,
  ''           as push_name,
  ''           as feed_name

FROM
       OCB.MART_APP_PUSH_RCTN_CTNT

WHERE
       base_dt     = '${hivevar:day2before}'
       AND tgt_push_id <> ''
       AND push_rcv_yn = '1'


UNION ALL

SELECT
  mbr_id       as member_id,
  base_dt      as dt,
  '03'         as push_type,
  tgt_push_id  as push_id,
  ''           as feed_id,
  ''           as push_name,
  ''           as feed_name

FROM
       OCB.MART_APP_PUSH_RCTN_CTNT
WHERE

       base_dt     = '${hivevar:day2before}'
       AND tgt_push_id <> ''
       AND push_clk_yn = '1'


UNION ALL

SELECT DISTINCT
  mbr_id                    as member_id,
  SUBSTR(cdc_base_time,1,8) as dt,
  '06'                      as push_type,
  ''                        as push_id,
  ''                        as feed_id,
  ''                        as push_name,
  ''                        as feed_name


FROM
       OCB.OCB_D_INTG_LOG

WHERE

       POC = '01'
       AND DT ='${hivevar:day2before}'
       AND LOCKERAPP_LOG_YN = 'Y'
       AND SUBSTR(cdc_base_time,1,8) = '${hivevar:day2before}'
       AND mbr_id IS NOT NULL AND mbr_id <> ''

) ma


INNER JOIN 

(

SELECT
ci,
ocb_id
from
dmp_pi.id_pool
where part_date = '${hivevar:day2before}'

) mb

ON 

ma.member_id = mb.ocb_id

LEFT JOIN

(

SELECT DISTINCT

funcs.aesdecrypt(translate(get_json_object(body, '\$.mbr_id'),'\n\r\t',''), '38880189683628965483845997893130223408') as mbr_id,
carrier_name,
manufacturer,
device_model,
os_type,
os_version

from
ocb.log_touch_50
where
part_hour between '${hivevar:day2before}00' and '${hivevar:day2before}24'
and
get_json_object(body, '\$.mbr_id') is not null and get_json_object(body, '\$.mbr_id') <> ''


) mc

ON

ma.member_id = mc.mbr_id

;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='4');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='4');

