USE dmp;


set hivevar:hourbefore;
set hivevar:hourcurrent;
set hivevar:hour91before;



set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


-- Allice

INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='123')
SELECT

a.uid,
a.request_date,
a.request_time,
CASE
      WHEN a.WEEKDAY = '1' THEN 'Mon'
      WHEN a.WEEKDAY = '2' THEN 'Tue'
      WHEN a.WEEKDAY = '3' THEN 'Wed'
      WHEN a.WEEKDAY = '4' THEN 'Thu'
      WHEN a.WEEKDAY = '5' THEN 'Fri'
      WHEN a.WEEKDAY = '6' THEN 'Sat'
      WHEN a.WEEKDAY = '7' THEN 'Sun'
END,
a.event,
'',
a.slot,
a.channel,
'',
'',
a.model,
'',
a.campaign_sq,
a.ad_sq,
'',
a.bill_type,
'',
a.media_sq,
'',

  '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '${hivevar:hourbefore}',
  log_time

FROM

(

 SELECT

 CASE WHEN gaid <> '' THEN CONCAT('(GAID)',gaid)
      WHEN idfa <> '' THEN CONCAT('(IDFA)',UPPER(idfa))
      ELSE CONCAT('(DMPC)',uid)
 END as uid,

 SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
 SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
 from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,
 event,
 slot,
 channel,
 model,
 campaign_sq,
 ad_sq,
 bill_type,
 media_sq,

 log_time

 FROM
 allice.allice_ad_log_live
 WHERE
 part_hour = '${hivevar:hourbefore}'

) a
WHERE uid <> ''
;

