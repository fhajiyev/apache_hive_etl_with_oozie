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


-- SK STOA

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='117')
  SELECT
  a.DMP_UID as uid,
  a.REQUEST_DATE,
  a.REQUEST_TIME,
  CASE
        WHEN a.WEEKDAY = '1' THEN 'Mon'
        WHEN a.WEEKDAY = '2' THEN 'Tue'
        WHEN a.WEEKDAY = '3' THEN 'Wed'
        WHEN a.WEEKDAY = '4' THEN 'Thu'
        WHEN a.WEEKDAY = '5' THEN 'Fri'
        WHEN a.WEEKDAY = '6' THEN 'Sat'
        WHEN a.WEEKDAY = '7' THEN 'Sun'
  END,

  if(ACTION   IS NULL, '', ACTION),
  if(ADVT_ID  IS NULL, '', ADVT_ID),
  if(EVENT_NM IS NULL, '', EVENT_NM),
  if(CMPG_ID  IS NULL, '', CMPG_ID),
  if(AD_ID    IS NULL, '', AD_ID),
  if(CRTV_ID  IS NULL, '', CRTV_ID),
  if(TARG_ID  IS NULL, '', TARG_ID),
  if(DEV_MOD  IS NULL, '', DEV_MOD),
  if(DEV_OS   IS NULL, '', DEV_OS),
  if(CHANNEL  IS NULL, '', CHANNEL),



  '', '', '', '', '', '', '',
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
         REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(


         CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(IDFA)',IDFA)
         ELSE DMP_UID
         END


         ,'\n',''),'\,',' '),'\;',' '),'\:',' ') AS DMP_UID,

        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,

        get_json_object(BODY, '$.action') AS ACTION,

        get_json_object(BODY, '$.channel') AS CHANNEL,

        get_json_object(BODY, '$.advertiser_id') AS ADVT_ID,
        get_json_object(BODY, '$.campaign_id')   AS CMPG_ID,
        get_json_object(BODY, '$.ad_id')          AS AD_ID,
        get_json_object(BODY, '$.creative_id')   AS CRTV_ID,
        get_json_object(BODY, '$.targeting_id')  AS TARG_ID,
        get_json_object(BODY, '$.model')          AS DEV_MOD,
        get_json_object(BODY, '$.os')              AS DEV_OS,

        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.event_name') is null, '', get_json_object(BODY, '$.event_name'))),'[\n\r]',' ')) AS EVENT_NM,

        log_time



  FROM dmp.log_server_idsync_collect
  WHERE
  part_hour = '${hivevar:hourbefore}'
  AND
  (
                                                (
                                                       DMP_UID IS NOT NULL
                                                         AND
                                                       SUBSTR(DMP_UID,1,6) = '(DMPC)'
                                                         AND
                                                       DMP_UID <> '(DMPC)00000000-0000-0000-0000-000000000000'
                                                )
                                                OR
                                                (
                                                      (GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID= LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000')
                                                         OR
                                                      (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000')
                                                )
  )
  AND DSID IS NOT NULL AND DSID = '1012'


  )
  a
;



