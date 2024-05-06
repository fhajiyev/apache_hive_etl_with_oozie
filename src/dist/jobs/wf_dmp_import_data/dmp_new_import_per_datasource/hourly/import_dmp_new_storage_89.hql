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


-- OCB Lock

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='89')
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

  ACTION,

  if(CHANNEL IS NULL, '', CHANNEL),

  if(ACTION NOT IN ('swipeleft'), '', COL002),
  if(ACTION NOT IN ('swipeleft', 'feedclick'), '', COL003),
  if(ACTION NOT IN ('swipeleft', 'feedclick'), '', COL004),
  if(ACTION NOT IN ('swipeleft', 'feedclick'), '', COL005),

  if(COL006 IS NULL, '', COL006),

  if(COL007 IS NULL, '', COL007),

  if(COL008 IS NULL, '', COL008),

  COL010,

  COL011,

  COL012,

  '', '', '', '', '',
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

        if(get_json_object(BODY, '$.col001') is null, '', get_json_object(BODY, '$.col001')) AS ACTION,

        get_json_object(BODY, '$.col009') AS CHANNEL,

        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col002') is null, '', get_json_object(BODY, '$.col002'))),'[\n\r]',' ')) AS COL002,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col003') is null, '', get_json_object(BODY, '$.col003'))),'[\n\r]',' ')) AS COL003,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col004') is null, '', get_json_object(BODY, '$.col004'))),'[\n\r]',' ')) AS COL004,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col005') is null, '', get_json_object(BODY, '$.col005'))),'[\n\r]',' ')) AS COL005,

        get_json_object(BODY, '$.col006') AS COL006,
        get_json_object(BODY, '$.col007') AS COL007,
        get_json_object(BODY, '$.col008') AS COL008,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col010') is null, '', get_json_object(BODY, '$.col010'))),'[\n\r]',' ')) AS COL010,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col011') is null, '', get_json_object(BODY, '$.col011'))),'[\n\r]',' ')) AS COL011,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col012') is null, '', get_json_object(BODY, '$.col012'))),'[\n\r]',' ')) AS COL012,

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
  AND DSID IS NOT NULL AND DSID = '89'


  )
  a
;




