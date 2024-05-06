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


-- SKN - 다락휴

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='116')
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

  if(ACTION = 'view', PAGE_CAT, ''),
  if(ACTION = 'view', PAGE_NM,  ''),
  BRANCH,
  if(ACTION = 'reservation', MEMBERS, ''),
  if(ACTION IN ('search','reservation'), CHECKIN_DT, ''),
  if(ACTION IN ('search','reservation'), CHECKOUT_DT, ''),
  if(ACTION IN ('search','reservation'), STAY_TM, ''),
  if(ACTION = 'search', EXTR_TM, ''),
  if(ACTION = 'reservation', ROOMS, ''),
  if(ACTION = 'reservation', ROOMS_TYPE, ''),


  '', '', '', '', '', '',
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




        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.action') is null, '', get_json_object(BODY, '$.action'))) AS ACTION,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.page_category') is null, '', get_json_object(BODY, '$.page_category'))) AS PAGE_CAT,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.page_name') is null, '', get_json_object(BODY, '$.page_name'))) AS PAGE_NM,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.branch') is null, '', get_json_object(BODY, '$.branch'))) AS BRANCH,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.members') is null, '', get_json_object(BODY, '$.members'))) AS MEMBERS,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.checkindate') is null, '', get_json_object(BODY, '$.checkindate'))), '\\.', '') AS CHECKIN_DT,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.checkoutdate') is null, '', get_json_object(BODY, '$.checkoutdate'))), '\\.', '') AS CHECKOUT_DT,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.staytime') is null, '', get_json_object(BODY, '$.staytime'))), '\\.', '') AS STAY_TM,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.extratime') is null, '', get_json_object(BODY, '$.extratime'))), '\\.', '') AS EXTR_TM,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.rooms') is null, '', get_json_object(BODY, '$.rooms'))) AS ROOMS,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.rooms_type') is null, '', get_json_object(BODY, '$.rooms_type'))) AS ROOMS_TYPE,

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
  AND DSID IS NOT NULL AND DSID = '1027'


  )
  a
  WHERE ACTION <> ''
  ;




