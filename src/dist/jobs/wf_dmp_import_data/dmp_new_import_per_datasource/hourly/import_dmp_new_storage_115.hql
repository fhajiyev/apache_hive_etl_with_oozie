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


-- SKN - 비스타워커힐

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='115')
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
  if(ACTION IN ('search','reservation'), MEMBERS, ''),
  if(ACTION IN ('search','reservation'), ROOM_CNT, ''),
  if(ACTION IN ('search','reservation'), CHECKIN_DT, ''),
  if(ACTION IN ('search','reservation'), CHECKOUT_DT, ''),
  if(ACTION = 'search', SEARCH_TYPE, ''),
  if(ACTION = 'reservation', ROOMS, ''),
  if(ACTION = 'reservation', PACKAGE_NM, ''),
  if(ACTION = 'reservation', PACKAGE_TAGNM, ''),
  if(ACTION = 'reservation', OPTN, ''),


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




        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.action') is null, '', get_json_object(BODY, '$.action'))) AS ACTION,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.page_category') is null, '', get_json_object(BODY, '$.page_category'))) AS PAGE_CAT,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.page_name') is null, '', get_json_object(BODY, '$.page_name'))) AS PAGE_NM,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.members') is null, '', get_json_object(BODY, '$.members'))) AS MEMBERS,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.roomcount') is null, '', get_json_object(BODY, '$.roomcount'))) AS ROOM_CNT,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.checkindate') is null, '', get_json_object(BODY, '$.checkindate'))), '\\.', '') AS CHECKIN_DT,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.checkoutdate') is null, '', get_json_object(BODY, '$.checkoutdate'))), '\\.', '') AS CHECKOUT_DT,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.searchtype') is null, '', get_json_object(BODY, '$.searchtype'))) AS SEARCH_TYPE,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.rooms') is null, '', get_json_object(BODY, '$.rooms'))) AS ROOMS,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.package_name') is null, '', get_json_object(BODY, '$.package_name'))) AS PACKAGE_NM,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.package_tagname') is null, '', get_json_object(BODY, '$.package_tagname'))) AS PACKAGE_TAGNM,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.option') is null, '', get_json_object(BODY, '$.option'))) AS OPTN,

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
  AND DSID IS NOT NULL AND DSID = '1026'


  )
  a
  WHERE ACTION <> ''
  ;




