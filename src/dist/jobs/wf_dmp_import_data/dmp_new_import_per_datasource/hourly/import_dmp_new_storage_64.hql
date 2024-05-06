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


-- SK Planet OCB

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='64')
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

  CASE
      WHEN a.ACTION IS NOT NULL THEN a.ACTION
      ELSE ''
  END AS ACTION,

  CASE
      WHEN a.CHANNEL IS NULL THEN ''
      WHEN a.CHANNEL = 'Android' THEN '1'
      WHEN a.CHANNEL = 'iOS' THEN '2'
      ELSE a.CHANNEL
  END AS CHANNEL,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'eventview' THEN a.PROD_NM
      ELSE ''
  END AS ITEM,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'eventview' THEN a.CATEGORY_CD1
      ELSE ''
  END AS CATEGORY,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN a.ID
      ELSE ''
  END AS KEYWORD,


  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN a.REF
      ELSE ''
  END AS MAG_NAME,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN a.ID
      ELSE ''
  END AS MAG_CONTENT_ID,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN a.PROD_NM
      ELSE ''
  END AS MAG_CONTENT_SUB,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN a.SALES_STATE
      ELSE ''
  END AS MAG_REWARD_TYPE,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'welcome' THEN
                CASE
                     WHEN (a.OCB_SERV = '2' OR a.OCB_SERV like '%,2' OR a.OCB_SERV like '2,%' OR a.OCB_SERV like '%,2,%') THEN 'Y'
                     ELSE 'N'
                END
      ELSE ''
  END AS OCB_LOCK_YN,

  a.GENCLASS,
  a.AGE,

  a.COUPON_ID,
  a.COUPON_NAME,

  '', '', '',
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
        REGEXP_REPLACE(get_json_object(BODY, '$.col005'),'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.col019') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS PROD_NM,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD3,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD4,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD5,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD6,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col002') IS NOT NULL THEN get_json_object(BODY, '$.col002') ELSE '' END),'[\n\r]',' ') AS REF,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col018') IS NOT NULL THEN get_json_object(BODY, '$.col018') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS SALES_STATE,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('search','welcome','eventview','magazineview') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\r]',' ')
            ELSE ''
        END AS ID,

        REFLECT('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(body, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END) AS OCB_SERV,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('view', 'cdown') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\r]',' ')
            ELSE ''
        END AS COUPON_ID,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('view', 'cdown') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\r]',' ')
            ELSE ''
        END AS COUPON_NAME,


        CASE
            WHEN get_json_object(BODY, '$.col005') = 'welcome' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col007')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('0','10','20','30','40','50','60','70','80','90')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col007')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END
            ELSE ''
        END AS AGE,

        CASE
            WHEN get_json_object(BODY, '$.col005') = 'welcome' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col018')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col018')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END
            ELSE ''
        END AS GENCLASS,

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
  AND REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ')='9' AND DSID = 'service_activity_log'

  )
  a;


  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='64')
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

  CASE
      WHEN a.ACTION IS NOT NULL THEN a.ACTION
      ELSE ''
  END AS ACTION,

  CASE
      WHEN a.CHANNEL IS NULL THEN ''
      ELSE a.CHANNEL
  END AS CHANNEL,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'eventview' THEN EVENT_TITLE
      ELSE ''
  END AS ITEM,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'eventview' THEN EVENT_TYPE
      ELSE ''
  END AS CATEGORY,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN KEYWORD
      ELSE ''
  END AS KEYWORD,

  '',

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN MAG_ID
      ELSE ''
  END AS MAG_CONTENT_ID,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN MAG_TITLE
      ELSE ''
  END AS MAG_CONTENT_SUB,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN MAG_TYPE
      ELSE ''
  END AS MAG_REWARD_TYPE,

  '',

  GENCLASS,
  AGE,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('couponview', 'coupondown') THEN COUPON_ID
        ELSE ''
  END AS COUPON_ID,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('couponview', 'coupondown') THEN COUPON_TITLE
        ELSE ''
  END AS COUPON_TITLE,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('couponview', 'coupondown') THEN COUPON_PRICE
        ELSE ''
  END AS COUPON_PRICE,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('couponview', 'coupondown') THEN COUPON_TYPE
        ELSE ''
  END AS COUPON_TYPE,

  MOD_NAME,
  MOD_MANUF,
  IF(TRIM(SERV_NAME) = 'LG U', 'LG U+', SERV_NAME),

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION = 'welcome' THEN POINT
        ELSE ''
  END AS POINT,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('dutumview', 'dutumissue') THEN DUTUM_ID
        ELSE ''
  END AS DUTUM_ID,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('dutumview', 'dutumissue') THEN DUTUM_TITLE
        ELSE ''
  END AS DUTUM_TITLE,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('dutumview', 'dutumissue') THEN DUTUM_PUB
        ELSE ''
  END AS DUTUM_PUB,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('dutumview', 'dutumissue') THEN DUTUM_PRICE
        ELSE ''
  END AS DUTUM_PRICE,

  '', '', '',
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
        REGEXP_REPLACE(get_json_object(BODY, '$.action'),'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.servicechannel') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.eventtitle')     IS NOT NULL THEN get_json_object(BODY, '$.eventtitle')    ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS EVENT_TITLE,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.eventtype')      IS NOT NULL THEN get_json_object(BODY, '$.eventtype')     ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS EVENT_TYPE,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.keyword')        IS NOT NULL THEN get_json_object(BODY, '$.keyword')        ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS KEYWORD,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.magazineid')     IS NOT NULL THEN get_json_object(BODY, '$.magazineid')    ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS MAG_ID,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.magazinetitle') IS NOT NULL THEN get_json_object(BODY, '$.magazinetitle') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS MAG_TITLE,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.magazinetype')  IS NOT NULL THEN get_json_object(BODY, '$.magazinetype')  ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS MAG_TYPE,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.couponid')     IS NOT NULL THEN get_json_object(BODY, '$.couponid')    ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS COUPON_ID,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.coupontitle') IS NOT NULL THEN get_json_object(BODY, '$.coupontitle') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS COUPON_TITLE,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.couponprice') IS NOT NULL THEN get_json_object(BODY, '$.couponprice') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS COUPON_PRICE,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.coupontype')  IS NOT NULL THEN get_json_object(BODY, '$.coupontype')  ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS COUPON_TYPE,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.point')  IS NOT NULL THEN get_json_object(BODY, '$.point')  ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS POINT,

        UPPER(REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.model') is null or get_json_object(BODY, '$.model')                 IN ( '' , '%7B%7BmodelNm%7D%7D'          , '%7B%7Bdevice_model%7D%7D'         ), '', get_json_object(BODY, '$.model')))) AS MOD_NAME,
        UPPER(REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.manufacturer') is null or get_json_object(BODY, '$.manufacturer') IN ( '' , '%7B%7BmanufacturerNm%7D%7D'  , '%7B%7Bdevice_manufacturer%7D%7D' ), '', get_json_object(BODY, '$.manufacturer')))) AS MOD_MANUF,
        UPPER(REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.carrier') is null or get_json_object(BODY, '$.carrier')             IN ( '' , '%7B%7BserviceProvider%7D%7D' , '%7B%7Btelecom_name%7D%7D'         ), '', get_json_object(BODY, '$.carrier')))) AS SERV_NAME,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.dutumid')       IS NOT NULL THEN get_json_object(BODY, '$.dutumid')    ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS DUTUM_ID,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.dutumtitle')   IS NOT NULL THEN get_json_object(BODY, '$.dutumtitle') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS DUTUM_TITLE,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.dutumpub')     IS NOT NULL THEN get_json_object(BODY, '$.dutumpub')    ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS DUTUM_PUB,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.dutumprice')  IS NOT NULL THEN get_json_object(BODY, '$.dutumprice')  ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS DUTUM_PRICE,


        CASE
            WHEN get_json_object(BODY, '$.action') = 'welcome' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.age')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('0','10','20','30','40','50','60','70','80','90')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.age')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END
            ELSE ''
        END AS AGE,

        CASE
            WHEN get_json_object(BODY, '$.action') = 'welcome' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.gender')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.gender')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END
            ELSE ''
        END AS GENCLASS,

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
  AND DSID = '64'

  )
  a;



