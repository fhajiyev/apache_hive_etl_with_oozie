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


-- SK Planet SyrupWallet

--  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='65')
--  SELECT
--  a.DMP_UID as uid,
--  a.REQUEST_DATE,
--  a.REQUEST_TIME,
--  CASE
--        WHEN a.WEEKDAY = '1' THEN 'Mon'
--        WHEN a.WEEKDAY = '2' THEN 'Tue'
--        WHEN a.WEEKDAY = '3' THEN 'Wed'
--        WHEN a.WEEKDAY = '4' THEN 'Thu'
--        WHEN a.WEEKDAY = '5' THEN 'Fri'
--        WHEN a.WEEKDAY = '6' THEN 'Sat'
--        WHEN a.WEEKDAY = '7' THEN 'Sun'
--  END,
--
--  CASE
--      WHEN a.ACTION IS NOT NULL THEN a.ACTION
--      ELSE ''
--  END AS ACTION,
--
--  CASE
--      WHEN a.CHANNEL IS NOT NULL THEN a.CHANNEL
--      ELSE ''
--  END AS CHANNEL,
--
--  CASE
--      WHEN a.ACTION IS NULL THEN ''
--      ELSE a.PROD_NM
--  END AS ITEM,
--
--  '' AS CATEGORY,
--
--  CASE
--        WHEN a.CATEGORY_CD1 IS NULL THEN ''
--        ELSE a.CATEGORY_CD1
--  END AS BIZCATEGORY,
--
--  CASE
--      WHEN a.ACTION IS NULL THEN ''
--      ELSE
--      TRIM(
--        CONCAT(
--         CASE WHEN a.CATEGORY_CD2 IS NULL OR TRIM(a.CATEGORY_CD2) IN ('') THEN ''
--         ELSE TRIM(SUBSTR(a.CATEGORY_CD2,(INSTR(a.CATEGORY_CD2, ':')+1)))
--         END,
--         CASE WHEN a.CATEGORY_CD3 IS NULL OR TRIM(a.CATEGORY_CD3) IN ('') THEN ''
--         ELSE CONCAT('\||',TRIM(SUBSTR(a.CATEGORY_CD3,(INSTR(a.CATEGORY_CD3, ':')+1))))
--         END
--         )
--         )
--  END AS POICATEGORY,
--
--  CASE
--      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN a.ID
--      ELSE ''
--  END AS KEYWORD,
--
--  '',
--  '',
--
--  a.GENCLASS,
--  a.AGE,
--
--  '', '', '', '', '', '',
--  '', '', '', '', '', '', '', '', '', '',
--  '', '', '', '', '', '', '', '', '', '',
--  '', '', '', '', '', '', '', '', '', '',
--  '', '', '', '', '', '', '', '', '', '',
--  '', '', '', '', '', '', '', '', '', '',
--  '', '', '', '', '', '', '', '', '', '',
--  '', '', '', '', '', '', '', '', '', '',
--  '', '', '', '', '', '', '', '', '', '',
--  '${hivevar:hourbefore}',
--  log_time
--
--  FROM
--  (
--  SELECT
--         REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
--
--
--         CASE
--         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(GAID)',GAID)
--         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(IDFA)',IDFA)
--         ELSE DMP_UID
--         END
--
--
--         ,'\n',''),'\,',' '),'\;',' '),'\:',' ') AS DMP_UID,
--
--        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
--        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
--        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,
--        REGEXP_REPLACE(get_json_object(BODY, '$.col005'),'[\n\,\;\:\r+]',' ') AS ACTION,
--        get_json_object(BODY, '$.col019') AS CHANNEL,
--
--        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS PROD_NM,
--
--        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD1,
--        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD2,
--        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD3,
--        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD4,
--        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD5,
--        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD6,
--
--        CASE
--            WHEN get_json_object(BODY, '$.col005') IN ('search','welcome','sdest','rdest','fdest') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\r]',' ')
--            ELSE ''
--        END AS ID,
--
--
--
--        CASE
--            WHEN get_json_object(BODY, '$.col005') = 'welcome' THEN
--            CASE
--                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col007')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('0','10','20','30','40','50','60','70','80','90')
--                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col007')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
--                ELSE ''
--            END
--            ELSE ''
--        END AS AGE,
--
--        CASE
--            WHEN get_json_object(BODY, '$.col005') = 'welcome' THEN
--            CASE
--                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col018')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
--                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col018')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
--                ELSE ''
--            END
--            ELSE ''
--        END AS GENCLASS,
--
--        log_time
--
--
--
--
--
--
--  FROM dmp.log_server_idsync_collect
--  WHERE
--  part_hour = '${hivevar:hourbefore}'
--  AND
--  (
--                    (
--                           DMP_UID IS NOT NULL
--                             AND
--                           SUBSTR(DMP_UID,1,6) = '(DMPC)'
--                             AND
--                           DMP_UID <> '(DMPC)00000000-0000-0000-0000-000000000000'
--                    )
--                    OR
--                    (
--                          (GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID= LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000')
--                             OR
--                          (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000')
--                    )
--  )
--  AND REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ')='10' AND DSID = 'service_activity_log'
--
--  )
--  a




  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='65')
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

  a.ACTION,

  IF(a.CHANNEL IS NULL, '', a.CHANNEL) AS CHANNEL,

  CASE
      WHEN TRIM(a.ACTION) IN ('membershipview', 'membershipdown') THEN MBSH_NM
      WHEN TRIM(a.ACTION) IN ('couponview',      'coupondown'    ) THEN COUP_NM
      WHEN TRIM(a.ACTION) IN ('feedclick')                           THEN FEED_NM
      ELSE''
  END AS ITEM_NM,

  '' AS ITEM_CAT,


  TRIM
  (
          CONCAT
          (
           CASE WHEN a.BIZCAT_CD1 IS NULL OR TRIM(a.BIZCAT_CD1) IN ('') THEN ''
           ELSE TRIM(a.BIZCAT_CD1)
           END,
           CASE WHEN a.BIZCAT_CD2 IS NULL OR TRIM(a.BIZCAT_CD2) IN ('') THEN ''
           ELSE CONCAT('\||', TRIM(a.BIZCAT_CD2))
           END,
           CASE WHEN a.BIZCAT_CD3 IS NULL OR TRIM(a.BIZCAT_CD3) IN ('') THEN ''
           ELSE CONCAT('\||', TRIM(a.BIZCAT_CD3))
           END
          )
  ) AS BIZCATEGORY,


  TRIM
  (
          CONCAT
          (
           CASE WHEN a.POICAT_CD1 IS NULL OR TRIM(a.POICAT_CD1) IN ('') THEN ''
           ELSE TRIM(a.POICAT_CD1)
           END,
           CASE WHEN a.POICAT_CD2 IS NULL OR TRIM(a.POICAT_CD2) IN ('') THEN ''
           ELSE CONCAT('\||', TRIM(a.POICAT_CD2))
           END,
           CASE WHEN a.POICAT_CD3 IS NULL OR TRIM(a.POICAT_CD3) IN ('') THEN ''
           ELSE CONCAT('\||', TRIM(a.POICAT_CD3))
           END
          )
  ) AS POICATEGORY,

  CASE
      WHEN TRIM(a.ACTION) = 'search' THEN a.ID
      ELSE ''
  END AS KEYWORD,

  a.DEVC_MOD,
  IF(a.SERV_PRO IS NULL, '', a.SERV_PRO) AS SERVICE_PROV,

  a.GENCLASS,
  a.AGE,

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
        REGEXP_REPLACE(CASE WHEN get_json_object(BODY, '$.col001') IS NOT NULL THEN get_json_object(BODY, '$.col001') ELSE '' END,'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.col021') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col004') IS NOT NULL THEN get_json_object(BODY, '$.col004') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS MBSH_NM,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col006') IS NOT NULL THEN get_json_object(BODY, '$.col006') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS COUP_NM,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col017') IS NOT NULL THEN get_json_object(BODY, '$.col017') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS FEED_NM,


        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS BIZCAT_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS BIZCAT_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS BIZCAT_CD3,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS POICAT_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col008') IS NOT NULL THEN get_json_object(BODY, '$.col008') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS POICAT_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS POICAT_CD3,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col002') IS NOT NULL THEN get_json_object(BODY, '$.col002') ELSE '' END),'[\n\r]',' ') ID,

        reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col022') IS NOT NULL THEN get_json_object(BODY, '$.col022') ELSE '' END) DEVC_MOD,
        get_json_object(BODY, '$.col024') SERV_PRO,


        CASE
            WHEN get_json_object(BODY, '$.col001') = 'welcome' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col020')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('0','10','20','30','40','50','60','70','80','90')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col020')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END
            ELSE ''
        END AS AGE,

        CASE
            WHEN get_json_object(BODY, '$.col001') = 'welcome' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col019')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col019')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
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
  AND DSID = '65'

  )
  a;



