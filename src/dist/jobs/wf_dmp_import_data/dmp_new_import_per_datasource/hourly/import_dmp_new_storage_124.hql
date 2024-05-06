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


-- NEPA몰

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='124')
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
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN a.KEYWORD
      ELSE ''
  END AS KEYWORD,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION NOT IN ('product_view', 'basket', 'order','category_view') THEN ''
      ELSE
      TRIM(
        CONCAT(
         CASE WHEN a.PROD_CAT1 IS NULL OR TRIM(a.PROD_CAT1) IN ('', 'null') THEN ''
         ELSE TRIM(a.PROD_CAT1)
         END,
         CASE WHEN a.PROD_CAT2 IS NULL OR TRIM(a.PROD_CAT2) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(a.PROD_CAT2))
         END,
         CASE WHEN a.PROD_CAT3 IS NULL OR TRIM(a.PROD_CAT3) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(a.PROD_CAT3))
         END
         )
         )
  END AS CATEGORY,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('product_view', 'basket', 'order','category_view') THEN a.PROD_CAT1
      ELSE ''
  END AS PROD_CAT1,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('product_view', 'basket', 'order','category_view') THEN a.PROD_CAT2
      ELSE ''
  END AS PROD_CAT2,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('product_view', 'basket', 'order','category_view') THEN a.PROD_CAT3
      ELSE ''
  END AS PROD_CAT3,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('product_view', 'basket', 'order') THEN a.PROD_NM
      ELSE ''
  END AS PROD_NM,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('product_view', 'basket', 'order') THEN a.PROD_PRICE
      ELSE ''
  END AS PROD_PRICE,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'page_view' THEN a.PAGE_NM
      ELSE ''
  END AS PAGE_NM,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'page_view' THEN a.PAGE_NM_DTL
      ELSE ''
  END AS PAGE_NM_DTL,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'store_search' THEN a.STORE_NM
      ELSE ''
  END AS STORE_NM,

  a.GENCLASS,
  a.AGE,

  CASE
        WHEN a.CHANNEL IS NOT NULL THEN a.CHANNEL
        ELSE ''
  END AS CHANNEL,

  a.CARRIER,
  a.DEV_MOD,
  a.DEV_MANUF,
  a.DEV_OS,



  '', '', '', '', '', '', '', '', '',
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
        get_json_object(BODY, '$.channel') AS CHANNEL,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.keyword') IS NOT NULL THEN get_json_object(BODY, '$.keyword') ELSE '' END),'[\n\r]',' ') AS KEYWORD,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.product_category_c1') IS NOT NULL THEN get_json_object(BODY, '$.product_category_c1') ELSE '' END),'[\n\r]',' ') AS PROD_CAT1,
        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.product_category_c2') IS NOT NULL THEN get_json_object(BODY, '$.product_category_c2') ELSE '' END),'[\n\r]',' ') AS PROD_CAT2,
        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.product_category_c3') IS NOT NULL THEN get_json_object(BODY, '$.product_category_c3') ELSE '' END),'[\n\r]',' ') AS PROD_CAT3,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.product_name')  IS NOT NULL THEN get_json_object(BODY, '$.product_name')  ELSE '' END),'[\n\r]',' ') AS PROD_NM,
        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.product_price') IS NOT NULL THEN get_json_object(BODY, '$.product_price') ELSE '' END),'[\n\r]',' ') AS PROD_PRICE,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.page_name') IS NOT NULL THEN get_json_object(BODY, '$.page_name') ELSE '' END),'[\n\r]',' ') AS PAGE_NM,
        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.page_name_detailed') IS NOT NULL THEN get_json_object(BODY, '$.page_name_detailed') ELSE '' END),'[\n\r]',' ') AS PAGE_NM_DTL,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.store_name') IS NOT NULL THEN get_json_object(BODY, '$.store_name') ELSE '' END),'[\n\r]',' ') AS STORE_NM,

            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.age')),'\n',''),'\,',' '),'\;',' '),'\:',' ') NOT IN ('-1')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.age')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END AS AGE,

            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.gender')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.gender')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END AS GENCLASS,

        REGEXP_REPLACE(CASE WHEN get_json_object(BODY, '$.carrier')      IS NOT NULL THEN get_json_object(BODY, '$.carrier')     ELSE '' END,'[\n\r]',' ') AS CARRIER,
        REGEXP_REPLACE(CASE WHEN get_json_object(BODY, '$.model')        IS NOT NULL THEN get_json_object(BODY, '$.model')        ELSE '' END,'[\n\r]',' ') AS DEV_MOD,
        REGEXP_REPLACE(CASE WHEN get_json_object(BODY, '$.manufacture') IS NOT NULL THEN get_json_object(BODY, '$.manufacture') ELSE '' END,'[\n\r]',' ') AS DEV_MANUF,
        REGEXP_REPLACE(CASE WHEN get_json_object(BODY, '$.os')            IS NOT NULL THEN get_json_object(BODY, '$.os')           ELSE '' END,'[\n\r]',' ') AS DEV_OS,


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
  AND DSID = '1041'

  )
  a;

