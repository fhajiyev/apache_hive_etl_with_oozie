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


-- Shoking Deal

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='77')
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

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('view', 'basket', 'order', 'wish') THEN a.PRD_NAME
        ELSE ''
  END AS PRD_NAME,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('view', 'basket', 'order', 'wish') THEN
        TRIM(
          CONCAT(
           CASE WHEN a.PRD_CAT1 IS NULL OR a.PRD_CAT1 IN (' ','') THEN ''
           ELSE a.PRD_CAT1
           END,
           CASE WHEN a.PRD_CAT2 IS NULL OR a.PRD_CAT2 IN (' ','') THEN ''
           ELSE CONCAT('\||',a.PRD_CAT2)
           END,
           CASE WHEN a.PRD_CAT3 IS NULL OR a.PRD_CAT3 IN (' ','') THEN ''
           ELSE CONCAT('\||',a.PRD_CAT3)
           END
           )
           )
        ELSE ''
  END AS CATEGORY,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('search') THEN a.KEYWRD
        ELSE ''
  END AS KEYWRD,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('view', 'basket', 'order', 'wish') THEN a.PRD_CD
        ELSE ''
  END AS PRD_CD,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('view', 'basket', 'order', 'wish') THEN a.PRD_PRC
        ELSE ''
  END AS PRD_PRC,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('basket', 'order') THEN a.PRD_OPT
        ELSE ''
  END AS PRD_OPT,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('order') THEN a.DLV_ADDR
        ELSE ''
  END AS DLV_ADDR,

  MOD_NAME,
  MOD_MANUF,
  IF(TRIM(SERV_NAME) = 'LG U', 'LG U+', SERV_NAME),

  a.GENCLASS,
  a.AGE,

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

        if(get_json_object(BODY, '$.col001') is null, '', get_json_object(BODY, '$.col001')) AS ACTION,

        get_json_object(BODY, '$.col019') AS CHANNEL,

        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col004') is null, '', get_json_object(BODY, '$.col004'))),'[\n\r]',' ')) AS PRD_NAME,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col009') is null, '', get_json_object(BODY, '$.col009'))),'[\n\r]',' ')) AS PRD_CAT1,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col010') is null, '', get_json_object(BODY, '$.col010'))),'[\n\r]',' ')) AS PRD_CAT2,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col011') is null, '', get_json_object(BODY, '$.col011'))),'[\n\r]',' ')) AS PRD_CAT3,

        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col002') is null, '', get_json_object(BODY, '$.col002'))),'[\n\r]',' ')) AS KEYWRD,

        if(get_json_object(BODY, '$.col003') is null, '', get_json_object(BODY, '$.col003')) AS PRD_CD,
        if(get_json_object(BODY, '$.col014') is null, '', get_json_object(BODY, '$.col014')) AS PRD_PRC,

        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col013') is null, '', get_json_object(BODY, '$.col013'))),'[\n\r]',' ')) AS PRD_OPT,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col016') is null, '', get_json_object(BODY, '$.col016'))),'[\n\r]',' ')) AS DLV_ADDR,

        REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col021') is null or get_json_object(BODY, '$.col021') IN ( '' , '%7B%7BmodelNm%7D%7D'          , '%7B%7Bdevice_model%7D%7D'         ), '', get_json_object(BODY, '$.col021'))) AS MOD_NAME,
        REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col022') is null or get_json_object(BODY, '$.col022') IN ( '' , '%7B%7BmanufacturerNm%7D%7D'  , '%7B%7Bdevice_manufacturer%7D%7D' ), '', get_json_object(BODY, '$.col022'))) AS MOD_MANUF,
        REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col023') is null or get_json_object(BODY, '$.col023') IN ( '' , '%7B%7BserviceProvider%7D%7D' , '%7B%7Btelecom_name%7D%7D'         ), '', get_json_object(BODY, '$.col023'))) AS SERV_NAME,


                    CASE
                        WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col018')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('0','10','20','30','40','50','60','70','80','90')
                        THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col018')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                        ELSE ''
                    END AS AGE,

                    CASE
                       WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col017')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
                       THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col017')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
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
  AND DSID IS NOT NULL AND DSID = '77'


  )
  a
;




