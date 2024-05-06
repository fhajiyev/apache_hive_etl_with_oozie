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


-- Nate Service

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='82')
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
      WHEN COL001 <> '' THEN 'search'
      WHEN COL002 <> '' THEN 'tabvisit'
      WHEN COL003 <> '' THEN 'news'
      WHEN COL007 <> '' THEN 'pann'
      ELSE ''
  END AS ACTION,

  if(CHANNEL IS NULL, '', CHANNEL),

  DEVMOD_NM,

  COL001,

  COL002,
 
  COL003,
 

  TRIM(
        CONCAT(
         CASE WHEN a.NEWS_CAT1 IS NULL OR TRIM(a.NEWS_CAT1) IN ('') THEN ''
         ELSE TRIM(a.NEWS_CAT1)
         END,
         CASE WHEN a.NEWS_CAT2 IS NULL OR TRIM(a.NEWS_CAT2) IN ('') THEN ''
         ELSE CONCAT('\||',TRIM(a.NEWS_CAT2))
         END,
         CASE WHEN a.NEWS_CAT3 IS NULL OR TRIM(a.NEWS_CAT3) IN ('') THEN ''
         ELSE CONCAT('\||',TRIM(a.NEWS_CAT3))
         END
         )
         ),


  COL007,


  TRIM(
        CONCAT(
         CASE WHEN a.MAG_CAT1 IS NULL OR TRIM(a.MAG_CAT1) IN ('') THEN ''
         ELSE TRIM(a.MAG_CAT1)
         END,
         CASE WHEN a.MAG_CAT2 IS NULL OR TRIM(a.MAG_CAT2) IN ('') THEN ''
         ELSE CONCAT('\||',TRIM(a.MAG_CAT2))
         END,
         CASE WHEN a.MAG_CAT3 IS NULL OR TRIM(a.MAG_CAT3) IN ('') THEN ''
         ELSE CONCAT('\||',TRIM(a.MAG_CAT3))
         END
         )
         ),

  REGEXP_REPLACE(COL001, ' ', ''),



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

        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode',if(get_json_object(BODY, '$.col001') is null, '', get_json_object(BODY, '$.col001'))),'[\n\r]',' ')) AS COL001,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode',if(get_json_object(BODY, '$.col002') is null, '', get_json_object(BODY, '$.col002'))),'[\n\r]',' ')) AS COL002,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode',if(get_json_object(BODY, '$.col003') is null, '', get_json_object(BODY, '$.col003'))),'[\n\r]',' ')) AS COL003,
        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode',if(get_json_object(BODY, '$.col007') is null, '', get_json_object(BODY, '$.col007'))),'[\n\r]',' ')) AS COL007,

        get_json_object(BODY, '$.col011') AS CHANNEL,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col012') is null, '', get_json_object(BODY, '$.col012'))) AS DEVMOD_NM,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col004') is null, '', get_json_object(BODY, '$.col004'))) AS NEWS_CAT1,
        
        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col005') is null, '', get_json_object(BODY, '$.col005'))) AS NEWS_CAT2,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col006') is null, '', get_json_object(BODY, '$.col006'))) AS NEWS_CAT3,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col008') is null, '', get_json_object(BODY, '$.col008'))) AS MAG_CAT1,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col009') is null, '', get_json_object(BODY, '$.col009'))) AS MAG_CAT2,

        reflect('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col010') is null, '', get_json_object(BODY, '$.col010'))) AS MAG_CAT3,

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
  AND DSID IS NOT NULL AND DSID = '82' 
  

  )
  a
  WHERE ((COL001 <> '')OR(COL002 <> '')OR(COL003 <> '')OR(COL007 <> ''));




