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


-- SK Broadband Oksusu

INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='71')
select

  CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(IDFA)',IDFA)
         ELSE DMP_UID
  END AS UID,

  SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
  SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
  CASE
        WHEN from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') = '1' THEN 'Mon'
        WHEN from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') = '2' THEN 'Tue'
        WHEN from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') = '3' THEN 'Wed'
        WHEN from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') = '4' THEN 'Thu'
        WHEN from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') = '5' THEN 'Fri'
        WHEN from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') = '6' THEN 'Sat'
        WHEN from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') = '7' THEN 'Sun'
  END,

  case
        when get_json_object(body, '$.col001') is null then ''
        else trim(get_json_object(body, '$.col001'))
  end AS ACTION,
  case
       when get_json_object(body, '$.col002') is null then ''
       else trim(get_json_object(body, '$.col002'))
  end AS PLACEMENT,
  case
       when get_json_object(body, '$.col003') is null then ''
       when reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col003')) is not null then TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col003')))
       else TRIM(get_json_object(body, '$.col003'))
  end AS CONTENT,
  case
       when get_json_object(body, '$.col004') is null then ''
       when reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col004')) is not null then TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col004')))
       else TRIM(get_json_object(body, '$.col004'))
  end AS CONTENT_GENRE,
  case
       when get_json_object(body, '$.col005') is null then ''
       else trim(get_json_object(body, '$.col005'))
  end AS CONTENT_PURCHASED,
  case
       when get_json_object(body, '$.col006') is null then ''
       else trim(get_json_object(body, '$.col006'))
  end AS CONTENT_CHARGED,
  case
       when get_json_object(body, '$.col007') is null then ''
       when reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col007')) is not null then TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col007')))
       else TRIM(get_json_object(body, '$.col007'))
  end AS CHANNEL,
  case
       when get_json_object(body, '$.col008') is null then ''
       when reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col008')) is not null then TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col008')))
       else TRIM(get_json_object(body, '$.col008'))
  end AS CHANNEL_GENRE,
  case
       when get_json_object(body, '$.col009') is null then ''
       when reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col009')) is not null then TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(body, '$.col009')))
       else TRIM(get_json_object(body, '$.col009'))
  end AS PROGRAM,
  case
       when get_json_object(body, '$.col010') is null then ''
       else TRIM(get_json_object(body, '$.col010'))
  end AS VIEWTIME,

                    CASE
                       WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col011')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
                       THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col011')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                       ELSE ''
                   END AS GENCLASS,

                   CASE
                       WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col012')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('0','10','20','30','40','50','60','70','80','90')
                       THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col012')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                       ELSE ''
                   END AS AGE,

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

from
DMP.LOG_SERVER_IDSYNC_COLLECT
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
AND DSID IS NOT NULL AND DSID = 'dmp-53'
;




