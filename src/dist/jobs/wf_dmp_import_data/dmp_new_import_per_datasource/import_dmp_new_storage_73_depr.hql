USE dmp;


set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day90before;
set hivevar:day91before;



set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

-- Yuhan Kimberly momQ

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daybefore}00', data_source_id='73')
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
      WHEN a.CHANNEL IS NOT NULL THEN a.CHANNEL
      ELSE ''
  END AS CHANNEL,

  CASE
        WHEN a.ACTION IS NULL THEN ''
        ELSE REGEXP_REPLACE(a.PROD_NM,'\\^',' ')
    END AS ITEM,

  CASE
        WHEN a.ACTION IS NULL THEN ''
        ELSE
        TRIM(
          CONCAT(
           CASE WHEN a.CATEGORY_CD1 IN (' ','') OR a.CATEGORY_CD1 IS NULL THEN ''
           ELSE SUBSTR(a.CATEGORY_CD1,(INSTR(a.CATEGORY_CD1, ':')+1))
           END,
           CASE WHEN a.CATEGORY_CD2 IN (' ','') OR a.CATEGORY_CD2 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(a.CATEGORY_CD2,(INSTR(a.CATEGORY_CD2, ':')+1)))
           END,
           CASE WHEN a.CATEGORY_CD3 IN (' ','') OR a.CATEGORY_CD3 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(a.CATEGORY_CD3,(INSTR(a.CATEGORY_CD3, ':')+1)))
           END
           )
           )
    END AS CATEGORY,

  CASE
      WHEN a.CATEGORY_CD1 IS NULL THEN ''
      ELSE a.CATEGORY_CD1
  END AS CATEGORY1,

  CASE
      WHEN a.CATEGORY_CD2 IS NULL THEN ''
      ELSE a.CATEGORY_CD2
  END AS CATEGORY2,

  CASE
      WHEN a.CATEGORY_CD3 IS NULL THEN ''
      ELSE a.CATEGORY_CD3
  END AS CATEGORY3,

  a.TOTAL_SALES,

  a.PROFIT,

  a.AMOUNT,

  CASE
        WHEN a.CAMPAIGN_CD IS NULL THEN ''
        ELSE a.CAMPAIGN_CD
  END AS CAMPAIGN_CODE,

  CASE
        WHEN a.REF IS NULL THEN ''
        ELSE a.REF
  END AS REFERRER_URL,


  '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '${hivevar:daybefore}00',
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
        REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.col014') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col002') IS NOT NULL THEN get_json_object(BODY, '$.col002') ELSE '' END),'[\n\,\;\:\r+]',' '), '\\\\\\/','\\/') AS PROD_NM,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col003') IS NOT NULL THEN get_json_object(BODY, '$.col003') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col004') IS NOT NULL THEN get_json_object(BODY, '$.col004') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col005') IS NOT NULL THEN get_json_object(BODY, '$.col005') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD3,

        get_json_object(BODY, '$.col006') AS TOTAL_SALES,
        get_json_object(BODY, '$.col007') AS PROFIT,
        get_json_object(BODY, '$.col008') AS AMOUNT,
        get_json_object(BODY, '$.col009') AS CAMPAIGN_CD,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\,\;\:\r+]',' '), '\\\\\\/','\\/') AS REF,

        log_time


  FROM dmp.log_server_idsync_collect
  WHERE
  part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
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
  AND DSID IS NOT NULL AND DSID = '73'

  )
  a;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='73', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');
  ALTER TABLE dmp.prod_data_source_store_parquet DROP IF EXISTS PARTITION (data_source_id='73', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');
