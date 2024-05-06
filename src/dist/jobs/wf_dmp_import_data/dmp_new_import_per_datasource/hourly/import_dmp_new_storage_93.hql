USE dmp;


set hivevar:hour10before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


-- 11st TUNE

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hour10before}', data_source_id='93')
  SELECT DISTINCT
  REGEXP_REPLACE(DMP_ID, '\001', '') as uid,
  REGEXP_REPLACE(REQUEST_DATE, '\001', ''),
  REGEXP_REPLACE(REQUEST_TIME, '\001', ''),
  REGEXP_REPLACE(
  CASE
        WHEN WEEKDAY = '1' THEN 'Mon'
        WHEN WEEKDAY = '2' THEN 'Tue'
        WHEN WEEKDAY = '3' THEN 'Wed'
        WHEN WEEKDAY = '4' THEN 'Thu'
        WHEN WEEKDAY = '5' THEN 'Fri'
        WHEN WEEKDAY = '6' THEN 'Sat'
        WHEN WEEKDAY = '7' THEN 'Sun'
  END,
  '\001', ''),

  REGEXP_REPLACE(ACTION, '\001', ''),

  REGEXP_REPLACE(CAMP_NM, '\001', ''),
  REGEXP_REPLACE(IF(TRIM(PUBL_NM) = '', 'organic', PUBL_NM), '\001', ''),
  REGEXP_REPLACE(ADV_SUB_SITE_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUB_CAMP_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUB_ADGR_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUB_ADVR_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUB_PUBL_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUB_KEYW_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUB_PLCM_NM, '\001', ''),
  REGEXP_REPLACE(EXST_USER, '\001', ''),
  REGEXP_REPLACE(DEV_MOD, '\001', ''),
  REGEXP_REPLACE(CARRIER_NM, '\001', ''),
  REGEXP_REPLACE(REVENUE, '\001', ''),
  REGEXP_REPLACE(PUBL_SUB_CAMP_NM, '\001', ''),
  REGEXP_REPLACE(DEV_BRAND, '\001', ''),
  REGEXP_REPLACE(OS_VERSION, '\001', ''),

  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '${hivevar:hour10before}',
  log_time

  FROM
  (
  SELECT

        CASE
            WHEN UIDTYPE = 'GAID' THEN CONCAT('(GAID)', UID)
            ELSE CONCAT('(IDFA)', UPPER(UID))
        END
        AS DMP_ID,

        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,

        CASE
            WHEN LSID = '1000' THEN 'install'
            WHEN LSID = '1001' THEN 'open'
            WHEN LSID = '1005' THEN 'purchase'
            WHEN LSID = '1006' THEN 'rated'
            WHEN LSID = '1007' THEN 'search'
            WHEN LSID = '1008' THEN 'add_to_wishlist'
            WHEN LSID = '1009' THEN 'share'
            WHEN LSID = '1010' THEN 'add_to_cart'
            WHEN LSID = '1011' THEN 'content_view'
        END
        AS ACTION,

        if(COL001 IS NULL, '', COL001) AS CAMP_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL002 IS NULL, '', COL002)
           ELSE if(COL003 IS NULL, '', COL003)
        END
        AS PUBL_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL012 IS NULL, '', COL012)
           ELSE if(COL027 IS NULL, '', COL027)
        END
        AS ADV_SUB_SITE_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL008 IS NULL, '', COL008)
           ELSE if(COL018 IS NULL, '', COL018)
        END
        AS ADV_SUB_CAMP_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL007 IS NULL, '', COL007)
           ELSE if(COL012 IS NULL, '', COL012)
        END
        AS ADV_SUB_ADGR_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL006 IS NULL, '', COL006)
           ELSE if(COL009 IS NULL, '', COL009)
        END
        AS ADV_SUB_ADVR_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL009 IS NULL, '', COL009)
           ELSE if(COL015 IS NULL, '', COL015)
        END
        AS ADV_SUB_PUBL_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL010 IS NULL, '', COL010)
           ELSE if(COL021 IS NULL, '', COL021)
        END
        AS ADV_SUB_KEYW_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL011 IS NULL, '', COL011)
           ELSE if(COL024 IS NULL, '', COL024)
        END
        AS ADV_SUB_PLCM_NM,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL003 IS NULL, '', COL003)
           ELSE ''
        END
        AS EXST_USER,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL005 IS NULL, '', COL005)
           ELSE ''
        END
        AS DEV_MOD,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL004 IS NULL, '', COL004)
           ELSE ''
        END
        AS CARRIER_NM,

        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL015 IS NULL, '', COL015)
           ELSE ''
        END
        AS DEV_BRAND,

        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL017 IS NULL, '', COL017)
           ELSE ''
        END
        AS OS_VERSION,

        CASE
           WHEN LSID = '1005' THEN if(COL008 IS NULL, '', COL008)
           ELSE ''
        END
        AS REVENUE,


        CASE
           WHEN LSID IN ('1000','1001') THEN if(COL013 IS NULL, '', COL013)
           ELSE if(COL030 IS NULL, '', COL030)
        END
        AS PUBL_SUB_CAMP_NM,

        log_time,
        part_hour

  FROM dmp.log_server_bulk_collect_sftp
  WHERE
  part_hour = '${hivevar:hour10before}'
  AND UID IS NOT NULL AND UID <> ''
  AND UIDTYPE IS NOT NULL AND UIDTYPE IN ('GAID', 'IDFA')
  AND LSID IS NOT NULL AND LSID IN ('1000','1001', '1005', '1006', '1007', '1008', '1009', '1010', '1011')


  )
  a

;



