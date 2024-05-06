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


-- Syrup TUNE

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hour10before}', data_source_id='111')
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
  REGEXP_REPLACE(PUBL_NM, '\001', ''),
  REGEXP_REPLACE(SITE_EV_NM, '\001', ''),
  REGEXP_REPLACE(STATUS, '\001', ''),
  REGEXP_REPLACE(PUBL_SUB_CAMP_NM, '\001', ''),
  REGEXP_REPLACE(PUBL_SUB_SITE_NM, '\001', ''),
  REGEXP_REPLACE(PUBL_SUB_PUBL_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUB_CAMP_NM, '\001', ''),

  REGEXP_REPLACE(DEV_MODEL, '\001', ''),
  REGEXP_REPLACE(DEV_BRAND, '\001', ''),
  REGEXP_REPLACE(DEV_CARRIER, '\001', ''),
  REGEXP_REPLACE(OS_VERSION, '\001', ''),

  '', '', '', '', '',
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
            WHEN LSID = '1023' THEN 'install'
            WHEN LSID = '1022' THEN 'open'
            WHEN LSID = '1031' THEN 'reinstall'
            WHEN LSID = '1032' THEN 'authentication'
            WHEN LSID = '1033' THEN 'issue_syrupcard'
            WHEN LSID = '1034' THEN 'issue_coupon'
            WHEN LSID = '1035' THEN 'issue_dutum'
        END
        AS ACTION,

        if(COL002 IS NULL, '', COL002) AS PUBL_NM,

        if(COL003 IS NULL, '', COL003) AS SITE_EV_NM,

        if(COL004 IS NULL, '', COL004) AS STATUS,

        if(COL007 IS NULL, '', COL007) AS PUBL_SUB_CAMP_NM,

        if(COL009 IS NULL, '', COL009) AS PUBL_SUB_SITE_NM,

        if(COL008 IS NULL, '', COL008) AS PUBL_SUB_PUBL_NM,

        if(COL010 IS NULL, '', COL010) AS ADV_SUB_CAMP_NM,


        if(COL011 IS NULL, '', COL011) AS DEV_MODEL,
        if(COL012 IS NULL, '', COL012) AS DEV_BRAND,
        if(COL013 IS NULL, '', COL013) AS DEV_CARRIER,
        if(COL014 IS NULL, '', COL014) AS OS_VERSION,


        log_time,
        part_hour

  FROM dmp.log_server_bulk_collect_sftp
  WHERE
  part_hour = '${hivevar:hour10before}'
  AND UID IS NOT NULL AND UID <> ''
  AND UIDTYPE IS NOT NULL AND UIDTYPE IN ('GAID', 'IDFA')
  AND LSID IS NOT NULL AND LSID IN ('1022', '1023', '1031', '1032', '1033', '1034', '1035')


  )
  a

;



