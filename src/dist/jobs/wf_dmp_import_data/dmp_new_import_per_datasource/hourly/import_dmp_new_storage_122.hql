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


-- OCB Lock TUNE

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hour10before}', data_source_id='122')
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
  REGEXP_REPLACE(STATUS, '\001', ''),
  REGEXP_REPLACE(EXIST_USER, '\001', ''),
  REGEXP_REPLACE(IS_REENGAGE, '\001', ''),
  REGEXP_REPLACE(AGENCY_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUBAD_NM, '\001', ''),
  REGEXP_REPLACE(ADV_SUBAD_GRP_NM, '\001', ''),
  REGEXP_REPLACE(CONTENT_ID, '\001', ''),
  REGEXP_REPLACE(ADV_SUBCAMP_NM, '\001', ''),

  REGEXP_REPLACE(DEV_MODEL, '\001', ''),
  REGEXP_REPLACE(DEV_BRAND, '\001', ''),
  REGEXP_REPLACE(DEV_CARRIER, '\001', ''),
  REGEXP_REPLACE(OS_VERSION, '\001', ''),

  '', '', '',
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
            WHEN LSID = '1036' THEN 'open'
            WHEN LSID = '1037' THEN 'install'
            WHEN LSID = '1038' THEN 'click'
            WHEN LSID = '1039' THEN 'registration'
            WHEN LSID = '1040' THEN 'registration_easy'
        END
        AS ACTION,

        if(COL002 IS NULL, '', COL002) AS PUBL_NM,

        if(COL004 IS NULL, '', COL004) AS STATUS,

        if(COL005 IS NULL, '', COL005) AS EXIST_USER,

        if(COL006 IS NULL, '', COL006) AS IS_REENGAGE,

        if(COL009 IS NULL, '', COL009) AS AGENCY_NM,

        if(COL010 IS NULL, '', COL010) AS ADV_SUBAD_NM,

        if(COL011 IS NULL, '', COL011) AS ADV_SUBAD_GRP_NM,

        if(COL012 IS NULL, '', COL012) AS CONTENT_ID,

        if(COL013 IS NULL, '', COL013) AS ADV_SUBCAMP_NM,

        if(COL014 IS NULL, '', COL014) AS DEV_MODEL,
        if(COL015 IS NULL, '', COL015) AS DEV_BRAND,
        if(COL016 IS NULL, '', COL016) AS DEV_CARRIER,
        if(COL017 IS NULL, '', COL017) AS OS_VERSION,


        log_time,
        part_hour

  FROM dmp.log_server_bulk_collect_sftp
  WHERE
  part_hour = '${hivevar:hour10before}'
  AND UID IS NOT NULL AND UID <> ''
  AND UIDTYPE IS NOT NULL AND UIDTYPE IN ('GAID', 'IDFA')
  AND LSID IS NOT NULL AND LSID IN ('1036', '1037', '1038', '1039', '1040')


  )
  a

;


