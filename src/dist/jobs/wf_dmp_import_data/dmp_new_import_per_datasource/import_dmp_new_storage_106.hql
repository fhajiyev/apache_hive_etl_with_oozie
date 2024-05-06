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

-- T-membership

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:day2before}00', data_source_id='106')
  SELECT
  DMP_ID as uid,
  REQUEST_DATE,
  REQUEST_TIME,
  CASE
        WHEN WEEKDAY = '1' THEN 'Mon'
        WHEN WEEKDAY = '2' THEN 'Tue'
        WHEN WEEKDAY = '3' THEN 'Wed'
        WHEN WEEKDAY = '4' THEN 'Thu'
        WHEN WEEKDAY = '5' THEN 'Fri'
        WHEN WEEKDAY = '6' THEN 'Sat'
        WHEN WEEKDAY = '7' THEN 'Sun'
  END,

  ACTION,
  if(ACTION = '1', GENCLASS,    ''),
  if(ACTION = '1', AGE,         ''),
  if(ACTION = '2', USAGE_PRICE, ''),
  if(ACTION = '2', MBR_LVL_1,   ''),
  if(ACTION = '2', MBR_LVL_2,   ''),
  if(ACTION = '2', MBR_LVL_3,   ''),
  if(ACTION = '2', MBR_LVL_4,   ''),
  if(ACTION = '2', MBR_LVL_5,   ''),

  '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  part_hour,
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

        if(COL001 IS NULL, '', COL001) AS ACTION,

        if(COL002 IS NULL, '', COL002) AS GENCLASS,

        if(COL003 IS NULL, '', COL003) AS AGE,

        if(COL004 IS NULL, '', COL004) AS USAGE_PRICE,

        if(COL005 IS NULL, '', COL005) AS MBR_LVL_1,

        if(COL006 IS NULL, '', COL006) AS MBR_LVL_2,

        if(COL007 IS NULL, '', COL007) AS MBR_LVL_3,

        if(COL008 IS NULL, '', COL008) AS MBR_LVL_4,

        if(COL009 IS NULL, '', COL009) AS MBR_LVL_5,

        log_time,
        part_hour

  FROM dmp.log_server_bulk_collect_sftp
  WHERE
  part_hour between '${hivevar:day2before}00' and '${hivevar:day2before}24'
  AND UID IS NOT NULL AND UID <> ''
  AND UIDTYPE IS NOT NULL AND UIDTYPE IN ('GAID', 'IDFA')
  AND LSID IS NOT NULL AND LSID IN ('1016')


  )
  a
  WHERE ACTION <> ''

;



