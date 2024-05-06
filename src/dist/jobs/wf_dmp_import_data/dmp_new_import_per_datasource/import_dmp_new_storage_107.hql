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

-- T DCB

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:day2before}00', data_source_id='107')
  SELECT
  b.DMP_ID as uid,
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
  if(ACTION = '1', PRICE,    ''),
  if(ACTION = '1', SITE_NM,  ''),
  if(ACTION = '1', PROD_CTG, ''),

  '', '', '',
  '', '', '', '', '', '', '', '', '', '',
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

        UID,

        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,

        if(COL001 IS NULL, '', COL001) AS ACTION,

        if(COL002 IS NULL, '', COL002) AS PRICE,

        if(COL003 IS NULL, '', COL003) AS SITE_NM,

        if(COL004 IS NULL, '', COL004) AS PROD_CTG,

        log_time,
        part_hour

  FROM dmp.log_server_bulk_collect_sftp
  WHERE
  part_hour between '${hivevar:day2before}00' and '${hivevar:day2before}24'
  AND UID IS NOT NULL AND UID <> ''
  AND UIDTYPE IS NOT NULL AND UIDTYPE IN ('U012')
  AND LSID IS NOT NULL AND LSID IN ('1017')


  )
  a

  INNER JOIN
  (

      SELECT
      DMP_ID,
      DSP_COOKIE_ID
      FROM
      SVC_DS_DMP.PROD_DMP_ID_MATCHING
      WHERE
      ID_TYPE IN ('uid012')

  ) b
  ON
  a.UID = b.DSP_COOKIE_ID

  WHERE a.ACTION <> ''

;



