USE dmp;


set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day90before;
set hivevar:day91before;


set mapreduce.job.reduces=64;
set hive.execution.engine=tez;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;


CREATE TABLE IF NOT EXISTS dmp.prod_data_source_store
(
  uid       string,

  col001    string,
  col002    string,
  col003    string,
  col004    string,
  col005    string,
  col006    string,
  col007    string,
  col008    string,
  col009    string,
  col010    string,

  col011    string,
  col012    string,
  col013    string,
  col014    string,
  col015    string,
  col016    string,
  col017    string,
  col018    string,
  col019    string,
  col020    string,

  col021    string,
  col022    string,
  col023    string,
  col024    string,
  col025    string,
  col026    string,
  col027    string,
  col028    string,
  col029    string,
  col030    string,

  col031    string,
  col032    string,
  col033    string,
  col034    string,
  col035    string,
  col036    string,
  col037    string,
  col038    string,
  col039    string,
  col040    string,

  col041    string,
  col042    string,
  col043    string,
  col044    string,
  col045    string,
  col046    string,
  col047    string,
  col048    string,
  col049    string,
  col050    string,

  col051    string,
  col052    string,
  col053    string,
  col054    string,
  col055    string,
  col056    string,
  col057    string,
  col058    string,
  col059    string,
  col060    string,

  col061    string,
  col062    string,
  col063    string,
  col064    string,
  col065    string,
  col066    string,
  col067    string,
  col068    string,
  col069    string,
  col070    string,

  col071    string,
  col072    string,
  col073    string,
  col074    string,
  col075    string,
  col076    string,
  col077    string,
  col078    string,
  col079    string,
  col080    string,

  col081    string,
  col082    string,
  col083    string,
  col084    string,
  col085    string,
  col086    string,
  col087    string,
  col088    string,
  col089    string,
  col090    string,

  col091    string,
  col092    string,
  col093    string,
  col094    string,
  col095    string,
  col096    string,
  col097    string,
  col098    string,
  col099    string,
  col100    string
)
PARTITIONED BY (part_hour string, data_source_id string);

set hive.exec.dynamic.partition.mode=true;

ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='62', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- SK Planet Demographics
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='63', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- SK Planet 11st
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='64', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- SK Planet OCB
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='65', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- SK Planet SyrupWallet
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='67', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- SK Telecom T-map
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='69', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- SK Broadband Demographics
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='73', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- Yuhan Kimberly momQ

-- SK Planet Demographics

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  DMP_UID as uid,
  AGE age,
  CASE
        WHEN GENCLASS IS NOT NULL AND GENCLASS IN ('7','4','8') THEN GENCLASS
        ELSE ''
  END genclass,
  POINT ocbpoint,
  COL001 ocb_serv,
  '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',

  '${hivevar:daybefore}00' as part_hour, '62' as data_source_id
  FROM svc_ds_dmp.dmp_log_server_tracking_user_mode_new
  WHERE (part_date = '${hivevar:daybefore}');

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '62' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 62;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='62', part_hour>='${hivevar:day2before}00', part_hour<='${hivevar:day2before}24');



-- SK Planet 11st

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
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
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('search') THEN ''
      ELSE REGEXP_REPLACE(a.PROD_NM,'\\^',' ')
  END AS ITEM,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('search') THEN ''
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
         END,
         CASE WHEN a.CATEGORY_CD4 IN (' ','') OR a.CATEGORY_CD4 IS NULL THEN ''
         ELSE CONCAT('\||',SUBSTR(a.CATEGORY_CD4,(INSTR(a.CATEGORY_CD4, ':')+1)))
         END,
         CASE WHEN a.CATEGORY_CD5 IN (' ','') OR a.CATEGORY_CD5 IS NULL THEN ''
         ELSE CONCAT('\||',SUBSTR(a.CATEGORY_CD5,(INSTR(a.CATEGORY_CD5, ':')+1)))
         END,
         CASE WHEN a.CATEGORY_CD6 IN (' ','') OR a.CATEGORY_CD6 IS NULL THEN ''
         ELSE CONCAT('\||',SUBSTR(a.CATEGORY_CD6,(INSTR(a.CATEGORY_CD6, ':')+1)))
         END
         )
         )
  END AS CATEGORY,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN REGEXP_REPLACE(a.ID,'\\^',' ')
      ELSE ''
  END AS KEYWORD,

  CASE
      WHEN a.ITEM_CODE IS NOT NULL THEN a.ITEM_CODE
      ELSE ''
  END AS ITEM_CODE,

  CASE
      WHEN a.AMOUNT IS NOT NULL THEN a.AMOUNT
      ELSE ''
  END AS AMOUNT,


  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',

  '${hivevar:daybefore}00' as part_hour, '63' as data_source_id

  FROM
  (
  SELECT
         REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(


         CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) =  LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(GAID)',SUBSTR(DMP_UID,7))
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) <> LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(IDFA)',SUBSTR(DMP_UID,7))
         ELSE DMP_UID
         END


         ,'\n',''),'\,',' '),'\;',' '),'\:',' ') AS DMP_UID,

        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,
        REGEXP_REPLACE(get_json_object(BODY, '$.col005'),'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.col019') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\,\;\:\r+]',' '), '\\\\\\/','\\/') AS PROD_NM,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD3,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD4,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD5,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD6,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('search','welcome','sdest','rdest','fdest') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\,\;\:\r+]',' ')
            ELSE ''
        END AS ID,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('view', 'basket', 'order', 'wish') THEN get_json_object(BODY, '$.col007')
            ELSE ''
        END AS ITEM_CODE,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('view', 'basket', 'order', 'wish') THEN get_json_object(BODY, '$.col015')
            ELSE ''
        END AS AMOUNT


  FROM dmp.log_server_idsync_collect
  WHERE
  part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
  AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
  AND REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ')='2'

  )
  a;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '63' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 63;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='63', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');



-- SK Planet OCB

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
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
      WHEN a.CHANNEL IS NULL THEN ''
      WHEN a.CHANNEL = 'Android' THEN '1'
      WHEN a.CHANNEL = 'iOS' THEN '2'
      ELSE a.CHANNEL
  END AS CHANNEL,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'eventview' THEN REGEXP_REPLACE(a.PROD_NM,'\\^',' ')
      ELSE ''
  END AS ITEM,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'eventview' THEN REGEXP_REPLACE(a.CATEGORY_CD1,'\\^',' ')
      ELSE ''
  END AS CATEGORY,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN REGEXP_REPLACE(a.ID,'\\^',' ')
      ELSE ''
  END AS KEYWORD,


  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN REGEXP_REPLACE(a.REF,'\\^',' ')
      ELSE ''
  END AS MAG_NAME,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN REGEXP_REPLACE(a.ID,'\\^',' ')
      ELSE ''
  END AS MAG_CONTENT_ID,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN REGEXP_REPLACE(a.PROD_NM,'\\^',' ')
      ELSE ''
  END AS MAG_CONTENT_SUB,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'magazineview' THEN REGEXP_REPLACE(a.SALES_STATE,'\\^',' ')
      ELSE ''
  END AS MAG_REWARD_TYPE,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'welcome' THEN
                CASE
                     WHEN (a.OCB_SERV = '2' OR a.OCB_SERV like '%,2' OR a.OCB_SERV like '2,%' OR a.OCB_SERV like '%,2,%') THEN 'Y'
                     ELSE 'N'
                END
      ELSE ''
  END AS OCB_LOCK_YN,



  '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',

  '${hivevar:daybefore}00' as part_hour, '64' as data_source_id

  FROM
  (
  SELECT
         REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(


         CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) =  LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(GAID)',SUBSTR(DMP_UID,7))
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) <> LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(IDFA)',SUBSTR(DMP_UID,7))
         ELSE DMP_UID
         END


         ,'\n',''),'\,',' '),'\;',' '),'\:',' ') AS DMP_UID,

        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,
        REGEXP_REPLACE(get_json_object(BODY, '$.col005'),'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.col019') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\,\;\:\r+]',' '), '\\\\\\/','\\/') AS PROD_NM,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD3,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD4,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD5,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD6,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col002') IS NOT NULL THEN get_json_object(BODY, '$.col002') ELSE '' END),'[\n\,\;\r+]',' ') AS REF,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col018') IS NOT NULL THEN get_json_object(BODY, '$.col018') ELSE '' END),'[\n\,\;\:\r+]',' '), '\\\\\\/','\\/') AS SALES_STATE,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('search','welcome','eventview','magazineview') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\,\;\:\r+]',' ')
            ELSE ''
        END AS ID,

        REFLECT('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(body, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END) AS OCB_SERV

  FROM dmp.log_server_idsync_collect
  WHERE
  part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
  AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
  AND REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ')='9'

  )
  a;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '64' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 64;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='64', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');



-- SK Planet SyrupWallet

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
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

  '' AS CATEGORY,

  CASE
        WHEN a.CATEGORY_CD1 IS NULL THEN ''
        ELSE a.CATEGORY_CD1
  END AS BIZCATEGORY,

  CASE
      WHEN a.ACTION IS NULL THEN ''
      ELSE
      TRIM(
        CONCAT(
         CASE WHEN a.CATEGORY_CD2 IN (' ','') OR a.CATEGORY_CD2 IS NULL THEN ''
         ELSE SUBSTR(a.CATEGORY_CD2,(INSTR(a.CATEGORY_CD2, ':')+1))
         END,
         CASE WHEN a.CATEGORY_CD3 IN (' ','') OR a.CATEGORY_CD3 IS NULL THEN ''
         ELSE CONCAT('\||',SUBSTR(a.CATEGORY_CD3,(INSTR(a.CATEGORY_CD3, ':')+1)))
         END
         )
         )
  END AS POICATEGORY,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN REGEXP_REPLACE(a.ID,'\\^',' ')
      ELSE ''
  END AS KEYWORD,


  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',

  '${hivevar:daybefore}00' as part_hour, '65' as data_source_id

  FROM
  (
  SELECT
         REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(


         CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) =  LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(GAID)',SUBSTR(DMP_UID,7))
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) <> LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(IDFA)',SUBSTR(DMP_UID,7))
         ELSE DMP_UID
         END


         ,'\n',''),'\,',' '),'\;',' '),'\:',' ') AS DMP_UID,

        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,
        REGEXP_REPLACE(get_json_object(BODY, '$.col005'),'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.col019') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\,\;\:\r+]',' '), '\\\\\\/','\\/') AS PROD_NM,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD3,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD4,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD5,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD6,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('search','welcome','sdest','rdest','fdest') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\,\;\:\r+]',' ')
            ELSE ''
        END AS ID

  FROM dmp.log_server_idsync_collect
  WHERE
  part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
  AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
  AND REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ')='10'

  )
  a;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '65' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 65;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='65', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');



-- SK Telecom T-map

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
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
     WHEN ACTION IN ('sdest','rdest','fdest') THEN REGEXP_REPLACE(a.COL001,'\\^',' ')
     ELSE ''
  END ADDRESS,

  CASE
      WHEN a.CATEGORY_CD1 IS NULL THEN ''
      ELSE a.CATEGORY_CD1
  END AS POICATEGORY1,

  CASE
      WHEN a.CATEGORY_CD2 IS NULL THEN ''
      ELSE a.CATEGORY_CD2
  END AS POICATEGORY2,

  CASE
      WHEN a.CATEGORY_CD3 IS NULL THEN ''
      ELSE a.CATEGORY_CD3
  END AS POICATEGORY3,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('sdest','rdest','fdest')  THEN REGEXP_REPLACE(a.ID,'\\^',' ')
      ELSE ''
  END AS DESTINATION,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN REGEXP_REPLACE(a.ID,'\\^',' ')
      ELSE ''
  END AS KEYWORD,

  CASE
      WHEN a.PROD_NM IS NOT NULL THEN a.PROD_NM
      ELSE ''
  END AS FAVORITE,


  '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',

  '${hivevar:daybefore}00' as part_hour, '67' as data_source_id

  FROM
  (
  SELECT
         REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(


         CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) =  LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(GAID)',SUBSTR(DMP_UID,7))
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) <> LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(IDFA)',SUBSTR(DMP_UID,7))
         ELSE DMP_UID
         END


         ,'\n',''),'\,',' '),'\;',' '),'\:',' ') AS DMP_UID,

        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,
        REGEXP_REPLACE(get_json_object(BODY, '$.col005'),'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.col019') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\,\;\:\r+]',' '), '\\\\\\/','\\/') AS PROD_NM,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD3,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD4,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD5,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END),'[\n\,\;\r+]',' '), '\\\\\\/','\\/') AS CATEGORY_CD6,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('search','welcome','sdest','rdest','fdest') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\,\;\:\r+]',' ')
            ELSE ''
        END AS ID,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col002') IS NOT NULL THEN get_json_object(BODY, '$.col002') ELSE '' END),'[\n\,\;\r+]',' ') AS COL001

  FROM dmp.log_server_idsync_collect
  WHERE
  part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
  AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
  AND REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ')='11'

  )
  a;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '67' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 67;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='67', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');



-- SK Broadband Demographics

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)

     SELECT
     T2.UID,
     T2.AGE,
     T2.GENCLASS,
     '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',
     '', '', '', '', '', '', '', '', '', '',

     '${hivevar:daybefore}00' as part_hour, '69' as data_source_id

     FROM
     (

             SELECT
             T1.UID,
             T1.AGE,
             T1.GENCLASS,
             ROW_NUMBER() OVER(PARTITION BY T1.UID ORDER BY T1.LOG_TIME DESC ) AS RN

             FROM
             (
                SELECT

                CASE
                    WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
                    WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
                    WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) =  LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(GAID)',SUBSTR(DMP_UID,7))
                    WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) <> LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(IDFA)',SUBSTR(DMP_UID,7))
                    ELSE DMP_UID
                END AS UID,

                CASE
                   WHEN get_json_object(BODY, '$.col012') IS NULL OR get_json_object(BODY, '$.col012') = 'NONE' THEN ''
                   ELSE REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col012')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                END
                AS AGE,

                CASE
                   WHEN get_json_object(BODY, '$.col011') IS NULL THEN ''
                   WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col011')),'\n',''),'\,',' '),'\;',' '),'\:',' ') = 'M' THEN '4'
                   WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col011')),'\n',''),'\,',' '),'\;',' '),'\:',' ') = 'F' THEN '8'
                   ELSE REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col011')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                END
                AS GENCLASS,

                LOG_TIME

                FROM dmp.log_server_idsync_collect
                WHERE
                part_hour between '${hivevar:day90before}00' and '${hivevar:daybefore}24'
                AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
                AND DSID IS NOT NULL AND DSID IN ('dmp-53')

             ) AS T1
             WHERE T1.AGE IS NOT NULL AND T1.AGE <> '' AND T1.GENCLASS IS NOT NULL AND T1.GENCLASS <> ''

     ) AS T2
     WHERE  T2.RN = 1

  ;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '69' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 69;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='69', part_hour>='${hivevar:day2before}00', part_hour<='${hivevar:day2before}24');



-- Yuhan Kimberly momQ

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
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

  '${hivevar:daybefore}00' as part_hour, '73' as data_source_id

  FROM
  (
  SELECT
         REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(


         CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) =  LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(GAID)',SUBSTR(DMP_UID,7))
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) <> LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(IDFA)',SUBSTR(DMP_UID,7))
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
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\,\;\:\r+]',' '), '\\\\\\/','\\/') AS REF


  FROM dmp.log_server_idsync_collect
  WHERE
  part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
  AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
  AND DSID IS NOT NULL AND DSID = '73'

  )
  a;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '73' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 73;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='73', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');

