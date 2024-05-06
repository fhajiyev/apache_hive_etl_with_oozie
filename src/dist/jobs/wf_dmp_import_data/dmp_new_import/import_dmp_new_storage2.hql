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

ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='71', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- SK Broadband Oksusu
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='68', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- SK Broadband B-tv
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='70', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- Recopick Demographics
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='72', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- Recopick Recopick
ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='75', part_hour>='${hivevar:daybefore}00', part_hour<='${hivevar:daybefore}24'); -- Yuhan Kimberly Demographics


-- SK Broadband Oksusu

INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
select

  CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) =  LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(GAID)',SUBSTR(DMP_UID,7))
         WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) <> LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(IDFA)',SUBSTR(DMP_UID,7))
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

  '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',

  '${hivevar:daybefore}00' as part_hour, '71' as data_source_id

from
DMP.LOG_SERVER_IDSYNC_COLLECT
WHERE
part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
AND DSID IS NOT NULL AND DSID = 'dmp-53'
;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '71' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 71;

ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='71', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');



-- SK Broadband B-tv

INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
select

  CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(U003)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(U003)',IDFA)
         ELSE CONCAT('(U003)',DMP_UID)
  END AS UID,

  CASE
         WHEN get_json_object(body, '$.col013') IS NULL OR get_json_object(body, '$.col013') = '' THEN ''
         ELSE CONCAT(SUBSTR(get_json_object(body, '$.col013'),1,4),SUBSTR(get_json_object(body, '$.col013'),6,2),SUBSTR(get_json_object(body, '$.col013'),9,2))
  END AS DATE,

  CASE
         WHEN get_json_object(body, '$.col013') IS NULL OR get_json_object(body, '$.col013') = '' THEN ''
         ELSE SUBSTR(get_json_object(body, '$.col013'),12,2)
  END AS TIME,

  CASE
          WHEN get_json_object(body, '$.col013') IS NULL OR get_json_object(body, '$.col013') = '' THEN ''
          WHEN from_unixtime(unix_timestamp(CONCAT(SUBSTR(get_json_object(body, '$.col013'),1,4),SUBSTR(get_json_object(body, '$.col013'),6,2),SUBSTR(get_json_object(body, '$.col013'),9,2)),'yyyyMMdd'),'u') = '1' THEN 'Mon'
          WHEN from_unixtime(unix_timestamp(CONCAT(SUBSTR(get_json_object(body, '$.col013'),1,4),SUBSTR(get_json_object(body, '$.col013'),6,2),SUBSTR(get_json_object(body, '$.col013'),9,2)),'yyyyMMdd'),'u') = '2' THEN 'Tue'
          WHEN from_unixtime(unix_timestamp(CONCAT(SUBSTR(get_json_object(body, '$.col013'),1,4),SUBSTR(get_json_object(body, '$.col013'),6,2),SUBSTR(get_json_object(body, '$.col013'),9,2)),'yyyyMMdd'),'u') = '3' THEN 'Wed'
          WHEN from_unixtime(unix_timestamp(CONCAT(SUBSTR(get_json_object(body, '$.col013'),1,4),SUBSTR(get_json_object(body, '$.col013'),6,2),SUBSTR(get_json_object(body, '$.col013'),9,2)),'yyyyMMdd'),'u') = '4' THEN 'Thu'
          WHEN from_unixtime(unix_timestamp(CONCAT(SUBSTR(get_json_object(body, '$.col013'),1,4),SUBSTR(get_json_object(body, '$.col013'),6,2),SUBSTR(get_json_object(body, '$.col013'),9,2)),'yyyyMMdd'),'u') = '5' THEN 'Fri'
          WHEN from_unixtime(unix_timestamp(CONCAT(SUBSTR(get_json_object(body, '$.col013'),1,4),SUBSTR(get_json_object(body, '$.col013'),6,2),SUBSTR(get_json_object(body, '$.col013'),9,2)),'yyyyMMdd'),'u') = '6' THEN 'Sat'
          WHEN from_unixtime(unix_timestamp(CONCAT(SUBSTR(get_json_object(body, '$.col013'),1,4),SUBSTR(get_json_object(body, '$.col013'),6,2),SUBSTR(get_json_object(body, '$.col013'),9,2)),'yyyyMMdd'),'u') = '7' THEN 'Sun'
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

  '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',

  '${hivevar:daybefore}00' as part_hour, '68' as data_source_id

from
DMP.LOG_SERVER_IDSYNC_COLLECT
WHERE
part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
AND DSID IS NOT NULL AND DSID = '68'
;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '68' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 68;

ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='68', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');



-- Recopick Demographics

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

       '${hivevar:daybefore}00' as part_hour, '70' as data_source_id

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
                     WHEN SUBSTR(DMPC,1,6)='(DMPD)' AND LENGTH(DMPC)=42 AND SUBSTR(DMPC,7) =  LOWER(SUBSTR(DMPC,7)) THEN CONCAT('(GAID)',SUBSTR(DMPC,7))
                     WHEN SUBSTR(DMPC,1,6)='(DMPD)' AND LENGTH(DMPC)=42 AND SUBSTR(DMPC,7) <> LOWER(SUBSTR(DMPC,7)) THEN CONCAT('(IDFA)',SUBSTR(DMPC,7))
                     ELSE DMPC
               END AS UID,

               case
                    when birthyear is null or birthyear = '' then ''
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 18 and 19 then '18-19'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 20 and 24 then '20-24'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 25 and 29 then '25-29'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 30 and 34 then '30-34'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 35 and 39 then '35-39'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 40 and 44 then '40-44'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 45 and 49 then '45-49'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 50 and 54 then '50-54'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 55 and 59 then '55-59'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 60 and 64 then '60-64'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 65 and 69 then '65-69'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) between 70 and 74 then '70-74'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int) >= 75               then '75+'
                    else ''
               end AS AGE,

               case
                    when gender is not null and gender in ('M','10') then 'Male'
                    when gender is not null and gender in ('F','20') then 'Female'
                    else ''
               end AS GENCLASS,

               LOG_TIME

               from (
                    select
                      trim(get_json_object(body, '$.dmpuid')) as dmpc,
                      trim(get_json_object(body, '$.birthyear')) as birthyear,
                      trim(get_json_object(body, '$.gender')) as gender,
                      trim(get_json_object(body, '$.gaid')) as gaid,
                      trim(get_json_object(body, '$.idfa')) as idfa,
                      log_time
                    from dmp.log_server_recopick
                    where
                    part_hour between '${hivevar:day90before}00' and '${hivevar:daybefore}24'
                    and get_json_object(body, '$.dmpuid') is not null and get_json_object(body, '$.dmpuid') <> '' AND (SUBSTR(get_json_object(body, '$.dmpuid'),1,6) = '(DMPC)' OR (SUBSTR(get_json_object(body, '$.dmpuid'),1,6) = '(DMPD)' AND ((get_json_object(body, '$.gaid') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.gaid')))=36 AND trim(get_json_object(body, '$.gaid'))=LOWER(trim(get_json_object(body, '$.gaid')))) OR (get_json_object(body, '$.idfa') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.idfa')))=36 AND trim(get_json_object(body, '$.idfa'))<>LOWER(trim(get_json_object(body, '$.idfa')))))))
                    and length(sid) <= 20
                   ) t

           ) AS T1
           WHERE T1.AGE IS NOT NULL AND T1.AGE <> '' AND T1.GENCLASS IS NOT NULL AND T1.GENCLASS <> ''

       ) AS T2
       WHERE  T2.RN = 1

  ;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '70' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 70;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='70', part_hour>='${hivevar:day2before}00', part_hour<='${hivevar:day2before}24');



-- Recopick Recopick

INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
SELECT
      a.UID,
      a.REQUEST_DATE,
      a.REQUEST_TIME,
      a.WEEKDAY,
      a.SITE,
      a.ACTION,
      CASE WHEN b.ITEM_NAME IS NULL THEN '' ELSE b.ITEM_NAME END,
      CASE WHEN b.ITEM_CAT  IS NULL THEN '' ELSE b.ITEM_CAT  END,
      a.KEYWORD,
      a.ITEMCODE,

      '',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',

      a.PART_HOUR,
      a.DATA_SOURCE_ID
FROM
(
  SELECT

  CASE
           WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
           WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
           WHEN SUBSTR(DMPC,1,6)='(DMPD)' AND LENGTH(DMPC)=42 AND SUBSTR(DMPC,7) =  LOWER(SUBSTR(DMPC,7)) THEN CONCAT('(GAID)',SUBSTR(DMPC,7))
           WHEN SUBSTR(DMPC,1,6)='(DMPD)' AND LENGTH(DMPC)=42 AND SUBSTR(DMPC,7) <> LOWER(SUBSTR(DMPC,7)) THEN CONCAT('(IDFA)',SUBSTR(DMPC,7))
           ELSE DMPC
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
  END AS WEEKDAY,

  site_id AS SITE,

  case
      when action_id is null then ''
      when action_id = '0'                              then 'visit'
      when cast(action_id as int) between   1 and  99 then 'view'
      when cast(action_id as int) between 101 and 199 then 'order'
      when cast(action_id as int) between 201 and 299 then 'basket'
      when cast(action_id as int) between 301 and 399 then 'search'
      when cast(action_id as int) between 401 and 499 then 'like'
      else action_id
  end AS ACTION,

  CASE
      WHEN get_json_object(item, '$.title') IS NULL THEN ''
      WHEN site_id IN ('47') AND reflect('java.net.URLDecoder', 'decode', get_json_object(item, '$.title')) IS NOT NULL THEN TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(item, '$.title')))
      ELSE TRIM(get_json_object(item, '$.title'))
  END AS ITEM,

      TRIM
      (
        CONCAT
         (
         CASE WHEN get_json_object(item, '$.c1') IS NULL THEN ''
         ELSE TRIM(get_json_object(item, '$.c1'))
         END,
         CASE WHEN get_json_object(item, '$.c2') IS NULL OR TRIM(get_json_object(item, '$.c2')) = '' THEN ''
         ELSE CONCAT('\||', TRIM(get_json_object(item, '$.c2')))
         END,
         CASE WHEN get_json_object(item, '$.c3') IS NULL OR TRIM(get_json_object(item, '$.c3')) = '' THEN ''
         ELSE CONCAT('\||', TRIM(get_json_object(item, '$.c3')))
         END,
         CASE WHEN get_json_object(item, '$.c4') IS NULL OR TRIM(get_json_object(item, '$.c4')) = '' THEN ''
         ELSE CONCAT('\||', TRIM(get_json_object(item, '$.c4')))
         END
         )
      ) AS CATEGORY,

  case
       when keyword is null then ''
       else keyword
  end AS KEYWORD,

  case
       when get_json_object(item, '$.id') is null then ''
       else TRIM(get_json_object(item, '$.id'))
  end AS ITEMCODE,

  '${hivevar:daybefore}00' as part_hour, '72' as data_source_id

from (
  select
    trim(get_json_object(body, '$.dmpuid')) as dmpc,
    log_time,
    sid as site_id,
    action_id,
    if(get_json_object(body, '$.tag_items') is null, array('-1'), split(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(get_json_object(body, '$.tag_items'), '\\[', ''), '\\]', ''), '\\"\\{', '\\{'), '\\}\\"', '\\}'), '\\\\\"', '\\"'), '\\},\\{', '\\},,,,\\{'), ',,,,')) as items,
    trim(get_json_object(body, '$.gaid')) as gaid,
    trim(get_json_object(body, '$.idfa')) as idfa,
    trim(get_json_object(body, '$.q')) as keyword
  from dmp.log_server_recopick
  where
  part_hour between '${hivevar:daybefore}00' and '${hivevar:daybefore}24'
  and get_json_object(body, '$.dmpuid') is not null and get_json_object(body, '$.dmpuid') <> '' AND (SUBSTR(get_json_object(body, '$.dmpuid'),1,6) = '(DMPC)' OR (SUBSTR(get_json_object(body, '$.dmpuid'),1,6) = '(DMPD)' AND ((get_json_object(body, '$.gaid') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.gaid')))=36 AND trim(get_json_object(body, '$.gaid'))=LOWER(trim(get_json_object(body, '$.gaid')))) OR (get_json_object(body, '$.idfa') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.idfa')))=36 AND trim(get_json_object(body, '$.idfa'))<>LOWER(trim(get_json_object(body, '$.idfa')))))))
  and length(sid) <= 20
) t
lateral view explode(t.items) items_view as item


)
a
LEFT JOIN
SVC_DS_DMP.PROD_RECOPICK_SERVICE_ITEM_ID_TO_ITEM_META_MAP b
ON
a.SITE = b.SITE_ID AND a.ITEMCODE = b.ITEM_ID
;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '72' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 72;

ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='72', part_hour>='${hivevar:day91before}00', part_hour<='${hivevar:day91before}24');



-- Yuhan Kimberly Demographics

INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)

  SELECT

  DMP_UID,
  MEMBER_TYPE,
  GRADE,
  CHLD_AGE,


  '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',

  '${hivevar:daybefore}00' as part_hour, '75' as data_source_id

  from (
    select
      REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(


               CASE
               WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
               WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
               WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) =  LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(GAID)',SUBSTR(DMP_UID,7))
               WHEN SUBSTR(DMP_UID,1,6)='(DMPD)' AND LENGTH(DMP_UID)=42 AND SUBSTR(DMP_UID,7) <> LOWER(SUBSTR(DMP_UID,7)) THEN CONCAT('(IDFA)',SUBSTR(DMP_UID,7))
               ELSE DMP_UID
               END


               ,'\n',''),'\,',' '),'\;',' '),'\:',' ') AS DMP_UID,

      split(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(body, '$.col012') IS NOT NULL THEN get_json_object(body, '$.col012') ELSE '' END), ',') as MEMBER_TYPES,
      reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(body, '$.col011') IS NOT NULL THEN get_json_object(body, '$.col011') ELSE '' END) as GRADE,
      CASE WHEN get_json_object(body, '$.col010') IS NOT NULL THEN get_json_object(body, '$.col010') ELSE '' END as CHLD_AGE

    FROM dmp.log_server_idsync_collect
    WHERE
    part_hour between '${hivevar:day90before}00' and '${hivevar:daybefore}24'
    AND DMP_UID IS NOT NULL AND DMP_UID <> '' AND (SUBSTR(DMP_UID,1,6) = '(DMPC)' OR (SUBSTR(DMP_UID,1,6) = '(DMPD)' AND ((GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID)) OR (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA)))))
    AND DSID IS NOT NULL AND DSID = '73'

  ) t
 lateral view explode(t.member_types) member_types_view as member_type
 WHERE ((MEMBER_TYPE IS NOT NULL AND MEMBER_TYPE <> '')OR(GRADE IS NOT NULL AND GRADE <> '')OR(CHLD_AGE IS NOT NULL AND CHLD_AGE <> ''))
 ;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:daybefore}00' as part_hour, '75' as data_source_id
  FROM svc_ds_dmp.prod_uid_upload
  WHERE datasourceid = 75;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='75', part_hour>='${hivevar:day2before}00', part_hour<='${hivevar:day2before}24');
