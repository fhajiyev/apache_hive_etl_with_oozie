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


-- SK Telecom T-map

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='67')

  SELECT

  b.uid,
  b.request_date,
  b.request_time,
  b.weekday,
  b.action,
  b.channel,
  b.address,
  b.POICATEGORY1,
  b.POICATEGORY2,
  b.POICATEGORY3,
  b.destination,
  b.keyword,
  b.favorite,
  b.dest1,
  b.dest2,
  b.dest3,

  if(c.ca_name is null, '', c.ca_name),
  if(c.cb_name is null, '', c.cb_name),
  if(c.cc_name is null, '', c.cc_name),
  if(c.cd_name is null, '', c.cd_name),

  TRIM
  (
        CONCAT
        (
         CASE WHEN c.ca_name IS NULL OR TRIM(c.ca_name) IN ('', 'null') THEN ''
         ELSE TRIM(c.ca_name)
         END,
         CASE WHEN c.cb_name IS NULL OR TRIM(c.cb_name) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(c.cb_name))
         END,
         CASE WHEN c.cc_name IS NULL OR TRIM(c.cc_name) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(c.cc_name))
         END,
         CASE WHEN c.cd_name IS NULL OR TRIM(c.cd_name) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(c.cd_name))
         END
        )
  ),

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
  END AS WEEKDAY,

  CASE
      WHEN a.ACTION IS NOT NULL THEN a.ACTION
      ELSE ''
  END AS ACTION,

  CASE
      WHEN a.CHANNEL IS NOT NULL THEN a.CHANNEL
      ELSE ''
  END AS CHANNEL,

  CASE
     WHEN ACTION IN ('sdest','rdest','fdest') THEN
        CASE
                     WHEN a.COL001 is null THEN ''

                     WHEN a.COL001 like '서울%'        THEN REGEXP_REPLACE(a.COL001,'서울특별시','서울')

                     WHEN a.COL001 like '인천%'        THEN REGEXP_REPLACE(a.COL001,'인천광역시','인천')
                     WHEN a.COL001 like '울산%'        THEN REGEXP_REPLACE(a.COL001,'울산광역시','울산')
                     WHEN a.COL001 like '부산%'        THEN REGEXP_REPLACE(a.COL001,'부산광역시','부산')
                     WHEN a.COL001 like '대전%'        THEN REGEXP_REPLACE(a.COL001,'대전광역시','대전')
                     WHEN a.COL001 like '대구%'        THEN REGEXP_REPLACE(a.COL001,'대구광역시','대구')
                     WHEN a.COL001 like '광주%'        THEN REGEXP_REPLACE(a.COL001,'광주광역시','광주')

                     WHEN a.COL001 like '세종%'        THEN REGEXP_REPLACE(REGEXP_REPLACE(a.COL001,'세종특별자치시','세종'), '세종시', '세종')
                     WHEN a.COL001 like '제주%'        THEN REGEXP_REPLACE(a.COL001,'제주특별자치도','제주')

                     WHEN a.COL001 like '강원%'        THEN REGEXP_REPLACE(a.COL001,'강원도','강원')
                     WHEN a.COL001 like '경기%'        THEN REGEXP_REPLACE(a.COL001,'경기도','경기')

                     WHEN a.COL001 like '전라남도%'        THEN REGEXP_REPLACE(a.COL001,'전라남도','전남')
                     WHEN a.COL001 like '전라북도%'        THEN REGEXP_REPLACE(a.COL001,'전라북도','전북')
                     WHEN a.COL001 like '충청남도%'        THEN REGEXP_REPLACE(a.COL001,'충청남도','충남')
                     WHEN a.COL001 like '충청북도%'        THEN REGEXP_REPLACE(a.COL001,'충청북도','충북')
                     WHEN a.COL001 like '경상남도%'        THEN REGEXP_REPLACE(a.COL001,'경상남도','경남')
                     WHEN a.COL001 like '경상북도%'        THEN REGEXP_REPLACE(a.COL001,'경상북도','경북')
                     ELSE ''
        END
     ELSE ''
  END ADDRESS,

  CASE
            WHEN a.CATEGORY_CD1 IS NULL THEN ''
            WHEN a.CATEGORY_CD1 like '서울%'        THEN '서울'

            WHEN a.CATEGORY_CD1 like '인천%'        THEN '인천'
            WHEN a.CATEGORY_CD1 like '울산%'        THEN '울산'
            WHEN a.CATEGORY_CD1 like '부산%'        THEN '부산'
            WHEN a.CATEGORY_CD1 like '대전%'        THEN '대전'
            WHEN a.CATEGORY_CD1 like '대구%'        THEN '대구'
            WHEN a.CATEGORY_CD1 like '광주%'        THEN '광주'

            WHEN a.CATEGORY_CD1 like '세종%'        THEN '세종'
            WHEN a.CATEGORY_CD1 like '제주%'        THEN '제주'

            WHEN a.CATEGORY_CD1 like '강원%'        THEN '강원'
            WHEN a.CATEGORY_CD1 like '경기%'        THEN '경기'

            WHEN a.CATEGORY_CD1 = '전라남도'        THEN '전남'
            WHEN a.CATEGORY_CD1 = '전라북도'        THEN '전북'
            WHEN a.CATEGORY_CD1 = '충청남도'        THEN '충남'
            WHEN a.CATEGORY_CD1 = '충청북도'        THEN '충북'
            WHEN a.CATEGORY_CD1 = '경상남도'        THEN '경남'
            WHEN a.CATEGORY_CD1 = '경상북도'        THEN '경북'
            ELSE ''
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
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('sdest','rdest','fdest')  THEN a.ID
      ELSE ''
  END AS DESTINATION,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN a.ID
      ELSE ''
  END AS KEYWORD,

  CASE
      WHEN a.PROD_NM IS NOT NULL THEN a.PROD_NM
      ELSE ''
  END AS FAVORITE,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('sdest','rdest','fdest')  THEN if(SPLIT(a.ID, ' +')[0] is null, '', SPLIT(a.ID, ' +')[0])
        ELSE ''
  END AS DEST1,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('sdest','rdest','fdest')  THEN if(SPLIT(a.ID, ' +')[1] is null, '', SPLIT(a.ID, ' +')[1])
        ELSE ''
  END AS DEST2,

  CASE
        WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('sdest','rdest','fdest')  THEN if(SPLIT(a.ID, ' +')[2] is null, '', SPLIT(a.ID, ' +')[2])
        ELSE ''
  END AS DEST3,

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
        REGEXP_REPLACE(get_json_object(BODY, '$.col005'),'[\n\,\;\:\r+]',' ') AS ACTION,
        get_json_object(BODY, '$.col019') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS PROD_NM,

        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/')) AS CATEGORY_CD1,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/')) AS CATEGORY_CD2,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/')) AS CATEGORY_CD3,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/')) AS CATEGORY_CD4,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/')) AS CATEGORY_CD5,
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/')) AS CATEGORY_CD6,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('search','welcome','sdest','rdest','fdest') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\r]',' ')
            ELSE ''
        END AS ID,

        TRIM(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col002') IS NOT NULL THEN get_json_object(BODY, '$.col002') ELSE '' END),'[\n\r]',' ')) AS COL001,

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
  AND REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ')='11' AND DSID = 'service_activity_log'

  )
  a

  )
  b



  LEFT JOIN
  (
      SELECT
      name_dmp,
      addr_dmp,
      ca_name,
      cb_name,
      cc_name,
      cd_name
      FROM
      dmp.prod_tmap_poi_info_v
      WHERE
      name_dmp is not null and name_dmp <> '' and addr_dmp is not null and addr_dmp <> ''
  )
  c
  ON b.address = c.addr_dmp and b.destination = c.name_dmp
  ;




