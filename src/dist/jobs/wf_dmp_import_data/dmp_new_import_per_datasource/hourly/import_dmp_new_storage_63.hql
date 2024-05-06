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



-- SK Planet 11st

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='63')
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
      ELSE a.PROD_NM
  END AS ITEM,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('search') THEN ''
      ELSE
      TRIM(
        CONCAT(
         CASE WHEN a.CATEGORY_CD1 IS NULL OR TRIM(a.CATEGORY_CD1) IN ('', 'null') THEN ''
         ELSE TRIM(SUBSTR(a.CATEGORY_CD1,(INSTR(a.CATEGORY_CD1, ':')+1)))
         END,
         CASE WHEN a.CATEGORY_CD2 IS NULL OR TRIM(a.CATEGORY_CD2) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(SUBSTR(a.CATEGORY_CD2,(INSTR(a.CATEGORY_CD2, ':')+1))))
         END,
         CASE WHEN a.CATEGORY_CD3 IS NULL OR TRIM(a.CATEGORY_CD3) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(SUBSTR(a.CATEGORY_CD3,(INSTR(a.CATEGORY_CD3, ':')+1))))
         END,
         CASE WHEN a.CATEGORY_CD4 IS NULL OR TRIM(a.CATEGORY_CD4) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(SUBSTR(a.CATEGORY_CD4,(INSTR(a.CATEGORY_CD4, ':')+1))))
         END
         )
         )
  END AS CATEGORY,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION = 'search' THEN a.ID
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

  '',
  '',
  '',
  '',
  '',
  '',

  a.GENCLASS,
  a.AGE,

  '', '',
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

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD3,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD4,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD5,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_CD6,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('search','welcome','sdest','rdest','fdest') THEN REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col007') IS NOT NULL THEN get_json_object(BODY, '$.col007') ELSE '' END),'[\n\r]',' ')
            ELSE ''
        END AS ID,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('view', 'basket', 'order', 'wish') THEN get_json_object(BODY, '$.col007')
            ELSE ''
        END AS ITEM_CODE,

        CASE
            WHEN get_json_object(BODY, '$.col005') IN ('view', 'basket', 'order', 'wish') THEN get_json_object(BODY, '$.col015')
            ELSE ''
        END AS AMOUNT,

        CASE
            WHEN get_json_object(BODY, '$.col005') = 'view' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col021')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('0','10','20','30','40','50','60','70','80','90')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col021')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END
            ELSE ''
        END AS AGE,

        CASE
            WHEN get_json_object(BODY, '$.col005') = 'view' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col020')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col020')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END
            ELSE ''
        END AS GENCLASS,

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
  AND REGEXP_REPLACE(get_json_object(BODY, '$.col001'),'[\n\,\;\:\r+]',' ')='2' AND DSID = 'service_activity_log'

  )
  a;




  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='63')
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
      ELSE a.PROD_NM
  END AS ITEM,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('search') THEN ''
      ELSE
      TRIM(
        CONCAT(
         CASE WHEN a.CATEGORY_NM1 IS NULL OR TRIM(a.CATEGORY_NM1) IN ('', 'null') THEN ''
         ELSE TRIM(SUBSTR(a.CATEGORY_NM1,(INSTR(a.CATEGORY_NM1, ':')+1)))
         END,
         CASE WHEN a.CATEGORY_NM2 IS NULL OR TRIM(a.CATEGORY_NM2) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(SUBSTR(a.CATEGORY_NM2,(INSTR(a.CATEGORY_NM2, ':')+1))))
         END,
         CASE WHEN a.CATEGORY_NM3 IS NULL OR TRIM(a.CATEGORY_NM3) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(SUBSTR(a.CATEGORY_NM3,(INSTR(a.CATEGORY_NM3, ':')+1))))
         END,
         CASE WHEN a.CATEGORY_NM4 IS NULL OR TRIM(a.CATEGORY_NM4) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(SUBSTR(a.CATEGORY_NM4,(INSTR(a.CATEGORY_NM4, ':')+1))))
         END
         )
         )
  END AS CATEGORY,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('search') THEN a.ID
      ELSE ''
  END AS KEYWORD,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('search') THEN ''
      ELSE a.CATEGORY_CD
  END AS ITEM_CODE,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('search') THEN ''
      ELSE a.AMOUNT
  END AS AMOUNT,

  CASE
      WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('basket','order') THEN a.ITEM_OPT
      ELSE ''
  END AS ITEM_OPT,

  CASE
       WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('order') THEN a.DEST_NM
       ELSE ''
  END AS DEST_NM,

  CASE
       WHEN a.ACTION IS NOT NULL AND a.ACTION IN ('order') THEN
       CASE
                            WHEN a.DEST_ADDR is null THEN ''
       
                            WHEN a.DEST_ADDR like '서울%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'서울특별시','서울')
       
                            WHEN a.DEST_ADDR like '인천%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'인천광역시','인천')
                            WHEN a.DEST_ADDR like '울산%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'울산광역시','울산')
                            WHEN a.DEST_ADDR like '부산%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'부산광역시','부산')
                            WHEN a.DEST_ADDR like '대전%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'대전광역시','대전')
                            WHEN a.DEST_ADDR like '대구%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'대구광역시','대구')
                            WHEN a.DEST_ADDR like '광주%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'광주광역시','광주')
       
                            WHEN a.DEST_ADDR like '세종%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'세종특별자치시','세종')
                            WHEN a.DEST_ADDR like '제주%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'제주특별자치도','제주')
       
                            WHEN a.DEST_ADDR like '강원%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'강원도','강원')
                            WHEN a.DEST_ADDR like '경기%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'경기도','경기')
       
                            WHEN a.DEST_ADDR like '전라남도%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'전라남도','전남')
                            WHEN a.DEST_ADDR like '전라북도%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'전라북도','전북')
                            WHEN a.DEST_ADDR like '충청남도%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'충청남도','충남')
                            WHEN a.DEST_ADDR like '충청북도%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'충청북도','충북')
                            WHEN a.DEST_ADDR like '경상남도%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'경상남도','경남')
                            WHEN a.DEST_ADDR like '경상북도%'        THEN REGEXP_REPLACE(a.DEST_ADDR,'경상북도','경북')
                            ELSE ''
       END       
       ELSE ''
  END AS DEST_ADDR,

  MOD_NAME,
  MOD_MANUF,
  IF(TRIM(SERV_NAME) = 'LG U', 'LG U+', SERV_NAME),

  a.GENCLASS,
  a.AGE,

  '', '',
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
        get_json_object(BODY, '$.col019') AS CHANNEL,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col004') IS NOT NULL THEN get_json_object(BODY, '$.col004') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS PROD_NM,

        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col009') IS NOT NULL THEN get_json_object(BODY, '$.col009') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_NM1,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col010') IS NOT NULL THEN get_json_object(BODY, '$.col010') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_NM2,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col011') IS NOT NULL THEN get_json_object(BODY, '$.col011') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_NM3,
        REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col012') IS NOT NULL THEN get_json_object(BODY, '$.col012') ELSE '' END),'[\n\r]',' '), '\\\\\\/','\\/') AS CATEGORY_NM4,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col002') IS NOT NULL THEN get_json_object(BODY, '$.col002') ELSE '' END),'[\n\r]',' ') AS ID,

        CASE WHEN get_json_object(BODY, '$.col003') IS NOT NULL THEN get_json_object(BODY, '$.col003') ELSE '' END AS CATEGORY_CD,

        CASE WHEN get_json_object(BODY, '$.col014') IS NOT NULL THEN get_json_object(BODY, '$.col014') ELSE '' END AS AMOUNT,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col013') IS NOT NULL THEN get_json_object(BODY, '$.col013') ELSE '' END),'[\n\r]',' ') AS ITEM_OPT,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col015') IS NOT NULL THEN get_json_object(BODY, '$.col015') ELSE '' END),'[\n\r]',' ') AS DEST_NM,

        REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(BODY, '$.col016') IS NOT NULL THEN get_json_object(BODY, '$.col016') ELSE '' END),'[\n\r]',' ') AS DEST_ADDR,

        UPPER(REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col021') is null or get_json_object(BODY, '$.col021') IN ( '' , '%7B%7BmodelNm%7D%7D'          , '%7B%7Bdevice_model%7D%7D'         ), '', get_json_object(BODY, '$.col021')))) AS MOD_NAME,
        UPPER(REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col022') is null or get_json_object(BODY, '$.col022') IN ( '' , '%7B%7BmanufacturerNm%7D%7D'  , '%7B%7Bdevice_manufacturer%7D%7D' ), '', get_json_object(BODY, '$.col022')))) AS MOD_MANUF,
        UPPER(REFLECT('java.net.URLDecoder', 'decode', if(get_json_object(BODY, '$.col023') is null or get_json_object(BODY, '$.col023') IN ( '' , '%7B%7BserviceProvider%7D%7D' , '%7B%7Btelecom_name%7D%7D'         ), '', get_json_object(BODY, '$.col023')))) AS SERV_NAME,


        CASE
            WHEN get_json_object(BODY, '$.col001') = 'view' THEN
            CASE
                WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col018')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('0','10','20','30','40','50','60','70','80','90')
                THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col018')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
                ELSE ''
            END
            ELSE ''
        END AS AGE,

        CASE
           WHEN get_json_object(BODY, '$.col001') = 'view' THEN
           CASE
               WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col017')),'\n',''),'\,',' '),'\;',' '),'\:',' ') IN ('4','8')
               THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', get_json_object(BODY, '$.col017')),'\n',''),'\,',' '),'\;',' '),'\:',' ')
               ELSE ''
           END
           ELSE ''
        END AS GENCLASS,

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
  AND DSID = '63'

  )
  a;





