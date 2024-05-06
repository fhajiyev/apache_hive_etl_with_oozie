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

-- Yuhan Kimberly Demographics

INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daybefore}00', data_source_id='75')

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
  '${hivevar:daybefore}00',
  '${hivevar:daybefore}00'

  from (
    select
      REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(


               CASE
               WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(GAID)',GAID)
               WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(IDFA)',IDFA)
               ELSE DMP_UID
               END


               ,'\n',''),'\,',' '),'\;',' '),'\:',' ') AS DMP_UID,

      split(reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(body, '$.col012') IS NOT NULL THEN get_json_object(body, '$.col012') ELSE '' END), ',') as MEMBER_TYPES,
      reflect('java.net.URLDecoder', 'decode', CASE WHEN get_json_object(body, '$.col011') IS NOT NULL THEN get_json_object(body, '$.col011') ELSE '' END) as GRADE,
      CASE WHEN get_json_object(body, '$.col010') IS NOT NULL THEN get_json_object(body, '$.col010') ELSE '' END as CHLD_AGE

    FROM dmp.log_server_idsync_collect
    WHERE
    part_hour between '${hivevar:day90before}00' and '${hivevar:daybefore}24'
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

  ) t
 lateral view explode(t.member_types) member_types_view as member_type
 WHERE ((MEMBER_TYPE IS NOT NULL AND MEMBER_TYPE <> '')OR(GRADE IS NOT NULL AND GRADE <> '')OR(CHLD_AGE IS NOT NULL AND CHLD_AGE <> ''))
 ;

  ALTER TABLE dmp.prod_data_source_store DROP IF EXISTS PARTITION (data_source_id='75', part_hour>='${hivevar:day2before}00', part_hour<='${hivevar:day2before}24');
  ALTER TABLE dmp.prod_data_source_store_parquet DROP IF EXISTS PARTITION (data_source_id='75', part_hour>='${hivevar:day2before}00', part_hour<='${hivevar:day2before}24');

