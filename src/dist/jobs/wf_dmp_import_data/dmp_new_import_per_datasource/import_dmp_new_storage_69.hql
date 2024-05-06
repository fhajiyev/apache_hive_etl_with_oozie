USE dmp;


set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day90before;
set hivevar:day91before;
set hivevar:daycurr;


set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

-- SK Broadband Demographics

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='69')

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
     '${hivevar:daycurr}00',
     '${hivevar:daycurr}00'

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
                    WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(GAID)',GAID)
                    WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(IDFA)',IDFA)
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
                AND DSID IS NOT NULL AND DSID IN ('dmp-53')

             ) AS T1
             WHERE T1.AGE IS NOT NULL AND T1.AGE <> '' AND T1.GENCLASS IS NOT NULL AND T1.GENCLASS <> ''

     ) AS T2
     WHERE  T2.RN = 1

  ;



