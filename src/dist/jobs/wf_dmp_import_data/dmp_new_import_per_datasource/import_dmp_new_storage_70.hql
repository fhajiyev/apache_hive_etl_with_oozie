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

-- Recopick Demographics

INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='70')

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
                     WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 THEN CONCAT('(GAID)',GAID)
                     WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 THEN CONCAT('(IDFA)',IDFA)
                     WHEN SUBSTR(DMPC,1,6)='(DMPD)' AND LENGTH(DMPC)=42 AND SUBSTR(DMPC,7) =  LOWER(SUBSTR(DMPC,7)) THEN CONCAT('(GAID)',SUBSTR(DMPC,7))
                     WHEN SUBSTR(DMPC,1,6)='(DMPD)' AND LENGTH(DMPC)=42 AND SUBSTR(DMPC,7) <> LOWER(SUBSTR(DMPC,7)) THEN CONCAT('(IDFA)',SUBSTR(DMPC,7))
                     ELSE DMPC
               END AS UID,

               case
                    when birthyear is null or birthyear = '' then ''
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 18 and 19 then '18-19'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 20 and 24 then '20-24'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 25 and 29 then '25-29'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 30 and 34 then '30-34'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 35 and 39 then '35-39'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 40 and 44 then '40-44'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 45 and 49 then '45-49'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 50 and 54 then '50-54'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 55 and 59 then '55-59'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 60 and 64 then '60-64'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 65 and 69 then '65-69'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 between 70 and 74 then '70-74'
                    when cast(substr('${hivevar:daybefore}', 1, 4)-birthyear as int)+1 >= 75               then '75+'
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
                      sid as site_id,
                      log_time
                    from dmp.log_server_recopick
                    where
                    part_hour between '${hivevar:day90before}00' and '${hivevar:daybefore}24'
                    and ((get_json_object(body, '$.dmpuid') is not null and get_json_object(body, '$.dmpuid') <> '' and SUBSTR(get_json_object(body, '$.dmpuid'),1,6) = '(DMPC)' AND trim(get_json_object(body, '$.dmpuid')) <> '(DMPC)00000000-0000-0000-0000-000000000000') OR (get_json_object(body, '$.gaid') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.gaid')))=36 AND trim(get_json_object(body, '$.gaid'))=LOWER(trim(get_json_object(body, '$.gaid'))) AND trim(get_json_object(body, '$.gaid')) <> '00000000-0000-0000-0000-000000000000') OR (get_json_object(body, '$.idfa') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.idfa')))=36 AND trim(get_json_object(body, '$.idfa'))<>LOWER(trim(get_json_object(body, '$.idfa'))) AND trim(get_json_object(body, '$.idfa')) <> '00000000-0000-0000-0000-000000000000'))
                    and length(sid) <= 20
                   ) t
                   where site_id not in ('18', '47', '76', '204', '212', '213', '216', '1865', '1868', '1649', '1652', '1850', '1851')

           ) AS T1
           WHERE T1.AGE IS NOT NULL AND T1.AGE <> '' AND T1.GENCLASS IS NOT NULL AND T1.GENCLASS <> ''

       ) AS T2
       WHERE  T2.RN = 1

  ;



