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

--  개인 Context 유추

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='100')
  SELECT DISTINCT

  CASE
      WHEN t.id_type = 'gaid' THEN CONCAT('(GAID)', t.ad_id)
      ELSE CONCAT('(IDFA)', t.ad_id)
  END,
  '',
  if(t.job_kind   is null, '', t.job_kind),
  if(t.hobby_kind is null, '', t.hobby_kind),
  if(t.hobby_freq is null, '', t.hobby_freq),
  if(t.comm_yn    is null, '', CASE WHEN t.comm_yn = 'Y' THEN 'commuter' ELSE '' END),
  if(t.income_val is null or t.income_val='', '', cast(t.income_val as decimal)),
  if(t.house_val  is null or t.house_val= '', '', cast(t.house_val  as decimal)),
  if(t.move_date  is null, '', t.move_date),

  '', '',
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
  AD_ID,
  a.KIND as job_kind,
  ''   as hobby_kind,
  ''   as hobby_freq,
  ''   as comm_yn,
  ''   as income_val,
  ''   as house_val,
  ''   as move_date,

  id_type

  FROM SVC_CONTEXT_ANO.PROD_BEHAVIOR_JOB a

  INNER JOIN
  (
   SELECT
   KIND,
   MAX(PART_DT) as maxdate
   FROM
   SVC_CONTEXT_ANO.PROD_BEHAVIOR_JOB
   GROUP BY KIND
  ) b

  ON a.kind = b.kind AND a.part_dt = b.maxdate

  UNION ALL

  SELECT
  AD_ID,
  ''    as job_kind,
  a.KIND  as hobby_kind,
  FREQ  as hobby_freq,
  ''    as comm_yn,
  ''    as income_val,
  ''    as house_val,
  ''    as move_date,

  id_type

  FROM SVC_CONTEXT_ANO.PROD_BEHAVIOR_HOBBY a

  INNER JOIN
  (
   SELECT
   KIND,
   MAX(PART_DT) as maxdate
   FROM
   SVC_CONTEXT_ANO.PROD_BEHAVIOR_HOBBY
   GROUP BY KIND
  ) b

  ON a.kind = b.kind AND a.part_dt = b.maxdate

  UNION ALL

  SELECT
  CASE
      WHEN A.AD_ID IS NOT NULL THEN A.AD_ID
      ELSE B.AD_ID
  END as AD_ID,
  ''          as job_kind,
  ''          as hobby_kind,
  ''          as hobby_freq,
  COMMUTER_YN as comm_yn,
  ''          as income_val,
  ''          as house_val,
  MOVE_DT     as move_date,

  CASE
      WHEN A.AD_ID IS NOT NULL THEN A.id_type
      ELSE B.id_type
  END as id_type

  FROM
          (
            SELECT
            AD_ID,
            VALUE AS COMMUTER_YN,
            id_type
            FROM SVC_CONTEXT_ANO.PROD_DEMOGRAPHIC_PERSONAL a
            WHERE
            a.KIND = 'commuter_yn'
            AND a.PART_DT IN (SELECT MAX(PART_DT) FROM SVC_CONTEXT_ANO.PROD_DEMOGRAPHIC_PERSONAL WHERE KIND = 'commuter_yn')

          ) A
          FULL JOIN
          (
            SELECT
            AD_ID,
            VALUE AS MOVE_DT,
            id_type
            FROM SVC_CONTEXT_ANO.PROD_DEMOGRAPHIC_PERSONAL a
            WHERE
            a.KIND = 'moved_person'
            AND a.PART_DT IN (SELECT MAX(PART_DT) FROM SVC_CONTEXT_ANO.PROD_DEMOGRAPHIC_PERSONAL WHERE KIND = 'moved_person')

          ) B ON A.AD_ID = B.AD_ID

  UNION ALL

  SELECT
  CASE
      WHEN A.AD_ID IS NOT NULL THEN A.AD_ID
      ELSE B.AD_ID
  END as AD_ID,
  ''           as job_kind,
  ''           as hobby_kind,
  ''           as hobby_freq,
  ''           as comm_yn,
  INCOME_VALUE as income_val,
  HOUSE_VALUE  as house_val,
  ''           as move_date,

  CASE
        WHEN A.AD_ID IS NOT NULL THEN A.id_type
        ELSE B.id_type
  END as id_type

  FROM

          (
            SELECT
            AD_ID,
            VALUE AS INCOME_VALUE,
            id_type
            FROM SVC_CONTEXT_ANO.PROD_DEMOGRAPHIC_PERSONAL a
            WHERE
            KIND = 'annual_income'
            AND a.PART_DT IN (SELECT MAX(PART_DT) FROM SVC_CONTEXT_ANO.PROD_DEMOGRAPHIC_PERSONAL WHERE KIND = 'annual_income')

          ) A
          FULL JOIN
          (
            SELECT
            AD_ID,
            VALUE AS HOUSE_VALUE,
            id_type
            FROM SVC_CONTEXT_ANO.PROD_DEMOGRAPHIC_PERSONAL a
            WHERE
            KIND = 'estimated_house_price'
            AND a.PART_DT IN (SELECT MAX(PART_DT) FROM SVC_CONTEXT_ANO.PROD_DEMOGRAPHIC_PERSONAL WHERE KIND = 'estimated_house_price')

          ) B ON A.AD_ID = B.AD_ID


  ) t
  WHERE t.AD_ID IS NOT NULL AND t.AD_ID <> '' AND t.id_type IS NOT NULL AND t.id_type IN ('gaid', 'idfa')
;


