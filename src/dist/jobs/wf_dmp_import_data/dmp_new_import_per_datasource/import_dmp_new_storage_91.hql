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

  -- SMS Payment

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:day2before}00', data_source_id='91')
  SELECT
  CONCAT('(GAID)', a.GAID) as uid,
  a.SPENT_DATE,
  '',
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
    WHEN CAST(a.AMNT AS INT) >= 0 THEN 'pay'
    ELSE 'cancel'
  END AS ACTION,

  if(a.SENDER is null, '', a.SENDER),

  if(a.CARD_NAME is null, '', a.CARD_NAME),

  if(a.CARD_TYPE is null, '', a.CARD_TYPE),

  if(a.CARD_SUB_TYPE is null, '', a.CARD_SUB_TYPE),

  if(a.NAMEVAL is null, '', a.NAMEVAL),

  a.AMNT,

  if(a.CURRENCY is null, '', a.CURRENCY),

  REGEXP_REPLACE(a.CATE_NAME,  '\;','||'),

  if(a.ALLIANCE_NM is null, '', a.ALLIANCE_NM),

  CASE
     WHEN sido_loc.local_nm IS NULL OR trim(sido_loc.local_nm) = '' THEN ''
     ELSE
     CASE
        WHEN sigungu_loc.local_nm IS NULL OR trim(sigungu_loc.local_nm) = '' THEN sido_loc.local_nm
        ELSE
        CASE
           WHEN dong_loc.local_nm IS NULL OR trim(dong_loc.local_nm) = '' THEN CONCAT(sido_loc.local_nm, ' ', sigungu_loc.local_nm)
           ELSE
           CONCAT(sido_loc.local_nm, ' ', sigungu_loc.local_nm, ' ', dong_loc.local_nm)
        END
     END
  END,

  if(SPLIT(a.NAMEVAL, ' +')[0] is null, '', TRIM(SPLIT(a.NAMEVAL, ' +')[0])),

  if(SPLIT(a.NAMEVAL, ' +')[1] is null, '', TRIM(SPLIT(a.NAMEVAL, ' +')[1])),

  if(SPLIT(a.NAMEVAL, ' +')[2] is null, '', TRIM(SPLIT(a.NAMEVAL, ' +')[2])),


  if(sido_loc.local_nm    IS NULL OR trim(sido_loc.local_nm)    = '', '', sido_loc.local_nm),

  if(sido_loc.local_nm    IS NULL OR trim(sido_loc.local_nm)    = '' OR sigungu_loc.local_nm IS NULL OR trim(sigungu_loc.local_nm) = '', '', CONCAT(sido_loc.local_nm, ' ', sigungu_loc.local_nm)),

  if(sido_loc.local_nm    IS NULL OR trim(sido_loc.local_nm)    = '' OR sigungu_loc.local_nm IS NULL OR trim(sigungu_loc.local_nm) = '' OR dong_loc.local_nm IS NULL OR trim(dong_loc.local_nm) = '', '', CONCAT(sido_loc.local_nm, ' ', sigungu_loc.local_nm, ' ', dong_loc.local_nm)),


  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '${hivevar:day2before}00',
  '${hivevar:day2before}00'

  FROM

  (

      SELECT
      GAID,
      PART_DATE,
      from_unixtime(unix_timestamp(SPENT_DATE,'yyyyMMdd'),'u') AS WEEKDAY,
      SPENT_DATE,
      SENDER,
      CARD_NAME,
      CARD_TYPE,
      CARD_SUB_TYPE,
      CASE
          WHEN NAME IS NULL OR NAME = '' THEN TRIM(SELLER)
          ELSE TRIM(NAME)
      END AS NAMEVAL,
      REGEXP_REPLACE(AMOUNT, ',', '') AS AMNT,
      CURRENCY,

      if(CATE_NAME is null, '', CATE_NAME) AS CATE_NAME,
      ALLIANCE_NM,
      SUBSTR(GEO_CODE,2,8) as GEO_CODE

      FROM

      SVC_CONTEXT_ANO.ITM_PAYMENT_MESSAGES_MAPPED

      WHERE
      PART_DATE = '${hivevar:day2before}' AND
      amount IS NOT NULL AND amount <> ''

  ) a

  left join

  (
            select
            local_cd as hc_sido,
            REGEXP_REPLACE(local_nm,  '세종시','세종') as local_nm
            from
            imc.dbm_m2_merch_local_a_v
            where
            length(local_cd) = 2
  ) sido_loc

  on sido_loc.hc_sido = SUBSTR(A.geo_code, 1, 2)

  left join

  (
            select
            local_cd as hc_sigungu,
            local_nm
            from
            imc.dbm_m2_merch_local_a_v
            where
            length(local_cd) = 5
  ) sigungu_loc

  on sigungu_loc.hc_sigungu = SUBSTR(A.geo_code, 1, 5)

  left join

  (
            select
            local_cd as hc_dong,
            local_nm
            from
            imc.dbm_m2_merch_local_a_v
            where
            length(local_cd) = 8
  ) dong_loc

  on dong_loc.hc_dong = A.geo_code

  ;


