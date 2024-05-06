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


-- Recopick Recopick

INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:hourbefore}', data_source_id='72')
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
      a.GENCLASS,
      a.AGE,
      cast(a.PRICE      as decimal),
      cast(a.TOTALSALES as decimal),
      os,
      device,

      '','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      '','','','','','','','','','',
      a.PART_HOUR,
      a.LOG_TIME

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

  ACTION,

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
       when ACTION = 'search' THEN if(keyword is null, '', keyword)
       else ''
  end AS KEYWORD,

  case
       when get_json_object(item, '$.id') is null then ''
       else TRIM(get_json_object(item, '$.id'))
  end AS ITEMCODE,


                 case
                      when birthyear is null or birthyear = '' then ''
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 18 and 19 then '18-19'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 20 and 24 then '20-24'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 25 and 29 then '25-29'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 30 and 34 then '30-34'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 35 and 39 then '35-39'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 40 and 44 then '40-44'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 45 and 49 then '45-49'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 50 and 54 then '50-54'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 55 and 59 then '55-59'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 60 and 64 then '60-64'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 65 and 69 then '65-69'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 between 70 and 74 then '70-74'
                      when cast(substr('${hivevar:hourbefore}', 1, 4)-birthyear as int)+1 >= 75               then '75+'
                      else ''
                 end AS AGE,

                 case
                      when gender is not null and gender in ('M','10') then 'Male'
                      when gender is not null and gender in ('F','20') then 'Female'
                      else ''
                 end AS GENCLASS,

  case
      when ACTION IN ('view','like') then if(get_json_object(item, '$.price') is null, '', get_json_object(item, '$.price'))
      else ''
  end AS PRICE,

  case
      when ACTION = 'order' then if(get_json_object(item, '$.total_sales') is null, '', get_json_object(item, '$.total_sales'))
      else ''
  end AS TOTALSALES,

  os,
  device,

  log_time, '${hivevar:hourbefore}' as part_hour

from (
  select
    trim(get_json_object(body, '$.dmpuid')) as dmpc,
    log_time,
    sid as site_id,
    os,
    device,

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

    if(get_json_object(body, '$.tag_items') is null, array('-1'), split(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(get_json_object(body, '$.tag_items'), '\\[', ''), '\\]', ''), '\\"\\{', '\\{'), '\\}\\"', '\\}'), '\\\\\"', '\\"'), '\\},\\{', '\\},,,,\\{'), ',,,,')) as items,
    trim(get_json_object(body, '$.gaid')) as gaid,
    trim(get_json_object(body, '$.idfa')) as idfa,
    trim(get_json_object(body, '$.q')) as keyword,
    trim(get_json_object(body, '$.birthyear')) as birthyear,
    trim(get_json_object(body, '$.gender')) as gender
  from dmp.log_server_recopick
  where
  part_hour = '${hivevar:hourbefore}'
  and ((get_json_object(body, '$.dmpuid') is not null and get_json_object(body, '$.dmpuid') <> '' and SUBSTR(get_json_object(body, '$.dmpuid'),1,6) = '(DMPC)' AND trim(get_json_object(body, '$.dmpuid')) <> '(DMPC)00000000-0000-0000-0000-000000000000') OR (get_json_object(body, '$.gaid') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.gaid')))=36 AND trim(get_json_object(body, '$.gaid'))=LOWER(trim(get_json_object(body, '$.gaid'))) AND trim(get_json_object(body, '$.gaid')) <> '00000000-0000-0000-0000-000000000000') OR (get_json_object(body, '$.idfa') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.idfa')))=36 AND trim(get_json_object(body, '$.idfa'))<>LOWER(trim(get_json_object(body, '$.idfa'))) AND trim(get_json_object(body, '$.idfa')) <> '00000000-0000-0000-0000-000000000000'))
  and length(sid) <= 20
) t
lateral view explode(t.items) items_view as item
where site_id not in ('18', '47', '76', '204', '212', '213', '216', '1865', '1868', '1649', '1652', '1850', '1851')

)
a
LEFT JOIN
SVC_DS_DMP.PROD_RECOPICK_SERVICE_ITEM_ID_TO_ITEM_META_MAP_V b
ON
a.SITE = b.SITE_ID AND a.ITEMCODE = b.ITEM_ID
;




