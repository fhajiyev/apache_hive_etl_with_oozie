
USE SVC_DS_DMP;

set hivevar:daybefore;
set hivevar:day90before;
set hivevar:day5before;

set hivevar:hourbefore;
set hivevar:hourcurrent;
set hivevar:hour91before;

set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


 INSERT OVERWRITE TABLE SVC_DS_DMP.PROD_RECOPICK_SERVICE_ITEM_ID_TO_ITEM_META_MAP PARTITION (PART_DATE='${hivevar:daybefore}')
 SELECT
 SITE_ID,
 ITEM_ID,
 ITEM_NAME,
 REGEXP_REPLACE(
 REGEXP_REPLACE(
 REGEXP_REPLACE(
 REGEXP_REPLACE(
 REGEXP_REPLACE(
                split(ITEM_CAT,'\\|\\|p')[0], '&amp\;', '&'), '&apos\;', '\''), '&nbsp\;', '""'), '&quot\;', '"'), '&#039\;', '\'')
 FROM
 (
     SELECT
     SITE_ID,
     ITEM_ID,
     ITEM_NAME,
     ITEM_CAT,
     ROW_NUMBER() OVER(PARTITION BY SITE_ID, ITEM_ID ORDER BY TIMESTMP DESC ) AS RN
     FROM
     (
        SELECT

        log_time AS TIMESTMP,

        site_id AS SITE_ID,

        case
            when get_json_object(item, '$.id') is null then ''
            else TRIM(get_json_object(item, '$.id'))
        end AS ITEM_ID,

        CASE
              WHEN get_json_object(item, '$.title') IS NULL THEN ''
              WHEN site_id IN ('47') AND reflect('java.net.URLDecoder', 'decode', get_json_object(item, '$.title')) IS NOT NULL THEN TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(item, '$.title')))
              ELSE TRIM(get_json_object(item, '$.title'))
        END AS ITEM_NAME,

        TRIM
              (
                CONCAT
                 (

                 CASE
                 WHEN get_json_object(item, '$.c1') IS NULL THEN ''
                 WHEN site_id = '1919' THEN TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(item, '$.c1')))
                 ELSE TRIM(get_json_object(item, '$.c1'))
                 END,

                 CASE
                 WHEN get_json_object(item, '$.c2') IS NULL OR TRIM(get_json_object(item, '$.c2')) = '' THEN ''
                 WHEN site_id = '1919' THEN CONCAT('\||', TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(item, '$.c2'))))
                 ELSE CONCAT('\||', TRIM(get_json_object(item, '$.c2')))
                 END,

                 CASE
                 WHEN get_json_object(item, '$.c3') IS NULL OR TRIM(get_json_object(item, '$.c3')) = '' THEN ''
                 WHEN site_id = '1919' THEN CONCAT('\||', TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(item, '$.c3'))))
                 ELSE CONCAT('\||', TRIM(get_json_object(item, '$.c3')))
                 END,

                 CASE
                 WHEN get_json_object(item, '$.c4') IS NULL OR TRIM(get_json_object(item, '$.c4')) = '' THEN ''
                 WHEN site_id = '1919' THEN CONCAT('\||', TRIM(reflect('java.net.URLDecoder', 'decode', get_json_object(item, '$.c4'))))
                 ELSE CONCAT('\||', TRIM(get_json_object(item, '$.c4')))
                 END

                 )
              ) AS ITEM_CAT

        FROM
        (
         SELECT
         log_time,
         if(get_json_object(body, '$.tag_items') is null, array('-1'), split(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(get_json_object(body, '$.tag_items'), '\\[', ''), '\\]', ''), '\\"\\{', '\\{'), '\\}\\"', '\\}'), '\\\\\"', '\\"'), '\\},\\{', '\\},,,,\\{'), ',,,,')) as items,
         sid as site_id

         FROM
         DMP.LOG_SERVER_RECOPICK
         WHERE
         part_hour between '${hivevar:hour91before}' and '${hivevar:hourbefore}'
         and ((get_json_object(body, '$.dmpuid') is not null and get_json_object(body, '$.dmpuid') <> '' and SUBSTR(get_json_object(body, '$.dmpuid'),1,6) = '(DMPC)' AND trim(get_json_object(body, '$.dmpuid')) <> '(DMPC)00000000-0000-0000-0000-000000000000') OR (get_json_object(body, '$.gaid') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.gaid')))=36 AND trim(get_json_object(body, '$.gaid'))=LOWER(trim(get_json_object(body, '$.gaid'))) AND trim(get_json_object(body, '$.gaid')) <> '00000000-0000-0000-0000-000000000000') OR (get_json_object(body, '$.idfa') IS NOT NULL AND LENGTH(trim(get_json_object(body, '$.idfa')))=36 AND trim(get_json_object(body, '$.idfa'))<>LOWER(trim(get_json_object(body, '$.idfa'))) AND trim(get_json_object(body, '$.idfa')) <> '00000000-0000-0000-0000-000000000000'))
         and length(sid) <= 20

        ) t
        lateral view explode(t.items) items_view as item
        where site_id not in ('18', '47', '76', '204', '212', '213', '216', '1865', '1868', '1649', '1652', '1850', '1851')

        ) a
        WHERE LENGTH(ITEM_ID) > 0 AND ((LENGTH(ITEM_NAME) > 0) OR (LENGTH(ITEM_CAT) > 0))

 ) b
 WHERE RN=1;


 alter table svc_ds_dmp.prod_recopick_service_item_id_to_item_meta_map drop partition (part_date < '${hivevar:day5before}');

 DROP VIEW IF EXISTS SVC_DS_DMP.PROD_RECOPICK_SERVICE_ITEM_ID_TO_ITEM_META_MAP_V;
 CREATE VIEW SVC_DS_DMP.PROD_RECOPICK_SERVICE_ITEM_ID_TO_ITEM_META_MAP_V AS
 SELECT
 SITE_ID,
 ITEM_ID,
 ITEM_NAME,
 ITEM_CAT
 FROM
 SVC_DS_DMP.PROD_RECOPICK_SERVICE_ITEM_ID_TO_ITEM_META_MAP
 WHERE
 PART_DATE = '${hivevar:daybefore}'
 ;
