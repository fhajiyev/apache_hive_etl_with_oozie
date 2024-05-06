


set hivevar:day2before;
set hivevar:day182before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='33')


SELECT
   mb.ci,
   ma.dt,
   '',
   CASE from_unixtime(unix_timestamp(ma.dt, 'yyyyMMdd'), 'u')
      WHEN 1 THEN 'mon'
      WHEN 2 THEN 'tue'
      WHEN 3 THEN 'wed'
      WHEN 4 THEN 'thu'
      WHEN 5 THEN 'fri'
      WHEN 6 THEN 'sat'
      ELSE 'sun'
   END,
   ma.action,
   '',
   ma.feed_id,
   ma.feed_name,
   '',

   '','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '${hivevar:day2before}00',
   '${hivevar:day2before}00'

   FROM


(

        SELECT
          mbr_id  as member_id,
          base_dt as dt,
          '01'   as action,
          feed_id as feed_id,
          feed_nm as feed_name

        FROM
               OCB.MART_APP_FEED_CLK_CTNT
        WHERE

               base_dt     = '${hivevar:day2before}'
               AND feed_exps_yn = '1'


        UNION ALL


        SELECT
          mbr_id  as member_id,
          base_dt as dt,
          '02'   as action,
          feed_id as feed_id,
          feed_nm as feed_name

        FROM
               OCB.MART_APP_FEED_CLK_CTNT

        WHERE
               base_dt    = '${hivevar:day2before}'
               AND feed_clk_yn = '1'

) ma


INNER JOIN

(

SELECT
ci,
ocb_id
from
dmp_pi.id_pool
where part_date = '${hivevar:day2before}'

) mb

ON

ma.member_id = mb.ocb_id

;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='33');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='33');

