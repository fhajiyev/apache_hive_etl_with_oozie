



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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='52')
select

  a.ci,
  b.base_dt,
  case from_unixtime(unix_timestamp(b.base_dt, 'yyyyMMdd'), 'u')
      when 1 then 'mon'
      when 2 then 'tue'
      when 3 then 'wed'
      when 4 then 'thu'
      when 5 then 'fri'
      when 6 then 'sat'
      else 'sun'
  end,
  b.event_id,
  b.event_nm,
  b.url,
  b.page_id,
  if(b.actn_id is null or b.actn_id NOT IN ('btn.earn_point','btn.enter_event','ba.enter_event','cp.enter_event'), '이벤트페이지 방문', b.actn_id),

  '','','',
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

  from

  (
      select
      ci,
      ocb_id
      from
      dmp_pi.id_pool
      where part_date = '${hivevar:day2before}'

   ) a

   INNER JOIN

   (
      select
      mbr_id,
      base_dt,
      event_id,
      event_nm,
      url,
      page_id,
      actn_id

      from
      ocb.mart_app_event_ctnt
      where
      base_dt = '${hivevar:day2before}'
      and
      mbr_id IS NOT NULL AND mbr_id <> ''

   ) b

   ON
   a.ocb_id = b.mbr_id;


alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='52');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='52');
