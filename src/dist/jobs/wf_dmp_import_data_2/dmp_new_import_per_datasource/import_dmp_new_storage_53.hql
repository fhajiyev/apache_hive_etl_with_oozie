
set hivevar:day2before;
set hivevar:day92before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='53')

  SELECT

  UID,

  job,
  hobby,
  income,
  house_price,
  commuter,
  device_change,
  operator_change,
  house_moved,
  isp,
  commercial_zone,
  comm_zone1,
  comm_zone2,
  comm_zone3,
  comm_zone4,
  comm_zone5,
  activity_area,
  residence,

  part_dt,
  os_name,

  '',
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
  svc_ds_dmp_pi.prod_integr_context_tbl;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day92before}00', data_source_id='53');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day92before}00', data_source_id='53');

