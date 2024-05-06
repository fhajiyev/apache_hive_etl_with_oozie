

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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='40')

 select distinct
 ids.uid,
 att.dt,
 att.std_tm,
 att.week,
 case when att.action = '06' then '01' else '02' end,
 att.mkt_id,
 att.mkt_nm,
 att.taid,
 att.aname,
 att.lm,
 att.mm,
 att.sm,
 att.cnames,
 att.seg_id,
 att.seg_nm,
 '', '', '', '', '', '',
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

 from dmp_pi.solution_pre_mkt_all att
 join
 (
   select ci as uid, concat('OCB_', ocb_id) as mbr_id from dmp_pi.id_pool where ocb_id <> '' and part_date='${hivevar:day2before}'
   union all
   select ci as uid, concat('SYRUP_', sw_id) as mbr_id from dmp_pi.id_pool where sw_id <> '' and part_date='${hivevar:day2before}'
 ) ids
 on
 (ids.mbr_id = att.mbr_id)
 where att.part_date = '${hivevar:day2before}' and att.action in ('06','07')
;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='40');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='40');



