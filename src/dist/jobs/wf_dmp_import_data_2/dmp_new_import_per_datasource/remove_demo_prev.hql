
use dmp_pi;

set hivevar:day2before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='1');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='1');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='10');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='10');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='18');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='18');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='19');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='19');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='20');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='20');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='21');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='21');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='23');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='23');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='32');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='32');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='35');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='35');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='36');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='36');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='37');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='37');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='38');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='38');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='41');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='41');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='44');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='44');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='43');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='43');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='48');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='48');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='51');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='51');

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day2before}00', data_source_id='54');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day2before}00', data_source_id='54');

