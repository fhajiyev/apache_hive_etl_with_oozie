
use dmp;

set hivevar:daycurr;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='62')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='62')

 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='69')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='69')

 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='70')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='70')

 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='90')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='90')

 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='92')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='92')

 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='94')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='94')

 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='100')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='100')

 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='120')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='120')

 -- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='121')
 -- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='121')

-- alter table dmp.prod_data_source_store drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='129')
-- alter table dmp.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:daycurr}00', data_source_id='129')


