USE dmp;


set hivevar:hourbefore;
set hivevar:hourcurrent;
set hivevar:hour91before;
set hivevar:hour2before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

set hive.exec.dynamic.partition.mode=true;

 insert overwrite table dmp_pi.prod_data_source_store_parquet PARTITION (part_hour,data_source_id)

 select * from dmp_pi.prod_data_source_store t where t.part_hour = '${hivevar:hour2before}' and t.data_source_id IN (select distinct datasourceid from svc_ds_dmp_pi.prod_audience_upload where parthour = '${hivevar:hourbefore}')
 ;

