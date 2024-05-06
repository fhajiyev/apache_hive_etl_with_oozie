USE dmp;


set hivevar:hourbefore;
set hivevar:hourcurrent;
set hivevar:hour10before;
set hivevar:hour91before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=true;

set hive.exec.max.dynamic.partitions=1000000;

 insert overwrite table dmp.prod_data_source_store_parquet PARTITION (part_hour,data_source_id)

 select * from dmp.prod_data_source_store where part_hour = '${hivevar:hourbefore}' and data_source_id IN ('63','64','65','67','68','71','72','77','82','89','97','117','118','113','114','115','116','123','124','137')
 ;

