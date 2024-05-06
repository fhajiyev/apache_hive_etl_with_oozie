set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour, data_source_id)
select uid, abgroup, 
'', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
part_hour,
'23' as data_source_id
from dmp_pi.prod_uid_life
where part_hour=from_unixtime(unix_timestamp() - 2 * 3600 *24, 'yyyyMMdd00');

