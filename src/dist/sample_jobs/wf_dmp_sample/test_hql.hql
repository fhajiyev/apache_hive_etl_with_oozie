USE ${db_name};

set tez.queue.name=COMMON;
set mapreduce.job.queuename=COMMON;
set hive.execution.engine=tez;
set mapreduce.job.reduces=128;

SELECT * 
FROM ${db_name}.${table_name}
LIMIT 10
;
