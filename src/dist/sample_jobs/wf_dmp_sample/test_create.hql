USE ${DB};

set tez.queue.name=COMMON;
set mapreduce.job.queuename=COMMON;
set hive.execution.engine=tez;
set mapreduce.job.reduces=128;

DROP TABLE IF EXISTS user_tadsvc.log_test;

CREATE TABLE user_tadsvc.log_test
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
AS
SELECT log_time, action, uid, site 
FROM tad.log_server_tracking
WHERE part_hour = '2016012019'
LIMIT 5;

