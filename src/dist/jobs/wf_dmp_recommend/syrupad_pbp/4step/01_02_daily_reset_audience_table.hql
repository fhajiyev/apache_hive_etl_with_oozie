USE svc_ds_dmp;

--set tez.queue.name=COMMON
--set mapreduce.job.queuename=COMMON
--set hive.execution.engine=tez


--reset every day at 4 am

INSERT INTO TABLE svc_ds_dmp.tmp_syrupad_recommend_${oozie_job_name} SELECT * FROM svc_ds_dmp.svc_syrupad_recommend_${oozie_job_name} WHERE ((FROM_UNIXTIME(UNIX_TIMESTAMP(), 'HH:mm:ss') >= '06:00:00' AND FROM_UNIXTIME(UNIX_TIMESTAMP(), 'HH:mm:ss') <= '23:59:59')OR(FROM_UNIXTIME(UNIX_TIMESTAMP(), 'HH:mm:ss') >= '00:00:00' AND FROM_UNIXTIME(UNIX_TIMESTAMP(), 'HH:mm:ss') <= '04:00:00'));



