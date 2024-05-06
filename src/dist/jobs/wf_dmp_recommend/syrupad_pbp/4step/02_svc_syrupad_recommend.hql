USE svc_ds_dmp;

--set tez.queue.name=COMMON
--set mapreduce.job.queuename=COMMON
--set hive.execution.engine=tez


--add low budget data

--INSERT INTO TABLE svc_ds_dmp.tmp_syrupad_recommend_${oozie_job_name} SELECT * FROM svc_ds_dmp.svc_syrupad_recommend_lowbudget_${oozie_job_name}



DROP TABLE IF EXISTS svc_ds_dmp.bak_syrupad_recommend_${oozie_job_name};

ALTER TABLE svc_syrupad_recommend_${oozie_job_name} RENAME TO bak_syrupad_recommend_${oozie_job_name};
ALTER TABLE tmp_syrupad_recommend_${oozie_job_name} RENAME TO svc_syrupad_recommend_${oozie_job_name};

