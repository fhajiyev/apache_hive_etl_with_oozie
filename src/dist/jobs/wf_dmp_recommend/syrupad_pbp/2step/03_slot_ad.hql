USE user_dmp;

--set tez.queue.name=COMMON
--set mapreduce.job.queuename=COMMON;
--set hive.execution.engine=tez

DROP TABLE user_dmp.slot_ad_${oozie_job_name};

CREATE TABLE user_dmp.slot_ad_${oozie_job_name} AS
SELECT m_slot, client_type, ads
  FROM svc_ds_dmp.svc_event_log_${oozie_job_name}
GROUP BY m_slot, client_type, ads;

