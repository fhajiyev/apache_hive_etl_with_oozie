USE user_dmp;

--set tez.queue.name=COMMON
--set mapreduce.job.queuename=COMMON;
--set hive.execution.engine=tez


-- Mahout Input 생성
INSERT OVERWRITE TABLE user_dmp.mahout_syrupad_input_${oozie_job_name}
SELECT b.duid_sq, a.ads, a.score
  FROM svc_ds_dmp.svc_event_log_${oozie_job_name} a
    JOIN user_dmp.mahout_duid_duid_sq_mapping_${oozie_job_name} b ON a.d_uid = b.d_uid;



-- 머하웃 구동

