USE user_dmp;

--set tez.queue.name=COMMON
--set mapreduce.job.queuename=COMMON;
--set hive.execution.engine=tez

-- Users 파일 생성
INSERT OVERWRITE TABLE user_dmp.mahout_syrupad_users_${oozie_job_name}
SELECT duid_sq
  FROM user_dmp.mahout_duid_duid_sq_mapping_${oozie_job_name};

