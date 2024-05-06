USE user_dmp;

--set tez.queue.name=SKPQ1;
set mapreduce.job.queuename=SKPQ1;
--set hive.execution.engine=tez;
set hive.execution.engine=mr;

-- Users 파일 생성
INSERT OVERWRITE TABLE user_dmp.mahout_users_${oozie_job_name}
SELECT uuid_sq
  FROM user_dmp.mahout_uuid_uuid_sq_mapping_${oozie_job_name}
DISTRIBUTE BY HASH(uuid_sq)
SORT BY uuid_sq;

