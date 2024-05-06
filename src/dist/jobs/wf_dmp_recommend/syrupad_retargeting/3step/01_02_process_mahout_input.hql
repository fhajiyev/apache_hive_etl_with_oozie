USE user_dmp;

set hive.mapred.local.mem=8192;

--set tez.queue.name=SKPQ1;
set mapreduce.job.queuename=SKPQ1;
--set hive.execution.engine=tez;
set mapreduce.job.reduces=128;
set hive.execution.engine=mr;

-- Mahout Input 생성
INSERT OVERWRITE TABLE user_dmp.mahout_input_${oozie_job_name}
SELECT b.uuid_sq, c.prod_sq, a.score
  FROM svc_ds_dmp.svc_retargeting_log_${oozie_job_name} a
  JOIN user_dmp.mahout_uuid_uuid_sq_mapping_${oozie_job_name} b ON a.uid = b.uuid
  JOIN user_dmp.mahout_prod_no_prod_sq_mapping_${oozie_job_name} c ON a.id = c.product_no AND a.site = c.site_code
 WHERE a.score > 0;

-- 머하웃 구동

