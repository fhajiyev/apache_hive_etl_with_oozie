USE svc_ds_dmp;

set hive.mapred.local.mem=16384;

--set tez.queue.name=COMMON;
set mapreduce.job.queuename=SKPQ1;
--set hive.execution.engine=tez;
set mapreduce.job.reduces=128;
set hive.execution.engine=mr;

-- Make Service Table Data
INSERT INTO TABLE svc_ds_dmp.tmp_recommend_${oozie_job_name}
PARTITION (recommend_type='1')
SELECT b.uuid, c.site_code, c.product_no, a.score, '${jobId}', FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyyMMdd')
  FROM (
        SELECT uuid_sq, prod_sq, REGEXP_REPLACE(score, '^\\[|\\]$', '') AS score 
          FROM user_dmp.mahout_output_${oozie_job_name}
        LATERAL VIEW EXPLODE(value) map_value_table AS prod_sq, score
       ) a
  JOIN user_dmp.mahout_uuid_uuid_sq_mapping_${oozie_job_name} b ON b.uuid_sq = a.uuid_sq
  JOIN user_dmp.mahout_prod_no_prod_sq_mapping_${oozie_job_name} c ON c.prod_sq = a.prod_sq;

