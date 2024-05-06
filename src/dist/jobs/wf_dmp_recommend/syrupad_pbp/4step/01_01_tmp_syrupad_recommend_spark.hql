USE svc_ds_dmp;

--set tez.queue.name=COMMON
--set mapreduce.job.queuename=COMMON
--set hive.execution.engine=tez

-- 에러가 발생한 경우 tmp만 존재할 수도 있음
DROP TABLE IF EXISTS svc_ds_dmp.tmp_syrupad_recommend_${oozie_job_name};

CREATE TABLE svc_ds_dmp.tmp_syrupad_recommend_${oozie_job_name} (
  rec_time    string,
  d_uid       string,
  m_slot      string,
  client_type string,
  ads         string,
  score       double,
  jobid       string,
  part_date   string
)
CLUSTERED BY (d_uid) SORTED BY (d_uid) INTO 256 BUCKETS;



-- Make Service Table Data from Spark output

INSERT INTO TABLE svc_ds_dmp.tmp_syrupad_recommend_${oozie_job_name} SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy/MM/dd HH:mm'), b.d_uid, c.m_slot, c.client_type, a.ad_sq, a.score, '${jobId}', FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyyMMdd') FROM ( SELECT duid_sq, ad_sq, REGEXP_REPLACE(score, '^\\[|\\]$', '') AS score FROM user_dmp.mahout_syrupad_output_${oozie_job_name} ) a JOIN user_dmp.mahout_duid_duid_sq_mapping_${oozie_job_name} b ON b.duid_sq = a.duid_sq JOIN user_dmp.slot_ad_${oozie_job_name} c ON c.ads = a.ad_sq
 WHERE b.d_uid NOT IN (select duid from user_dmp.kruxactive_user_count);



-- filtering section

--INSERT INTO TABLE svc_csw1.tmp_syrupad_recommend_10559 SELECT '${hivevar:start}', b.d_uid, c.m_slot, c.client_type, a.ads, a.score FROM (SELECT duid_sq, ads, REGEXP_REPLACE(score, '^\\[|\\]$', '') AS score FROM user_tadsvc.mahout_syrupad_output LATERAL VIEW EXPLODE(value) map_value_table AS ads, score WHERE ads = '10559') a JOIN user_tadsvc.mahout_duid_duid_sq_mapping b ON b.duid_sq = a.duid_sq JOIN user_tadsvc.slot_ad c ON c.ads = a.ads ORDER BY a.score DESC LIMIT 10000

-- pushing section

--INSERT INTO TABLE svc_csw1.tmp_syrupad_recommend SELECT * FROM svc_csw1.tmp_syrupad_recommend_10559






