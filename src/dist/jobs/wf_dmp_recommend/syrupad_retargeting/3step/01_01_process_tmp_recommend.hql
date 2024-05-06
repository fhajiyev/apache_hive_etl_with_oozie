USE svc_ds_dmp;

--set tez.queue.name=SKPQ1;
set mapreduce.job.queuename=SKPQ1;
--set hive.execution.engine=tez;
set hive.execution.engine=mr;

ADD JAR hdfs://skpds/user/dmp/lib/nexr-hive-udf-0.2-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION TADSVC_ROW_NUMBER AS 'com.nexr.platform.hive.udf.GenericUDFRowNumber';

-- 에러가 발생한 경우 tmp만 존재할 수도 있음
DROP TABLE IF EXISTS svc_ds_dmp.tmp_recommend_${oozie_job_name};

CREATE TABLE svc_ds_dmp.tmp_recommend_${oozie_job_name} (
  uuid           string,
  site_code      string,
  product_no     string,
  score          double,
  jobid          string,
  part_date      string
)
PARTITIONED BY (recommend_type string)
CLUSTERED BY (uuid) SORTED BY (uuid) INTO 256 BUCKETS;

INSERT INTO TABLE svc_ds_dmp.tmp_recommend_${oozie_job_name}
PARTITION (recommend_type='0')
SELECT uid, site, id, UNIX_TIMESTAMP(log_time, 'yyyy/MM/dd HH:mm:ss') AS score, '${jobId}', FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyyMMdd')
  FROM svc_ds_dmp.svc_retargeting_log_${oozie_job_name};

