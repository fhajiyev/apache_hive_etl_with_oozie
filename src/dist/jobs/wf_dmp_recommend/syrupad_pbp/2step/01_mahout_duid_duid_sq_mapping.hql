USE user_dmp;

--set tez.queue.name=COMMON
--set mapreduce.job.queuename=COMMON;
--set hive.execution.engine=tez

ADD JAR ${libDir}/nexr-hive-udf-0.2-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION TADSVC_ROW_NUMBER AS 'com.nexr.platform.hive.udf.GenericUDFRowNumber';

DROP TABLE IF EXISTS user_dmp.mahout_duid_duid_sq_mapping_${oozie_job_name};

CREATE TABLE user_dmp.mahout_duid_duid_sq_mapping_${oozie_job_name} (
  duid_sq  bigint,
  d_uid     string
)
CLUSTERED BY (duid_sq) SORTED BY (duid_sq) INTO 256 BUCKETS
STORED AS orc;

INSERT OVERWRITE TABLE user_dmp.mahout_duid_duid_sq_mapping_${oozie_job_name}
SELECT TADSVC_ROW_NUMBER(0) AS duid_sq, d_uid
  FROM ( 
        SELECT d_uid
          FROM svc_ds_dmp.svc_event_log_${oozie_job_name}
        GROUP BY d_uid
        DISTRIBUTE BY HASH(d_uid)
       ) a;

