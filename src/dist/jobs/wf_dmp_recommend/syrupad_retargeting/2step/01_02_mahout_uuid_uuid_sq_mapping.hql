USE user_dmp;

--set tez.queue.name=SKPQ1;
set mapreduce.job.queuename=SKPQ1;
--set hive.execution.engine=tez;
set hive.execution.engine=mr;

ADD JAR hdfs://skpds/user/dmp/lib/nexr-hive-udf-0.2-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION TADSVC_ROW_NUMBER AS 'com.nexr.platform.hive.udf.GenericUDFRowNumber';

DROP TABLE IF EXISTS user_dmp.mahout_uuid_uuid_sq_mapping_${oozie_job_name};

CREATE TABLE user_dmp.mahout_uuid_uuid_sq_mapping_${oozie_job_name} (
  uuid_sq  bigint,
  uuid     string
)
CLUSTERED BY (uuid_sq) SORTED BY (uuid_sq) INTO 256 BUCKETS
STORED AS orc;

INSERT OVERWRITE TABLE user_dmp.mahout_uuid_uuid_sq_mapping_${oozie_job_name}
SELECT TADSVC_ROW_NUMBER(0) AS uuid_sq, uid
  FROM ( 
        SELECT uid
          FROM svc_ds_dmp.svc_retargeting_log_${oozie_job_name}
        GROUP BY uid
        DISTRIBUTE BY HASH(uid)
       ) a;

