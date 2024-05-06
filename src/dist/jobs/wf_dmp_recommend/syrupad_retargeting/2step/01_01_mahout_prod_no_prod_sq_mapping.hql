USE user_dmp;

--set tez.queue.name=SKPQ1;
set mapreduce.job.queuename=SKPQ1;
--set hive.execution.engine=tez;
set hive.execution.engine=mr;

ADD JAR hdfs://skpds/user/dmp/lib/nexr-hive-udf-0.2-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION TADSVC_ROW_NUMBER AS 'com.nexr.platform.hive.udf.GenericUDFRowNumber';

DROP TABLE IF EXISTS user_dmp.mahout_prod_no_prod_sq_mapping_${oozie_job_name};

CREATE TABLE user_dmp.mahout_prod_no_prod_sq_mapping_${oozie_job_name} (
  prod_sq     bigint,
  site_code   string,
  product_no  string
)
CLUSTERED BY (prod_sq, site_code) SORTED BY (prod_sq, site_code) INTO 256 BUCKETS
STORED AS orc;

INSERT OVERWRITE TABLE user_dmp.mahout_prod_no_prod_sq_mapping_${oozie_job_name}
SELECT TADSVC_ROW_NUMBER(0) AS prod_sq, site, id
  FROM ( 
        SELECT site, id
          FROM svc_ds_dmp.svc_retargeting_log_${oozie_job_name}
        GROUP BY site, id
        DISTRIBUTE BY HASH(site, id)
       ) a;

