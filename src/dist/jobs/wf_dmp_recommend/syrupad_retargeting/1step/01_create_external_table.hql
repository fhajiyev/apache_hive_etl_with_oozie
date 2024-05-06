USE user_dmp;

set hivevar:term;
set hivevar:oozie_job_name;
set hivevar:dmp_site_id;
set hivevar:dmp_data_source_id;

set hive.mapred.local.mem=8192;

--set tez.queue.name=SKPQ1;
set mapreduce.job.queuename=SKPQ1;
--set hive.execution.engine=tez;
set mapreduce.job.reduces=128;
set hive.execution.engine=mr;

DROP TABLE IF EXISTS user_dmp.log_job_${oozie_job_name};

CREATE TABLE user_dmp.log_job_${oozie_job_name} AS
SELECT col003 as log_time, col004 as action, site_uid as uid, col005 as site, col006 as url, col007 as id, col008 as count, col009 as total_sales, col010 as title, col011 as img, col012 as sales_state
  FROM dmp.data_store
 WHERE (dmp_part_date >= FROM_UNIXTIME(UNIX_TIMESTAMP()-3600*24*${hivevar:term}, 'yyyyMMddHH') AND dmp_part_date <= FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyyMMddHH'))
   AND site_uid <> 'none' AND dmp_site_id = '${hivevar:dmp_site_id}' AND dmp_data_source_id = '${hivevar:dmp_data_source_id}';

DROP TABLE IF EXISTS user_dmp.max_logtime_id_${oozie_job_name};

CREATE TABLE user_dmp.max_logtime_id_${oozie_job_name} AS
SELECT site, id, MAX(log_time) AS log_time
  FROM user_dmp.log_job_${oozie_job_name}
GROUP BY site, id;

DROP TABLE IF EXISTS user_dmp.available_id_${oozie_job_name};

CREATE TABLE user_dmp.available_id_${oozie_job_name} AS
SELECT b.site, b.id
  FROM user_dmp.log_job_${oozie_job_name} a
   JOIN user_dmp.max_logtime_id_${oozie_job_name} b
     ON a.site = b.site AND a.id = b.id AND a.log_time = b.log_time AND a.sales_state = '1'
GROUP BY b.site, b.id;

DROP TABLE IF EXISTS svc_ds_dmp.svc_retargeting_log_${oozie_job_name};

CREATE TABLE svc_ds_dmp.svc_retargeting_log_${oozie_job_name}
(
 log_time      string,
 site          string,
 uid           string,
 id            string,
 title         string,
 img           string,
 url           string,
 total_sales   bigint,
 count         bigint,
 score         double
);

INSERT OVERWRITE TABLE svc_ds_dmp.svc_retargeting_log_${oozie_job_name}
SELECT
 MAX(a.log_time),
 a.site,
 a.uid,
 a.id,
 MAX(a.title),
 MAX(a.img),
 MAX(a.url),
 SUM(CAST(a.total_sales AS INT)),
 SUM(CAST(a.count AS INT)),
 ${preference} AS score
  FROM user_dmp.log_job_${oozie_job_name} a
   JOIN user_dmp.available_id_${oozie_job_name} b ON a.site = b.site AND a.id = b.id
GROUP BY a.site, a.uid, a.id;


-- adjustment for syrupad integration

DROP TABLE IF EXISTS user_dmp.highest_perform_users_${oozie_job_name};

CREATE TABLE user_dmp.highest_perform_users_${oozie_job_name}
(
  uid string
);

INSERT INTO TABLE user_dmp.highest_perform_users_${oozie_job_name}
SELECT a.uid
FROM
(
  SELECT uid, sum(score) as totalscore
  FROM svc_ds_dmp.svc_retargeting_log_${oozie_job_name}
  GROUP BY uid
  ORDER BY totalscore DESC
  LIMIT 1000000
) a;

DROP TABLE IF EXISTS user_dmp.mahout_input_${oozie_job_name};

CREATE TABLE user_dmp.mahout_input_${oozie_job_name}
(
  uuid_sq     bigint,
  prod_sq     bigint,
  score       double
)
CLUSTERED BY (uuid_sq, prod_sq) SORTED BY (uuid_sq, prod_sq) INTO 256 BUCKETS
ROW FORMAT 
DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://skpds/${retargeting_hdfs_home}/${oozie_job_name}/mahout_input';

DROP TABLE IF EXISTS user_dmp.mahout_output_${oozie_job_name};

CREATE EXTERNAL TABLE user_dmp.mahout_output_${oozie_job_name}
(
  uuid_sq bigint,
  value MAP<bigint, string>
)
CLUSTERED BY (uuid_sq) SORTED BY (uuid_sq) INTO 256 BUCKETS
ROW FORMAT 
DELIMITED 
FIELDS TERMINATED BY '\t' ESCAPED BY '['
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://skpds/${retargeting_hdfs_home}/${oozie_job_name}/mahout_output';

DROP TABLE IF EXISTS user_dmp.mahout_users_${oozie_job_name};

CREATE TABLE user_dmp.mahout_users_${oozie_job_name}
(
  uuid_sq        bigint
)
CLUSTERED BY (uuid_sq) SORTED BY (uuid_sq) INTO 256 BUCKETS
ROW FORMAT 
DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://skpds/${retargeting_hdfs_home}/${oozie_job_name}/mahout_users';

