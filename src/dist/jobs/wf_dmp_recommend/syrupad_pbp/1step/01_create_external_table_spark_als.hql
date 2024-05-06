USE user_dmp;

--set tez.queue.name=COMMON
--set mapreduce.job.queuename=COMMON
--set hive.execution.engine=tez

--set hive.mapred.local.mem=8192
--set hive.execution.engine=mr




set hivevar:preference;
set hivevar:oozie_job_name;
set hivevar:dmp_site_id;
set hivevar:dmp_data_source_id;



DROP TABLE IF EXISTS user_dmp.log_job_${oozie_job_name};

CREATE TABLE user_dmp.log_job_${oozie_job_name} AS
SELECT col003   as log_time,
       col004   as k_event,
       col005   as cps,
       col006   as ads,
       col007   as adt,
       col008   as blt,
       col009   as cts,
       col010   as tgs,
       col011   as mds,
       col012   as ccb,
       col013   as ctc,
       col014   as sxb,
       col015   as age,
       col016   as crr,
       col017   as m_client_id,
       col018   as m_slot,
       site_uid as d_uid,
       col019   as d_os_name,
       col020   as d_os_ver,
       col021   as d_model,
       col022   as d_network,
       col023   as u_network_operator,
       col024   as u_phone_number,
       col025   as m_sdk_ver,
       col026   as u_geolocation,
       col027   as u_terms,
       col028   as k_pilot,
       col029   as x_products,
       col030   as d_sales,
       col031   as d_gadid,
       col032   as bir,
       col033   as kwd,
       col034   as d_idfa

FROM dmp.data_store
WHERE
(dmp_part_date >= FROM_UNIXTIME(UNIX_TIMESTAMP()-3600*24*${term}, 'yyyyMMdd') AND dmp_part_date <= FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyyMMdd'))
AND
(col001 >= FROM_UNIXTIME(UNIX_TIMESTAMP()-3600*24*${term}, 'yyyy/MM/dd HH:mm:ss') AND col002 <= FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy/MM/dd HH:mm:ss'))
AND
dmp_site_id = '${hivevar:dmp_site_id}'
AND
dmp_data_source_id = '${hivevar:dmp_data_source_id}'

AND (col005 IS NOT NULL AND col005 <> '')
AND (col004 >= '200')
AND (site_uid <> 'none')
AND (site_uid <> 'NONE');

DROP TABLE IF EXISTS svc_ds_dmp.svc_event_log_${oozie_job_name};

CREATE TABLE svc_ds_dmp.svc_event_log_${oozie_job_name}
(
  d_uid       string,
  m_slot      string,
  client_type string,
  ads         string,
  score       double
);


INSERT OVERWRITE TABLE svc_ds_dmp.svc_event_log_${oozie_job_name}
SELECT
  a.d_uid,
  a.m_slot,
  CASE WHEN SUBSTRING(a.m_client_id, 1, 1) = 'M' THEN 'W' ELSE 'A' END,
  regexp_extract(a.ads, '[0-9]+', 0),
  ${hivevar:preference}
  AS score
  FROM user_dmp.log_job_${oozie_job_name} a
 GROUP BY a.d_uid, a.m_slot, CASE WHEN SUBSTRING(a.m_client_id, 1, 1) = 'M' THEN 'W' ELSE 'A' END, regexp_extract(a.ads, '[0-9]+', 0);


DROP TABLE IF EXISTS user_dmp.mahout_syrupad_input_${oozie_job_name};

CREATE TABLE user_dmp.mahout_syrupad_input_${oozie_job_name}
(
  duid_sq   bigint,
  ad_sq     bigint,
  score     double
)
CLUSTERED BY (duid_sq, ad_sq) SORTED BY (duid_sq, ad_sq) INTO 256 BUCKETS
ROW FORMAT 
DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://skpds/${syrupad_pbp_hdfs_home}/${oozie_job_name}/mahout_input';

DROP TABLE IF EXISTS user_dmp.mahout_syrupad_output_${oozie_job_name};

CREATE EXTERNAL TABLE user_dmp.mahout_syrupad_output_${oozie_job_name} ( duid_sq bigint, ad_sq bigint, score double ) CLUSTERED BY (duid_sq) SORTED BY (duid_sq) INTO 256 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION 'hdfs://skpds/${syrupad_pbp_hdfs_home}/${oozie_job_name}/mahout_output';


DROP TABLE IF EXISTS user_dmp.mahout_syrupad_users_${oozie_job_name};

CREATE TABLE user_dmp.mahout_syrupad_users_${oozie_job_name}
(
  duid_sq        bigint
)
CLUSTERED BY (duid_sq) SORTED BY (duid_sq) INTO 256 BUCKETS
ROW FORMAT 
DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://skpds/${syrupad_pbp_hdfs_home}/${oozie_job_name}/mahout_users';

