USE ${db};

set hivevar:dmp_site_id;
set hivevar:dmp_data_source_id;
set hivevar:from_dt;
set hivevar:to_dt;

ALTER TABLE ${db}.data_store DROP IF EXISTS PARTITION (dmp_site_id='${hivevar:dmp_site_id}', dmp_data_source_id='${hivevar:dmp_data_source_id}', dmp_part_date='${hivevar:from_dt}');

set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=true;

INSERT INTO TABLE dmp.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT null as dmp_uid, uid as site_uid,

'${hivevar:from_dt}' AS from_dt, '${hivevar:to_dt}' AS to_dt, log_time, action, site, url, id, count, total_sales, title,
img, sales_state, mid, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,

null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
'${hivevar:dmp_site_id}' as dmp_site_id, '${hivevar:dmp_data_source_id}' as dmp_data_source_id, part_hour as dmp_part_date
 FROM tad.log_server_tracking
WHERE part_hour >= '${hivevar:from_dt}' AND part_hour <= '${hivevar:to_dt}';