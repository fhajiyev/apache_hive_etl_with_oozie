USE ${db};

set hivevar:dmp_site_id;
set hivevar:dmp_data_source_id;
set hivevar:from_ts;
set hivevar:to_ts;
set hivevar:from_dt;
set hivevar:to_dt;

set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=true;

INSERT INTO TABLE dmp.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT null as dmp_uid, d_uid as site_uid,

'${hivevar:from_ts}' AS from_ts, '${hivevar:to_ts}' AS to_ts, log_time, k_event, cps, ads, adt, blt, cts, tgs,
mds, ccb, ctc, sxb, age, crr, m_client_id, m_slot, d_os_name, d_os_ver,
d_model, d_network, u_network_operator, u_phone_number, m_sdk_ver, u_geolocation, u_terms, k_pilot, x_products, d_sales,
d_gadid, bir, kwd, d_idfa, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
null, null, null, null, null, null, null, null, null, null,
'syrupad' as dmp_site_id, 'syrupad_pbp_activity_log' as dmp_data_source_id, part_date as dmp_part_date
 FROM tad.log_server_event
 WHERE log_time >= '${hivevar:from_ts}' AND log_time < '${hivevar:to_ts}' AND part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}';