USE user_tadsvc;

set hivevar:from_dt;
set hivevar:to_dt;

--set tez.queue.name=COMMON;
set mapreduce.job.queuename=COMMON;
--set hive.execution.engine=tez;
--set hive.execution.engine=mr;

DROP TABLE IF EXISTS dmp.data_store_test;

CREATE TABLE IF NOT EXISTS dmp.data_store_test
(
dmp_uid   string,
site_uid   string,

col001    string,
col002    string,
col003    string,
col004    string,
col005    string,
col006    string,
col007    string,
col008    string,
col009    string,
col010    string,

col011    string,
col012    string,
col013    string,
col014    string,
col015    string,
col016    string,
col017    string,
col018    string,
col019    string,
col020    string,

col021    string,
col022    string,
col023    string,
col024    string,
col025    string,
col026    string,
col027    string,
col028    string,
col029    string,
col030    string,

col031    string,
col032    string,
col033    string,
col034    string,
col035    string,
col036    string,
col037    string,
col038    string,
col039    string,
col040    string,

col041    string,
col042    string,
col043    string,
col044    string,
col045    string,
col046    string,
col047    string,
col048    string,
col049    string,
col050    string,

col051    string,
col052    string,
col053    string,
col054    string,
col055    string,
col056    string,
col057    string,
col058    string,
col059    string,
col060    string,

col061    string,
col062    string,
col063    string,
col064    string,
col065    string,
col066    string,
col067    string,
col068    string,
col069    string,
col070    string,

col071    string,
col072    string,
col073    string,
col074    string,
col075    string,
col076    string,
col077    string,
col078    string,
col079    string,
col080    string,

col081    string,
col082    string,
col083    string,
col084    string,
col085    string,
col086    string,
col087    string,
col088    string,
col089    string,
col090    string,

col091    string,
col092    string,
col093    string,
col094    string,
col095    string,
col096    string,
col097    string,
col098    string,
col099    string,
col100    string

)
PARTITIONED BY (dmp_site_id string, dmp_data_source_id string, dmp_part_date string)
CLUSTERED BY (site_uid) SORTED BY (site_uid) INTO 2000 BUCKETS
;

set hive.exec.dynamic.partition.mode=true;
set mapreduce.job.maps=200;

INSERT INTO TABLE dmp.data_store_test PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT null as dmp_uid, d_uid as site_uid,

  log_time,
  k_event, cps, ads, adt, blt, cts, tgs, mds, ccb, ctc,
  sxb, age, crr, m_client_id, m_slot, d_os_name, d_os_ver, d_model, d_network, u_network_operator,
  u_phone_number, m_sdk_ver, u_geolocation, u_terms, k_pilot, x_products, d_sales, d_gadid, bir, kwd,
  d_idfa, col1, col2, col3, col4, col5, col6, col7, col8, col9,
  col10, col11, col12, col13, col14, col15, col16, col17, col18, col19,
  col20, col21, col22, col23, col24, col25, col26, col27, col28, col29,
  col30, null, null, null, null, null, null, null, null, null,
  
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, 

  'syrupad' as dmp_site_id, 'service_activity_log' as dmp_data_source_id, part_date as dmp_part_date
 FROM tad.log_server_event
WHERE part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}';


INSERT INTO TABLE dmp.data_store_test PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT null as dmp_uid, d_uid as site_uid,

  log_time,
  k_event, cps, ads, adt, blt, cts, tgs, mds, ccb, ctc,
  sxb, age, crr, m_client_id, m_slot, d_os_name, d_os_ver, d_model, d_network, u_network_operator,
  u_phone_number, m_sdk_ver, u_geolocation, u_terms, k_pilot, x_products, d_sales, d_gadid, bir, kwd,
  d_idfa, col1, col2, col3, col4, col5, col6, col7, col8, col9,
  col10, col11, col12, col13, col14, col15, col16, col17, col18, col19,
  col20, col21, col22, col23, col24, col25, col26, col27, col28, col29,
  col30, null, null, null, null, null, null, null, null, null,

  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, 

  'syrupad' as dmp_site_id, 'service_activity_log' as dmp_data_source_id, part_date as dmp_part_date
 FROM tad.log_server_request
WHERE part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}';

INSERT INTO TABLE dmp.data_store_test PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT null as dmp_uid, uid as site_uid,

  log_time, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  
  null, action, site, ref, url, REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', id),'\;',' '),'\,',' '),'\n',' '), count, REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', c1),'\;',' '),'\,',' '),'\n',' '), REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', c2),'\;',' '),'\,',' '),'\n',' '), REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', c3),'\;',' '),'\,',' '),'\n',' '),
  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', c4),'\;',' '),'\,',' '),'\n',' '), REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', c5),'\;',' '),'\,',' '),'\n',' '), REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', c6),'\;',' '),'\,',' '),'\n',' '), total_sales, REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(reflect('java.net.URLDecoder', 'decode', title),'\;',' '),'\,',' '),'\n',' '), img, sales_state, ref_cd, mid, col1,
  col2, col3, col4, col5, col6, col7, col8, col9, col10, col11,
  col12, col13, col14, col15, col16, col17, col18, col19, col20, col21,

  'syrupad' as dmp_site_id, 'service_activity_log' as dmp_data_source_id, substring(part_hour, 1, 8) as dmp_part_date
 FROM tad.log_server_tracking
WHERE part_hour >= '${hivevar:from_dt}00' AND part_hour <= '${hivevar:to_dt}23'; 

