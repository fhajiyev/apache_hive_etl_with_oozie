USE dmp;


set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day90before;
set hivevar:day91before;
set hivevar:daycurr;


set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

-- SK Planet Demographics

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='62')
  SELECT
  DMP_UID as uid,
  AGE age,
  CASE
        WHEN GENCLASS IS NOT NULL AND GENCLASS IN ('7','4','8') THEN GENCLASS
        ELSE ''
  END genclass,
  POINT ocbpoint,
  COL001 ocb_serv,
  COL002 mbr_class,

  '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '${hivevar:daycurr}00',
  '${hivevar:daycurr}00'

  FROM svc_ds_dmp.dmp_log_server_tracking_user_mode_new
  WHERE (part_date = '${hivevar:daybefore}')
  AND DMP_UID NOT IN ('(DMPC)00000000-0000-0000-0000-000000000000','(GAID)00000000-0000-0000-0000-000000000000','(IDFA)00000000-0000-0000-0000-000000000000')
  ;

