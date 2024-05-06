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

  -- App Installs

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='92')
  SELECT
  CONCAT('(GAID)', t1.AD_ID) as uid,

  if(t1.PACKAGE is null, '', t1.PACKAGE),

  if(t1.VERSION is null, '', t1.VERSION),

  if(t1.CREATED is null, '', t1.CREATED),

  if(t1.UPDATED is null, '', t1.UPDATED),

  if(t2.APP_NAME is null, '', t2.APP_NAME),

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

  FROM

  (
  SELECT
    AD_ID,
    PACKAGE,
    VERSION,
    CREATED,
    UPDATED
  FROM
  SVC_CONTEXT_ANO.ITM_WEEKLY_INSTALLATIONS a
  WHERE a.PART_YW IN (SELECT MAX(PART_YW) FROM SVC_CONTEXT_ANO.ITM_WEEKLY_INSTALLATIONS)
  AND AD_ID IS NOT NULL AND AD_ID <> ''
  )
  t1

  left join
  svc_context_ano.meta_app_pkg_label t2
  on
  t1.package = t2.package

  ;

