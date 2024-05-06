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

-- Snapshot History

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='121')
  SELECT
  a.uid,
  CONCAT('-', CONCAT_WS('-', a.seg_ids), '-'),

  '', '', '', '', '', '', '', '', '',
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
   SVC_DS_DMP.PROD_SEG_USER_VIEW a

   WHERE

   (a.uid <> '' AND INSTR(a.uid, ',') <= 0 AND INSTR(a.uid, '\;') <= 0)
   AND
   a.uid not in ('(DMPC)00000000-0000-0000-0000-000000000000','(GAID)00000000-0000-0000-0000-000000000000','(IDFA)00000000-0000-0000-0000-000000000000')

  ;

