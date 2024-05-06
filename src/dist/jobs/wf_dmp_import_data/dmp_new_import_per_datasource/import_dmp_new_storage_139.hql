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

-- LifeStyle

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='139')

  SELECT
  uid,
  date_type,
  category,
  division,
  section,
  lifestyle_id,
  lifestyle_name,
  freq,
  total_freq,
  cast(rate * 1000 as decimal),
  cast(avg_rate * 1000 as decimal),
  cast(std_rate * 1000 as decimal),
  stanine_score,
  cast(pr * 1000 as decimal),
  ntile_v,
  rn,
  concat(rate, '_', avg_rate, '_', std_rate, '_', pr),

'', '', '', '',
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
dmp.prod_lifestyle_data_mart_view
;
