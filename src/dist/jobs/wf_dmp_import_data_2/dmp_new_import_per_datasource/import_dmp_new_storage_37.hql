

set hivevar:day2before;
set hivevar:day5before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='37')

SELECT DISTINCT
ci,
if(brand_cd is null, '', brand_cd),
if(brand_nm is null, '', brand_nm),
if(buy_scr  is null, '', cast(buy_scr as decimal)),
'','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'${hivevar:day2before}00',
'${hivevar:day2before}00'

FROM

svc_custpfdb.pdb_mp_com_m_brand_interest
;


