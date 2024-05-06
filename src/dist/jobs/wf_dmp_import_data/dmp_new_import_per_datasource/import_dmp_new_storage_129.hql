USE dmp;


set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day90before;
set hivevar:day91before;
set hivevar:daycurr;

set mapreduce.job.queuename=COMMON;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;


--  통합 Context 유추

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='129')
  SELECT

  CASE
       WHEN UID=LOWER(UID)  AND UID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(GAID)',UID)
       WHEN UID<>LOWER(UID) AND UID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(IDFA)',UID)
  END AS UID,

  if(job is null, '', job),
  if(hobby is null, '', hobby),
  if(income is null, '', cast(income as decimal)),
  if(house_price is null, '', cast(house_price as decimal)),

  if(commuter is null or commuter = '', 'N', commuter),

  if(device_change is null, '', device_change),
  if(operator_change is null, '', operator_change),
  if(house_moved is null, '', house_moved),
  if(isp is null, '', isp),

  if(commercial_zone is null, '', commercial_zone),

  if(commercial_zone is null, '', if(commsplit[1] is null, '', commsplit[1])),
  if(commercial_zone is null, '', if(commsplit[2] is null, '', commsplit[2])),
  if(commercial_zone is null, '', if(commsplit[3] is null, '', commsplit[3])),
  if(commercial_zone is null, '', if(commsplit[4] is null, '', commsplit[4])),
  if(commercial_zone is null, '', if(commsplit[5] is null, '', commsplit[5])),

  if(activity_area is null, '', activity_area),
  if(residence is null, '', residence),

  '', '', '',
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

  select
  uid,
  job,
  hobby,
  income,
  house_price,
  commuter,
  device_change,
  operator_change,
  house_moved,
  isp,
  commercial_zone,
  split(commercial_zone, '\\|\\|') as commsplit,
  activity_area,
  residence

  FROM

  svc_context_ano.view_person_context

) t


  WHERE UID IS NOT NULL AND UID <> ''
;


