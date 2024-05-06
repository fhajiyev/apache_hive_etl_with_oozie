
set hivevar:daybefore;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table svc_ds_dmp_pi.prod_integr_context_tbl

  SELECT

  UID,

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

  '',
  ''

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

  svc_cim.view_person_context

) t


  WHERE UID IS NOT NULL AND UID <> ''
;


insert into table svc_ds_dmp_pi.prod_integr_context_tbl

  SELECT

  UID,

  '','','','','','','','','','',
  '','','','','','','',

  if(part_dt is null, '', part_dt),
  if(os_name is null, '', os_name)

  FROM
(

  select
    ci as uid,
    part_dt,
    os_name

    FROM

    svc_cim.prod_foreign_country_stay
    where part_dt = '${hivevar:daybefore}'

) t


  WHERE UID IS NOT NULL AND UID <> ''
;

