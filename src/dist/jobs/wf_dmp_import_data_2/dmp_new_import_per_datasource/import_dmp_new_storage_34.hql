



set hivevar:day2before;
set hivevar:day182before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='34')
select
  ma.ci,
  mb.dateval,
  '',
  case from_unixtime(unix_timestamp(mb.dateval, 'yyyyMMdd'), 'u')
      when 1 then 'mon'
      when 2 then 'tue'
      when 3 then 'wed'
      when 4 then 'thu'
      when 5 then 'fri'
      when 6 then 'sat'
      else 'sun'
  end,
  mb.action,
  if (mb.feed_action is null, '', mb.feed_action),
  if (mb.feed_id is null,     '', mb.feed_id),
  if (mb.feed_name is null,   '', mb.feed_name),



  '','','',
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

from
(

SELECT
ci,
sw_id
from
dmp_pi.id_pool
where part_date = '${hivevar:day2before}'

) ma

 inner join

(




  select
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '02' as action
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  'tap.topbanner' as feed_action
     ,  a.banner_id     as feed_id
     ,  if(b.name is null, '', b.name)          as feed_name
     ,  a.slot_id       as feed_site
     ,  '' as syrup_card_type
     ,  '' as syrup_card_id
     ,  '' as syrup_card_nm
     ,  a.carrier_name
     ,  a.device_model
     ,  a.os_type
     ,  a.os_version


   from

   (


    select
    member_id,
    base_time,
    banner_id,
    slot_id,

    carrier_name,
    device_model,
    os_type,
    os_version
    from smartwallet.smw_client_log
    where current_page in ('/lounge')
    and member_id is not null and member_id <> ''
    and action_id is not null and action_id in ('tap.topbanner')
    and dt = '${hivevar:day2before}'


   ) a
   left join
   smartwallet.mt3_banner b
   on
   a.banner_id = b.banner_id


  union all


  select
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '02' as action
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  'dotoom_tap.banner' as feed_action
     ,  a.banner_id          as feed_id
     ,  if(b.name is null, '', b.name)               as feed_name
     ,  a.display_order      as feed_site
     ,  '' as syrup_card_type
     ,  '' as syrup_card_id
     ,  '' as syrup_card_nm
     ,  a.carrier_name
     ,  a.device_model
     ,  a.os_type
     ,  a.os_version



   from

   (


    select
    member_id,
    base_time,
    banner_id,
    display_order,

    carrier_name,
    device_model,
    os_type,
    os_version
    from smartwallet.smw_client_log
    where current_page in ('/lounge_coupon/recommend')
    and member_id is not null and member_id <> ''
    and action_id is not null and action_id in ('dotoom_tap.banner')
    and dt = '${hivevar:day2before}'


   ) a
   left join

   smartwallet.mt3_banner b
   on
   a.banner_id = b.banner_id


  union all


  select
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '02' as action
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  'tap.cta' as feed_action
     ,  a.marketing_id    as feed_id
     ,  if(b.marketing_title is null, '', b.marketing_title) as feed_name
     ,  a.display_order   as feed_site
     ,  '' as syrup_card_type
     ,  '' as syrup_card_id
     ,  '' as syrup_card_nm
     ,  a.carrier_name
     ,  a.device_model
     ,  a.os_type
     ,  a.os_version



   from

   (


    select
    member_id,
    base_time,
    marketing_id,
    display_order,

    carrier_name,
    device_model,
    os_type,
    os_version
    from smartwallet.smw_client_log
    where current_page in ('/dfp_ad')
    and member_id is not null and member_id <> ''
    and action_id is not null and action_id in ('tap.cta')
    and dt = '${hivevar:day2before}'


   ) a
   left join
   (

    select
    marketing_id,
    marketing_title
    from
    istore.dbm_prd_mkt
    where part_date = '${hivevar:day2before}'

   ) b

   on
   a.marketing_id = b.marketing_id


  union all


  select
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '02' as action
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  'smallbanner_tap.img' as feed_action
     ,  a.banner_id          as feed_id
     ,  if(b.name is null, '', b.name)               as feed_name
     ,  a.display_order      as feed_site
     ,  '' as syrup_card_type
     ,  '' as syrup_card_id
     ,  '' as syrup_card_nm
     ,  a.carrier_name
     ,  a.device_model
     ,  a.os_type
     ,  a.os_version



   from

   (


    select
    member_id,
    base_time,
    banner_id,
    display_order,

    carrier_name,
    device_model,
    os_type,
    os_version
    from smartwallet.smw_client_log
    where current_page in ('/lounge')
    and member_id is not null and member_id <> ''
    and action_id is not null and action_id in ('smallbanner_tap.img')
    and dt = '${hivevar:day2before}'


   ) a
   left join

   smartwallet.mt3_banner b
   on
   a.banner_id = b.banner_id


--  union all


--  select
--        a.member_id
--     ,  substr(a.base_time,1,8) as dateval
--     ,  '' as push_id_recv
--     ,  '' as push_id_resp
--     ,  '02' as action
--     ,  '' as seg_id
--     ,  '' as seg_nm
--     ,  '' as push_name
--     ,  'best_tap.dotoom' as feed_action
--     ,  a.marketing_id    as feed_id
--     ,  if(b.marketing_title is null, '', b.marketing_title) as feed_name
--     ,  a.display_order   as feed_site
--     ,  '' as syrup_card_type
--     ,  '' as syrup_card_id
--     ,  '' as syrup_card_nm
--     ,  a.carrier_name
--     ,  a.device_model
--     ,  a.os_type
--     ,  a.os_version



--   from

--   (


--    select
--    member_id,
--    base_time,
--    marketing_id,
--    display_order,

--    carrier_name,
--    device_model,
--    os_type,
--    os_version
--    from smartwallet.smw_client_log
--    where current_page in ('/lounge')
--    and member_id is not null and member_id <> ''
--    and action_id is not null and action_id in ('best_tap.dotoom')
--    and dt = '${hivevar:day2before}'


--   ) a
--   left join
--   (

--    select
--    marketing_id,
--    marketing_title
--    from
--    istore.dbm_prd_mkt
--    where part_date = '${hivevar:day2before}'

--   ) b

--   on
--   a.marketing_id = b.marketing_id


--  union all


--  select
--        a.member_id
--     ,  substr(a.base_time,1,8) as dateval
--     ,  '' as push_id_recv
--     ,  '' as push_id_resp
--     ,  '02' as action
--     ,  '' as seg_id
--     ,  '' as seg_nm
--     ,  '' as push_name
--     ,  'best_tap.coupon' as feed_action
--     ,  a.coupon_id       as feed_id
--     ,  if(b.coupon_name is null, '', b.coupon_name)     as feed_name
--     ,  a.display_order   as feed_site
--     ,  '' as syrup_card_type
--     ,  '' as syrup_card_id
--     ,  '' as syrup_card_nm
--     ,  a.carrier_name
--     ,  a.device_model
--     ,  a.os_type
--     ,  a.os_version



--   from

--   (


--    select
--    member_id,
--    base_time,
--    coupon_id,
--    display_order,

--    carrier_name,
--    device_model,
--    os_type,
--    os_version
--    from smartwallet.smw_client_log
--    where current_page in ('/lounge')
--    and member_id is not null and member_id <> ''
--    and action_id is not null and action_id in ('best_tap.coupon')
--    and dt = '${hivevar:day2before}'


--   ) a
--   left join
--   smartwallet.mt3_direct_coupon b
--   on
--   a.coupon_id = b.direct_coupon_id


--  union all


--  select
--        a.member_id
--     ,  substr(a.base_time,1,8) as dateval
--     ,  '' as push_id_recv
--     ,  '' as push_id_resp
--     ,  '02' as action
--     ,  '' as seg_id
--     ,  '' as seg_nm
--     ,  '' as push_name
--     ,  'best_tap.event' as feed_action
--     ,  a.event_id       as feed_id
--     ,  if(b.title is null, '', b.title)          as feed_name
--     ,  ''               as feed_site
--     ,  '' as syrup_card_type
--     ,  '' as syrup_card_id
--     ,  '' as syrup_card_nm
--     ,  a.carrier_name
--     ,  a.device_model
--     ,  a.os_type
--     ,  a.os_version



--   from

--   (


--    select
--    member_id,
--    base_time,
--    event_id,
--    '',

--    carrier_name,
--    device_model,
--    os_type,
--    os_version
--    from smartwallet.smw_client_log
--    where current_page in ('/lounge')
--    and member_id is not null and member_id <> ''
--    and action_id is not null and action_id in ('best_tap.event')
--    and dt = '${hivevar:day2before}'


--   ) a
--   left join

--   smartwallet.mt3_event b
--   on
--   a.event_id = b.event_id


--  union all


--  select
--        a.member_id
--     ,  substr(a.base_time,1,8) as dateval
--     ,  '' as push_id_recv
--     ,  '' as push_id_resp
--     ,  '02' as action
--     ,  '' as seg_id
--     ,  '' as seg_nm
--     ,  '' as push_name
--     ,  'event_tap.banner' as feed_action
--     ,  a.event_id         as feed_id
--     ,  if(b.title is null, '', b.title)            as feed_name
--     ,  a.display_order    as feed_site
--     ,  '' as syrup_card_type
--     ,  '' as syrup_card_id
--     ,  '' as syrup_card_nm
--     ,  a.carrier_name
--     ,  a.device_model
--     ,  a.os_type
--     ,  a.os_version



--   from

--   (


--    select
--    member_id,
--    base_time,
--    event_id,
--    display_order,

--    carrier_name,
--    device_model,
--    os_type,
--    os_version
--    from smartwallet.smw_client_log
--    where current_page in ('/lounge')
--    and member_id is not null and member_id <> ''
--    and action_id is not null and action_id in ('event_tap.banner')
--    and dt = '${hivevar:day2before}'


--   ) a
--   left join

--   smartwallet.mt3_event b
--   on
--   a.event_id = b.event_id


) mb
on ( ma.sw_id = mb.member_id )
;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='34');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='34');




