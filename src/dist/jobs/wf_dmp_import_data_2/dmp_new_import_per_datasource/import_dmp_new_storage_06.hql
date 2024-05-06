



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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='6')
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
  '',
  mb.push_type,
  '',
  '',
  '',
  '',
  if (mb.feed_action is null, '', mb.feed_action),
  if (mb.feed_id is null,     '', mb.feed_id),
  if (mb.feed_name is null,   '', mb.feed_name),
  if (mb.feed_site is null,   '', mb.feed_site ),
  CASE WHEN mb.syrup_card_type = '04' THEN mb.syrup_card_id ELSE '' END,
  CASE WHEN mb.syrup_card_type = '04' THEN mb.syrup_card_nm ELSE '' END,
  CASE WHEN mb.syrup_card_type = '05' THEN mb.syrup_card_id ELSE '' END,
  CASE WHEN mb.syrup_card_type = '05' THEN mb.syrup_card_nm ELSE '' END, 

  
  if (mb.carrier_name is null,   '', mb.carrier_name),
  '',
  if (mb.device_model is null,   '', mb.device_model),
  if (mb.os_type is null,        '', mb.os_type     ),
  if (mb.os_version is null,     '', mb.os_version  ),


  '','','','','','','','',
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

join

(



  select distinct
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '00' as push_type
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  '' as feed_action
     ,  '' as feed_id
     ,  '' as feed_name
     ,  '' as feed_site
     ,  '' as syrup_card_type
     ,  '' as syrup_card_id
     ,  '' as syrup_card_nm
     ,  a.carrier_name
     ,  a.device_model
     ,  a.os_type
     ,  a.os_version

  FROM 
  smartwallet.smw_client_log a
  where a.dt = '${hivevar:day2before}'
      and a.member_id is not null and a.member_id <> ''
      and a.current_page <> '/appexec'
      and a.current_page is not null
      and lower(trim(a.current_page)) not in ('', 'n')
      and lower(trim(a.action_id)) not in ('event_receive', 'server_comm')
      and lower(trim(a.action_id)) not in ('background.display', 'background.display_native', 'background.dispaly_native')
      and lower(concat_ws('',trim(a.current_page),trim(a.action_id))) not in (-- '/start', '/start/guide'
         '/ble/icon', 'ble icon', '/ble/network/error', '/ble/noti', '/ble/iconble_tap.delete'
       , '/preordericon', '/preordericonorder_tap.delete'
       , '/pushnoti/popup', '/pushnoti/indicator', '/push/noti', '/pushnoti/indicatorindicator_tap.notidel'
       , '/pushnoti/indicator_bluetooth'
       , '/push/ticket'
       , '/push/coin', '/push/coinlongpress.coin'
       , '/wifi/noti'
       , '/push/generalticket'
       , '/push_device/noti'
       , '/permission/main'
       , '/noti/guide', '/welcome', '/noti/guidepushlist_tap.close', '/welcometap.close', '/shoppingbackground.display', '/newsbackground.display', '/guidelockbackground.display'
       , '/guidein/noti', '/guideout/noti', '/guidewelcome', '/guidewelcometap.close'
       , '/main/couponbackground.couponlist' -- couponlist
       , '/btpopup', '/btpopupbottom_tap.cancelbtn')    
     
     

  union all

  select

        aa.member_id
     ,  substr(dd.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '06' as push_type
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  '' as feed_action
     ,  '' as feed_id
     ,  '' as feed_name
     ,  '' as feed_site
     , CASE
         WHEN cc.card_type_cd = '28' THEN '05'
         ELSE '04'
       END AS syrup_card_type
     ,  bb.card_id as syrup_card_id
     ,  cc.card_name as syrup_card_nm
     ,  dd.carrier_name
     ,  dd.device_model
     ,  dd.os_type
     ,  dd.os_version


  from smartwallet.mt3_member aa
  join
  (

    select
             member_id,
             base_time,
             carrier_name,
             device_model,
             os_type,
             os_version
    from
    smartwallet.smw_client_log
    where dt = '${hivevar:day2before}'
      and current_page in ('/issue/mcard', '/detail/mcard')
      and (action_id is null or trim(action_id) = '')

  ) dd on aa.member_id = dd.member_id
  join smartwallet.mt3_mem_card bb on aa.member_id = bb.member_id
  join smartwallet.mt3_card cc on bb.card_id = cc.card_id





) mb
on ( ma.sw_id = mb.member_id )
;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='6');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='6');




