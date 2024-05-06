#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi

HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_sw_act\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
#    $HDD fs -put proc_$CTIME.log hdfs://skpds/user/dmp_pi/dmp_pi_etl_stat/
}

die() {
    etl_stat 'FAIL';
    echo >&2 -e "\nERROR: $@\n"; exit 1;
}
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }
print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }
print_sstr() { printf "\e[93m$@\e[0m\n"; }

HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"



LAST_JOB_DAY=`expr 2 + $1`
LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`

DROP_DAYS=`expr 182 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

etl_stat 'DOING'
print_bline
print_bstr "시럽 액티비티 = 6"

echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store_alter partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='6')
select
  distinct ma.ci,
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
  '','','','','','','','','',''

from 
(
  select
      a1.ci,
      a1.member_id

      from smartwallet.mt3_member a1      
      WHERE
      a1.ci IS NOT NULL AND a1.ci <> '' AND a1.ci not like '%\u0001%'
      AND wallet_accept = 1 and wallet_accept1 = 1 and wallet_accept2 = 1 and vm_state_cd = '9' and length(last_auth_dt) = 14 
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
  where a.dt = '${LAST_JOB_DATE_NOSLA}'
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


  select distinct
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '04' as push_type
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
    and action_id is not null and action_id in ('tap.topbanner')
    and dt = '${LAST_JOB_DATE_NOSLA}'


   ) a
   left join
   smartwallet.mt3_banner b
   on
   a.banner_id = b.banner_id 
 

  union all

   
  select distinct
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '04' as push_type
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  'best_tap.dotoom' as feed_action
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
    where current_page in ('/lounge')
    and action_id is not null and action_id in ('best_tap.dotoom')
    and dt = '${LAST_JOB_DATE_NOSLA}'


   ) a
   left join
   (

    select
    marketing_id,
    marketing_title
    from
    istore.dbm_prd_mkt
    where part_date = '${LAST_JOB_DATE_NOSLA}'

   ) b

   on 
   a.marketing_id = b.marketing_id
 

  union all
  
 
  select distinct
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '04' as push_type
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  'best_tap.coupon' as feed_action
     ,  a.coupon_id       as feed_id
     ,  if(b.coupon_name is null, '', b.coupon_name)     as feed_name
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
    coupon_id,
    display_order,

    carrier_name,
    device_model,
    os_type,
    os_version
    from smartwallet.smw_client_log
    where current_page in ('/lounge')
    and action_id is not null and action_id in ('best_tap.coupon')
    and dt = '${LAST_JOB_DATE_NOSLA}'


   ) a
   left join
   smartwallet.mt3_direct_coupon b
   on
   a.coupon_id = b.direct_coupon_id


  union all
  

  select distinct
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '04' as push_type
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  'best_tap.event' as feed_action
     ,  a.event_id       as feed_id
     ,  if(b.title is null, '', b.title)          as feed_name
     ,  ''               as feed_site
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
    event_id,
    '',

    carrier_name,
    device_model,
    os_type,
    os_version
    from smartwallet.smw_client_log
    where current_page in ('/lounge')
    and action_id is not null and action_id in ('best_tap.event')
    and dt = '${LAST_JOB_DATE_NOSLA}'


   ) a
   left join
 
   smartwallet.mt3_event b
   on
   a.event_id = b.event_id


  union all


  select distinct
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '04' as push_type
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
    where current_page in ('/lounge')
    and action_id is not null and action_id in ('dotoom_tap.banner')
    and dt = '${LAST_JOB_DATE_NOSLA}'


   ) a
   left join

   smartwallet.mt3_banner b
   on
   a.banner_id = b.banner_id


  union all


  select distinct
        a.member_id
     ,  substr(a.base_time,1,8) as dateval
     ,  '' as push_id_recv
     ,  '' as push_id_resp
     ,  '04' as push_type
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  '' as push_name
     ,  'event_tap.banner' as feed_action
     ,  a.event_id         as feed_id
     ,  if(b.title is null, '', b.title)            as feed_name
     ,  a.display_order    as feed_site
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
    event_id,
    display_order,

    carrier_name,
    device_model,
    os_type,
    os_version
    from smartwallet.smw_client_log
    where current_page in ('/lounge')
    and action_id is not null and action_id in ('event_tap.banner')
    and dt = '${LAST_JOB_DATE_NOSLA}'


   ) a
   left join

   smartwallet.mt3_event b
   on
   a.event_id = b.event_id


  union all


  select distinct

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
    distinct member_id,
             base_time,
             carrier_name,
             device_model,
             os_type,
             os_version
    from
    smartwallet.smw_client_log
    where dt = '${LAST_JOB_DATE_NOSLA}'
      and current_page in ('/issue/mcard', '/detail/mcard')
      and (action_id is null or trim(action_id) = '')

  ) dd on aa.member_id = dd.member_id
  join smartwallet.mt3_mem_card bb on aa.member_id = bb.member_id
  join smartwallet.mt3_card cc on bb.card_id = cc.card_id





) mb
on ( ma.member_id = mb.member_id )
;" > sw_act_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -f sw_act_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store_alter drop partition (part_hour < '${DROP_DATE}00', data_source_id='6');"

etl_stat 'DONE'

rm -f sw_act_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log


