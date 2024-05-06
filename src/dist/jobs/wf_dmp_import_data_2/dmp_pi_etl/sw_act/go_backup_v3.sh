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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='6')
select
  distinct ma.ci,
  ma.dateval,
  '',
  case from_unixtime(unix_timestamp(ma.dateval, 'yyyyMMdd'), 'u')
      when 1 then 'mon'
      when 2 then 'tue'
      when 3 then 'wed'
      when 4 then 'thu'
      when 5 then 'fri'
      when 6 then 'sat'
      else 'sun'
  end,
  '',
  if (mb.push_type is null, '00', mb.push_type),
  case mb.push_type
      when null then ''
      when '01' then mb.push_id_recv
      when '02' then mb.push_id_resp
      else ''
  end, 
  if (mb.seg_id is null, '', mb.seg_id),
  if (mb.seg_nm is null, '', mb.seg_nm),

  if (mb.push_name is null,   '', mb.push_name),
  if (mb.feed_action is null, '', mb.feed_action),
  if (mb.feed_id is null,     '', mb.feed_id),
  if (mb.feed_name is null,   '', mb.feed_name),
  if (mb.feed_site is null,   '', mb.feed_site ),
  CASE WHEN mb.syrup_card_type = '04' THEN mb.syrup_card_id ELSE '' END,
  CASE WHEN mb.syrup_card_type = '04' THEN mb.syrup_card_nm ELSE '' END,
  CASE WHEN mb.syrup_card_type = '05' THEN mb.syrup_card_id ELSE '' END,
  CASE WHEN mb.syrup_card_type = '05' THEN mb.syrup_card_nm ELSE '' END, 

  
  if (ma.carrier_name is null,   '', ma.carrier_name),
  '',
  if (ma.device_model is null,   '', ma.device_model),
  if (ma.os_type is null,        '', ma.os_type     ),
  if (ma.os_version is null,     '', ma.os_version  ),


  '','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','',''

from (
  select
      b.ci,
      b.member_id,
      substr(a.base_time,1,8) as dateval,
      a.current_page as page_id,
      a.action_id,
     
      a.carrier_name,     
      a.device_model,
      a.os_type,
      a.os_version


  from smartwallet.mt3_member b
  join smartwallet.smw_client_log a on b.member_id = a.member_id
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
) ma
left outer join (


  select distinct
        a.member_id
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
     

   from
   
   (


    select
    member_id,
    banner_id,
    slot_id 
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
   

   from

   (


    select
    member_id,
    marketing_id,
    display_order
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


   from

   (


    select
    member_id,
    coupon_id,
    display_order
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


   from

   (


    select
    member_id,
    event_id,
    '' 
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


   from

   (


    select
    member_id,
    banner_id,
    display_order
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


   from

   (


    select
    member_id,
    event_id,
    display_order
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

  from smartwallet.mt3_member aa
  join
  (

    select
    distinct member_id
    from
    smartwallet.smw_client_log
    where current_page in ('/issue/mcard', '/detail/mcard')
      and (action_id is null or trim(action_id) = '')
      and dt = '${LAST_JOB_DATE_NOSLA}'

  ) dd on aa.member_id = dd.member_id
  join smartwallet.mt3_mem_card bb on aa.member_id = bb.member_id
  join smartwallet.mt3_card cc on bb.card_id = cc.card_id



  union all


  select distinct 
        aa.member_id
     ,  bb.ev_id as push_id_recv
     ,  '' as push_id_resp 
     ,  '01' as push_type
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  bb.ev_name as push_name
     ,  '' as feed_action
     ,  '' as feed_id
     ,  '' as feed_name
     ,  '' as feed_site
     ,  '' as syrup_card_type
     ,  '' as syrup_card_id
     ,  '' as syrup_card_nm
 
   from
    (

     select
     member_id,
     base_time,
     push_id
     from
     smartwallet.smw_client_log
     where dt = '${LAST_JOB_DATE_NOSLA}'
     and current_page in ('/push/noti','/push/coin', '/push_device/noti')
     and (action_id is null or trim(action_id) = '')
     and push_id is not null and trim(push_id) <> ''

    ) aa

   join( 
  
         select   t22.push_id,
                  t22.ev_id,
                  t22.ev_name
         from
         (  
         
             select   t2.push_id             
                     ,t2.ev_id as ev_id
                     ,t2.ev_name as ev_name
                     ,ROW_NUMBER() OVER( PARTITION BY t2.push_id ORDER BY t2.ev_id DESC ) AS RN
             from
                     (  select
                              s1.ev_id as ev_id, s2.push_id as push_id, s1.ev_title as ev_name from smartwallet.proc_ss_push s1
                            inner join 
                                  smartwallet.process s2
                            on s1.ev_id = s2.ev_id union all
                        select
                              s1.ev_id as ev_id, s2.push_id as push_id, s1.ev_title as ev_name from smartwallet.proc_ss_push s1
                            inner join 
                                  smartwallet.proc_ss_push_dtl s2
                            on s1.ev_id = s2.ev_id
                     ) t2
                     where nvl(push_id, '') <> '' 
                     and ev_id >= 0         
         ) t22
         where t22.RN=1                                     

        ) bb on aa.push_id = bb.push_id
   

  union all


  select distinct 
        aa.member_id
     ,  '' as push_id_recv
     ,  bb.ev_id as push_id_resp
     ,  '02' as push_type 
     ,  '' as seg_id
     ,  '' as seg_nm
     ,  bb.ev_name as push_name
     ,  '' as feed_action
     ,  '' as feed_id
     ,  '' as feed_name
     ,  '' as feed_site
     ,  '' as syrup_card_type
     ,  '' as syrup_card_id
     ,  '' as syrup_card_nm

   from
    (

     select
     member_id,
     base_time,
     push_id
     from
     smartwallet.smw_client_log
     where dt = '${LAST_JOB_DATE_NOSLA}'
     and current_page in ('/push/noti','/push/coin', '/push_device/noti')
     and (action_id is not null and trim(action_id) <> '')
     and push_id is not null and trim(push_id) <> ''

    ) aa

   join(

         select   t22.push_id,
                  t22.ev_id,
                  t22.ev_name

         from 
         (
   
            select
                     t2.push_id
                    ,t2.ev_id as ev_id
                    ,t2.ev_name as ev_name
                    ,ROW_NUMBER() OVER( PARTITION BY t2.push_id ORDER BY t2.ev_id DESC ) AS RN
                 
            from
                    (  select
                             s1.ev_id as ev_id, s2.push_id as push_id, s1.ev_title as ev_name from smartwallet.proc_ss_push s1
                           inner join
                                 smartwallet.process s2
                           on s1.ev_id = s2.ev_id union all
                       select
                             s1.ev_id as ev_id, s2.push_id as push_id, s1.ev_title as ev_name from smartwallet.proc_ss_push s1
                           inner join 
                                 smartwallet.proc_ss_push_dtl s2
                           on s1.ev_id = s2.ev_id
                    ) t2
            where nvl(push_id, '') <> '' 
            and ev_id >= 0          
         
         )t22
         where t22.RN=1         
                                               
        ) bb on aa.push_id = bb.push_id
   

  union all
  

  SELECT DISTINCT

        sT0.MBR_ID as member_id

  
        
        ,CASE WHEN NVL(sT0.EVNT_ID, '') <> '' THEN sT0.EVNT_ID ELSE sT2.EV_ID END as push_id_recv
        ,CASE WHEN NVL(sT0.EVNT_ID, '') <> '' THEN sT0.EVNT_ID ELSE sT2.EV_ID END as push_id_resp
        ,CASE WHEN sT0.PUSH_STEP_CD = '0' THEN '01' WHEN sT0.PUSH_STEP_CD = '2' THEN '02' END AS push_type
              
        , sT3.SEG_ID
        , sT3.SEG_NM             
        , CASE WHEN sT2.ev_name IS NULL THEN '' ELSE sT2.ev_name END as push_name
        ,  '' as feed_action
        ,  '' as feed_id
        ,  '' as feed_name
        ,  '' as feed_site
        ,  '' as syrup_card_type
        ,  '' as syrup_card_id
        ,  '' as syrup_card_nm

  FROM
        (
        -- 수신/반응건수
            SELECT
                     PART_DT
                    ,BASE_DTTM
                    ,MBR_ID
                    ,CASE WHEN PUSH_STEP_CD = '0' AND PUSH_ID IS NULL THEN '0'
                             ELSE PUSH_ID
                     END AS PUSH_ID
                    ,EVNT_ID
                    ,PUSH_STEP_CD

            FROM SMARTWALLET.PROC_SYW_D_PUSH_LOG
            WHERE PART_DT = '${LAST_JOB_DATE_NOSLA}'  -- 수신, 반응 일자 겸 PARTITION 날짜
              AND PUSH_STEP_CD IN ('0', '2')
            -- 01 Marketing_Push
            AND PUSH_TYP_CD = '01'
        ) sT0
  JOIN
        (
            SELECT
                      MBR_ID
                     ,CASE WHEN PUSH_ID IS NULL THEN '0'
                             ELSE PUSH_ID
                      END AS PUSH_ID
            FROM SMARTWALLET.PROC_SYW_D_PUSH_LOG
            WHERE PART_DT = '${LAST_JOB_DATE_NOSLA}'  -- 수신 일자 겸 PARTITION 날짜
              AND PUSH_STEP_CD = '0'       -- 노출(수신)이 된 회원
            GROUP BY MBR_ID, PUSH_ID
        ) sT1
  ON 
  (
    sT0.MBR_ID = sT1.MBR_ID     AND sT0.PUSH_ID = sT1.PUSH_ID
  )

  JOIN (
       SELECT DISTINCT
         EV_ID
        ,SEG_ID AS SEG_ID  -- SEG ID
        ,SUBSTR( REGEXP_REPLACE(SEND_DATE,\"[-:./' ']\",''), 1, 8 ) AS SND_DT  -- 발송일자
        ,SEG_NAME AS SEG_NM  -- SEG 이름
        ,MARKETING_ID AS MKTNG_ID -- 마케팅ID

  FROM SMARTWALLET.PROC_SS_PUSH
  WHERE SEG_ID IS NOT NULL

  ) sT3
  ON sT0.EVNT_ID = sT3.EV_ID

  LEFT OUTER JOIN
        (

           select t22.push_id,
                  t22.ev_id,
                  t22.ev_name

           from
           (


            SELECT
                     sT2.PUSH_ID
                    ,sT2.EV_ID AS EV_ID
                    ,sT2.EV_NAME AS EV_NAME
                    ,ROW_NUMBER() OVER( PARTITION BY sT2.push_id ORDER BY sT2.ev_id DESC ) AS RN
            FROM
                    (
                        SELECT
                                 sS1.EV_ID AS EV_ID
                                ,sS2.PUSH_ID AS PUSH_ID
                                ,sS1.EV_TITLE AS EV_NAME 
                        FROM SMARTWALLET.PROC_SS_PUSH sS1
                        INNER JOIN SMARTWALLET.PROCESS sS2
                        ON sS1.EV_ID = sS2.EV_ID
                        UNION ALL
                        SELECT
                                 sS1.EV_ID AS EV_ID
                                ,sS2.PUSH_ID AS PUSH_ID
                                ,sS1.EV_TITLE AS EV_NAME
                        FROM SMARTWALLET.PROC_SS_PUSH sS1
                        INNER JOIN SMARTWALLET.PROC_SS_PUSH_DTL sS2
                        ON sS1.EV_ID = sS2.EV_ID
                    ) sT2
            WHERE NVL(PUSH_ID, '') <> ''
            AND EV_ID >= 0       

           )t22
           where t22.RN=1


        ) sT2


  ON sT0.PUSH_ID = sT2.PUSH_ID


) mb
on ( ma.member_id = mb.member_id)
where ma.ci is not null and ma.ci <> ''
;" > sw_act_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -f sw_act_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='6');"

etl_stat 'DONE'

rm -f sw_act_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log


