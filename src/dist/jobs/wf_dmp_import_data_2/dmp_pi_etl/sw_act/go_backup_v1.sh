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
  ma.date,
  '',
  case from_unixtime(unix_timestamp(ma.date, 'yyyyMMdd'), 'u')
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
  '','',
  '','','','','','','','','','',
  '','','','','','','','','','',
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
      substr(a.base_time,1,8) as date,
      a.current_page as page_id,
      a.action_id
  from smartwallet.mt3_member b
  join smartwallet.smw_client_log a on b.member_id = a.member_id
  where a.dt = '${LAST_JOB_DATE_NOSLA}'  -- µ¥ÀÌÅÍ ÀûÀç ÀÏÀÚ
      and a.current_page <> '/appexec'
      and a.current_page is not null -- app ±¸µ¿ ·Î±×
      and lower(trim(a.current_page)) not in ('', 'n') -- app ±¸µ¿ ·Î±× / ble server log
      and lower(trim(a.action_id)) not in ('event_receive', 'server_comm') -- ble server log
      and lower(trim(a.action_id)) not in ('background.display', 'background.display_native', 'background.dispaly_native') -- ÄíÆù/Ä«µå/¹è³Ê ³ëÃâ·Î±×¸¦ ¼öÁýÇÏ±â À§ÇÑ ·Î±×
      and lower(concat_ws('',trim(a.current_page),trim(a.action_id))) not in (-- '/start', '/start/guide' -- ½ÃÀÛÆäÀÌÁö/°¡ÀÌµåÆäÀÌÁö
                    '/ble/icon', 'ble icon', '/ble/network/error', '/ble/noti', '/ble/iconble_tap.delete' -- ble °ü·Ã ·Î±×
                   , '/preordericon', '/preordericonorder_tap.delete' -- syrup order °ü·Ã ·Î±×
                   , '/pushnoti/popup', '/pushnoti/indicator', '/push/noti', '/pushnoti/indicatorindicator_tap.notidel' -- push °ü·Ã ·Î±×
                   , '/pushnoti/indicator_bluetooth' -- ble zone¿¡¼­ ble¸¦ ÄÑ ´Þ¶ó´Â ÆË¾÷
                   , '/push/ticket' -- ¸Þ°¡¹Ú½º Æ¼ÄÏ °ü·Ã Çª½¬
                   , '/push/coin', '/push/coinlongpress.coin' -- ocb µ¿Àü Çª½¬
                   , '/wifi/noti' --È¨ÇÃ·¯½º wifi Çª½¬
                   , '/push/generalticket' -- ÀÏ¹Ý Æ¼ÄÏ ¾Ë¶÷ Çª½¬(ex. º¸±¤)
                   , '/push_device/noti' -- ´Ü¸» ÀÚÃ¼ Çª½¬(ex. mms)
                   , '/permission/main' -- android m ´ëÀÀ ¸ÞÀÎ ÆË¾÷
                   , '/noti/guide', '/welcome', '/noti/guidepushlist_tap.close', '/welcometap.close', '/shoppingbackground.display', '/newsbackground.display', '/guidelockbackground.display' -- ¸ô°¡ÀÌµå °ü·Ã Á¦¿Ü
                   , '/guidein/noti', '/guideout/noti', '/guidewelcome', '/guidewelcometap.close' -- ¸ô°¡ÀÌµå Ãß°¡
                   , '/main/couponbackground.couponlist' -- couponlist
                   , '/btpopup', '/btpopupbottom_tap.cancelbtn')
) ma
left outer join (


  select distinct 
        aa.member_id
     ,  bb.ev_id as push_id_recv
     ,  '' as push_id_resp 
     ,  '01' as push_type
     , '' as seg_id
     , '' as seg_nm
   from smartwallet.smw_client_log aa
   join( 
   
         select
                  t2.push_id
                 ,max(t2.ev_id) as ev_id
         from
                 (  select ev_id, push_id from smartwallet.ss_push_dtl union all
                     select
                              s1.ev_id as ev_id, s2.push_id as push_id from smartwallet.proc_ss_push s1
                     inner join 
                              smartwallet.process s2
                     on s1.ev_id = s2.ev_id union all
                     select
                              s1.ev_id as ev_id, s2.push_id as push_id
                     from smartwallet.proc_ss_push s1
                     inner join smartwallet.proc_ss_push_dtl  s2
                     on s1.ev_id = s2.ev_id
                 ) t2
         where nvl(push_id, '') <> '' 
         and ev_id >= 0 
         group by t2.push_id
                                                        
        ) bb on aa.push_id = bb.push_id
   
  where aa.current_page in ('/push/noti','/push/coin', '/push_device/noti') 
    and (aa.action_id is null or trim(aa.action_id) = '') 
    and aa.dt = '${LAST_JOB_DATE_NOSLA}'
    and aa.push_id is not null and trim(aa.push_id) <> ''


  union all


  select distinct 
        aa.member_id
     ,  '' as push_id_recv
     ,  bb.ev_id as push_id_resp
     ,  '02' as push_type 
     , '' as seg_id
     , '' as seg_nm
   from smartwallet.smw_client_log aa
   join( 
   
         select
                  t2.push_id
                 ,max(t2.ev_id) as ev_id
         from
                 (  select ev_id, push_id from smartwallet.ss_push_dtl union all
                     select
                              s1.ev_id as ev_id, s2.push_id as push_id from smartwallet.proc_ss_push s1
                     inner join
                              smartwallet.process s2
                     on s1.ev_id = s2.ev_id union all
                     select
                              s1.ev_id as ev_id, s2.push_id as push_id
                     from smartwallet.proc_ss_push s1
                     inner join smartwallet.proc_ss_push_dtl  s2
                     on s1.ev_id = s2.ev_id
                 ) t2
         where nvl(push_id, '') <> '' 
         and ev_id >= 0 
         group by t2.push_id
                                                        
        ) bb on aa.push_id = bb.push_id
   
  where aa.current_page in ('/push/noti','/push/coin', '/push_device/noti') 
    and (aa.action_id is not null and trim(aa.action_id) <> '') 
    and aa.dt = '${LAST_JOB_DATE_NOSLA}'
    and aa.push_id is not null and trim(aa.push_id) <> ''


  union all
  

  SELECT DISTINCT

        sT0.MBR_ID as member_id

  
        
        ,CASE WHEN NVL(sT0.EVNT_ID, '') <> '' THEN sT0.EVNT_ID ELSE sT2.EV_ID END as push_id_recv
        ,CASE WHEN NVL(sT0.EVNT_ID, '') <> '' THEN sT0.EVNT_ID ELSE sT2.EV_ID END as push_id_resp
        ,CASE WHEN sT0.PUSH_STEP_CD = '0' THEN '01' WHEN sT0.PUSH_STEP_CD = '2' THEN '02' END AS push_type
              
        , sT3.SEG_ID
        , sT3.SEG_NM              

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
            SELECT
                     sT2.PUSH_ID
                    ,MAX(sT2.EV_ID) AS EV_ID
            FROM
                    (
                        SELECT
                                 sS1.EV_ID AS EV_ID
                                ,sS2.PUSH_ID AS PUSH_ID
                        FROM SMARTWALLET.PROC_SS_PUSH sS1
                        INNER JOIN SMARTWALLET.PROCESS sS2
                        ON sS1.EV_ID = sS2.EV_ID
                        UNION ALL
                        SELECT
                                 sS1.EV_ID AS EV_ID
                                ,sS2.PUSH_ID AS PUSH_ID
                        FROM SMARTWALLET.PROC_SS_PUSH sS1
                        INNER JOIN SMARTWALLET.PROC_SS_PUSH_DTL sS2
                        ON sS1.EV_ID = sS2.EV_ID
                    ) sT2
            WHERE NVL(PUSH_ID, '') <> ''
            AND EV_ID >= 0
            GROUP BY sT2.PUSH_ID
        ) sT2
  ON sT0.PUSH_ID = sT2.PUSH_ID


) mb
on ( ma.member_id = mb.member_id)
where ma.ci is not null and ma.ci <> ''
;" > sw_act_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -f sw_act_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='6');"

etl_stat 'DONE'

rm -f sw_act_*.hql

rm -f *.log


