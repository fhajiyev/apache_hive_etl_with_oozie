#!/bin/bash
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_etl/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_solution\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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

IST_JOB_DATE=`date +%Y/%m/%d -d "1 day ago"`
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
ID_POOL=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE


DROP_DAYS=`expr 367 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
insert overwrite table dmp_pi.solution_pre_mkt partition (part_date='${LAST_JOB_DATE_NOSLA}')

select 
  distinct g.marketing_id as mkt_id, 
  g.taid as taid, 
  qa.alliance_nm as aname, 
--  g.mid as mid, 
--  qc.sid as sid,
  qr.lgrp_cd as lc,
  qr.mgrp_cd as mc,
  qr.sgrp_cd as sc,
  qr.lgrp_nm as lm,
  qr.mgrp_nm as mm,
  qr.sgrp_nm as sm,

  if(q.serv_sol_id is null, '', q.serv_sol_id) as serv_sol_id,
  if(g.mkt_nm      is null, '',      g.mkt_nm) as mkt_nm

--  qr.sgrp_nm as sm,
--  qe.mid_nm as mname,
--  qf.bname as sid_name
from        
(
  select aa.marketing_id, aa.taid as taid, aa.mid as mid, bb.marketing_title as mkt_nm 
  from
  ( 
    select distinct marketing_id, use_plc_id as taid, '' mid
    from istore.dbm_mkt_use_plc_v 
    where use_plc_type in ('02', '03')
    union all
    select distinct marketing_id, taid, a.mid 
    from 
    (
      select marketing_id, use_plc_id as mid 
      from istore.dbm_mkt_use_plc_v
      where use_plc_type ='01'
    ) a 
    join (select mid, taid from imc.dbm_imc_m2_merch_alliance_v) b on a.mid = b.mid
  ) as aa
  
  left join
  (

    select
    marketing_id,
    marketing_title
    from
    istore.dbm_prd_mkt
    where part_date = '${LAST_JOB_DATE_NOSLA}'

  ) as bb on aa.marketing_id = bb.marketing_id

) g
join
(
  select mkt_id, event_id as serv_sol_id, hhh.solution_id from PICASO.dbm_e_brand_service_v hhh where hhh.solution_id='0107'
  union all 
  select marketing_id as mkt_id, service_solution_id as serv_sol_id, jjj.solution_id from istore.dbm_prd_mkt_set_v jjj
--  join (select * from istore.dbm_prd_sltn_v where solution_code ='0104') p on (jjj.solution_id = p.solution_id)
) q
on (g.marketing_id = q.mkt_id)
join
(
  select taid, alliance_nm, cate 
  from IMC.dbm_imc_m2_alliance_basic_v
) qa
on (qa.taid = g.taid)
join 
(
  select wb.cate_is_cd lgrp_cd, wb.cate_name lgrp_nm, wc.cate_is_cd mgrp_cd, wc.cate_name mgrp_nm, wa.cate_is_cd sgrp_cd, wa.cate_name sgrp_nm
  from IMC.dbm_imc_m2_cate_is_v wa
  join IMC.dbm_imc_m2_cate_is_v wb
  on substr(wa.cate_is_cd,1,2) = wb.cate_is_cd
  join IMC.dbm_imc_m2_cate_is_v wc
  on substr(wa.cate_is_cd,1,4) = wc.cate_is_cd              
) qr
on (qr.sgrp_cd = qa.cate)  
--left outer join 
--(
--  select mid, sid from IMC.dbm_imc_m2_merch_source_v
--) qc
----on (qc.mid = g.mid)
--left outer join
--(
--  select mid, sname mid_nm from IMC.dbm_imc_m2_merch_basic_v
--) qe
--on (qe.mid = g.mid)
--left outer join
--( 
--  select sid, bname from IMC.dbm_imc_m2_merch_source_v
--) qf
--on (qf.sid = qc.sid)
;
" > solution_pre_mkt_${LAST_JOB_DATE_NOSLA}.hql


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
insert overwrite table dmp_pi.solution_pre_mkt_all partition (part_date='${LAST_JOB_DATE_NOSLA}')
  select
    distinct case sa.poc_ind_cd
        when 'O' then concat('OCB_', sa.mbr_id)
        when 'S' then concat('SYRUP_', sa.mbr_id)
    end as mbr_id,
    sa.mkt_id as mkt_id, 
    case sa.poc_ind_cd
        when 'O' then '01'
        when 'S' then '02'
    end as channel,    
    sa.dt as dt, 
    substr(sa.tm,9,2) std_tm, 
    case from_unixtime(unix_timestamp(sa.dt, 'yyyyMMdd'), 'u')
      when 1 then 'mon'
      when 2 then 'tue'
      when 3 then 'wed'
      when 4 then 'thu'
      when 5 then 'fri'
      when 6 then 'sat'
      else 'sun'
    end as week,
    tt.taid as taid,
    case sa.sltn_full_actn_cd
        when '0107EN01' then '01'
        when '0107PU01' then '02'
        when '0107PU02' then '03'
        when '0104PU01' then '04'
        when '0104EN01' then '05'
        when '06'       then '06'
        when '07'       then '07' 
    end as action,
    tt.aname as aname,
    tt.lc, 
    tt.mc,
    tt.sc,
    tt.lm,
    tt.mm,
    tt.sm,
    concat(tt.lm, '||', tt.mm, '||', tt.sm) as cnames,
--    if (sa.sltn_full_actn_cd <> '0107PU01', sa.sltn_full_actn_cd, sa.sid) my_sid,
--    if (sa.sltn_full_actn_cd <> '0107PU01', sa.sltn_full_actn_cd, tt.sid) tb_sid,
--    if (sa.sltn_full_actn_cd <> '0107PU01', sa.sltn_full_actn_cd, tt.mid) my_sid,
--    if (sa.sltn_full_actn_cd <> '0107PU01', sa.sltn_full_actn_cd, qm.mid) tb_mid,
--    if (sa.sltn_full_actn_cd <> '0107PU01', sa.sltn_full_actn_cd, tt.mname),
--    if (sa.sltn_full_actn_cd <> '0107PU01', sa.sltn_full_actn_cd, tt.sid_name)
--    sa.sid as my_sid,
--    tt.sid as tb_sid,
--    tt.mid as my_mid,
--    qm.mid as tb_mid,
--    tt.mname,
    sa.sid,
    qf.mcnt_nm as sid_name,
    tt.serv_sol_id,
    tt.mkt_nm
  from
  (              
    select mkt_id, mbr_id, max(std_tm) tm, poc_ind_cd, dt, sltn_full_actn_cd, sid
    from ISTORE.ss3_d_sltn_cust_asst_src_sta 
    where dt = '${LAST_JOB_DATE_NOSLA}' 
    and CUST_IND_CD = 'I' 
    and
    (
      sltn_full_actn_cd = '0107EN01' or 
      sltn_full_actn_cd = '0107PU01' or
      sltn_full_actn_cd = '0107PU02' or
      sltn_full_actn_cd = '0104PU01' or
      sltn_full_actn_cd = '0104EN01' 

    )  
    and mbr_id is not null
    and mbr_id <> ''
    and mkt_id is not null
    and mkt_id <> ''
    group by mkt_id, mbr_id, sid, dt, poc_ind_cd, sltn_full_actn_cd



    union all



    select
    bb.marketing_id          as mkt_id,
    aa.member_id             as mbr_id,
    aa.base_time             as tm,
    'S'                      as poc_ind_cd,
    substr(aa.base_time,1,8) as dt,
    '06'                     as sltn_full_actn_cd,
    ''                       as sid

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
                  t22.ev_name,
                  t22.marketing_id 
         from
         (

             select   t2.push_id
                     ,t2.ev_id as ev_id
                     ,t2.ev_name as ev_name
                     ,t2.marketing_id as marketing_id   
                     ,ROW_NUMBER() OVER( PARTITION BY t2.push_id ORDER BY t2.ev_id DESC ) AS RN
             from
                     (  select
                              s1.ev_id as ev_id, s2.push_id as push_id, s1.ev_title as ev_name, s1.marketing_id as marketing_id from smartwallet.proc_ss_push s1
                            inner join
                                  smartwallet.process s2
                            on s1.ev_id = s2.ev_id
                            where s1.marketing_id is not null
                     
                        union all

                        select
                              s1.ev_id as ev_id, s2.push_id as push_id, s1.ev_title as ev_name, s1.marketing_id as marketing_id from smartwallet.proc_ss_push s1
                            inner join
                                  smartwallet.proc_ss_push_dtl s2
                            on s1.ev_id = s2.ev_id
                            where s1.marketing_id is not null
 
                     ) t2
                     where nvl(push_id, '') <> ''
                     and ev_id >= 0
         ) t22
         where t22.RN=1

        ) bb on aa.push_id = bb.push_id



    union all



    select
    bb.marketing_id          as mkt_id,
    aa.member_id             as mbr_id,
    aa.base_time             as tm,
    'S'                      as poc_ind_cd,
    substr(aa.base_time,1,8) as dt,
    '07'                     as sltn_full_actn_cd,
    ''                       as sid

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
                  t22.ev_name,
                  t22.marketing_id
         from
         (

            select
                     t2.push_id
                    ,t2.ev_id as ev_id
                    ,t2.ev_name as ev_name
                    ,t2.marketing_id as marketing_id
                    ,ROW_NUMBER() OVER( PARTITION BY t2.push_id ORDER BY t2.ev_id DESC ) AS RN

            from
                    (  select
                             s1.ev_id as ev_id, s2.push_id as push_id, s1.ev_title as ev_name, s1.marketing_id as marketing_id from smartwallet.proc_ss_push s1
                           inner join
                                 smartwallet.process s2
                           on s1.ev_id = s2.ev_id
                           where s1.marketing_id is not null

                       union all

                       select
                             s1.ev_id as ev_id, s2.push_id as push_id, s1.ev_title as ev_name, s1.marketing_id as marketing_id from smartwallet.proc_ss_push s1
                           inner join
                                 smartwallet.proc_ss_push_dtl s2
                           on s1.ev_id = s2.ev_id
                           where s1.marketing_id is not null

                    ) t2
            where nvl(push_id, '') <> ''
            and ev_id >= 0

         )t22
         where t22.RN=1

        ) bb on aa.push_id = bb.push_id



    union all



    select
    sT3.mktng_id              as mkt_id,
    sT0.mbr_id                as mbr_id,
    sT0.base_dttm             as tm,
    'S'                       as poc_ind_cd,
    sT0.part_dt               as dt,
    CASE
       WHEN sT0.push_step_cd = '0' THEN '06'
       WHEN sT0.push_step_cd = '2' THEN '07'
    END                       as sltn_full_actn_cd,
    ''                        as sid


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
  WHERE MARKETING_ID IS NOT NULL

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



  ) as sa 
  left outer join
  (
    select mkt_id, taid, aname, lc, mc, sc, lm, mm, sm, serv_sol_id, mkt_nm from dmp_pi.solution_pre_mkt where part_date='${LAST_JOB_DATE_NOSLA}'
--    select mkt_id, taid, aname, lc, mc, sc, lm, mm, sm, sid, mid, mname, sid_name from dmp_pi.solution_pre_mkt where part_date='${LAST_JOB_DATE_NOSLA}'
  ) tt 
  on (sa.mkt_id = tt.mkt_id)
--  left outer join 
--  (
--    select mid, sid from imc.dbm_imc_m2_merch_source_v where sid is not null and sid <> ''
--    group by mid, sid
--  ) qm
--  on (qm.sid = sa.sid)
  left outer join
  ( 
    select mcnt_cd, mcnt_nm from ocb.mart_mcnt_mst where mcnt_cd is not null and mcnt_cd <> ''
--    select sid, bname from IMC.dbm_imc_m2_merch_source_v where sid is not null and sid <> ''
  ) qf
  on (qf.mcnt_cd = sa.sid)
;
" > solution_pre_mkt_all_${LAST_JOB_DATE_NOSLA}.hql


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='7')
select
  distinct ids.uid as uid,
  att.dt as col001,
  att.std_tm as col002,
 att.week as col003,
 att.action as col004, 
 att.serv_sol_id, -- att.channel as col005,
 att.mkt_id as col006,
 att.mkt_nm as col007,
 att.taid as col008,
 att.aname as col009,
 att.lc as col010,
 att.mc as col011,
 att.sc as col012,
 att.lm as col013,
 att.mm as col014,
 att.sm as col015,
 att.cnames as col016,
 if (att.action = '02', att.sid, '') col017,
 if (att.action = '02', att.sid_name, '') col018,
 '', '', 
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', ''
from dmp_pi.solution_pre_mkt_all att
join 
( 
  select ci as uid, concat('OCB_', ocb_id) as mbr_id from dmp_pi.id_pool where ocb_id <> '' and part_date='${TODAY_JOB_DATE}'
  union all
  select ci as uid, concat('SYRUP_', sw_id) as mbr_id from dmp_pi.id_pool where sw_id <> '' and part_date='${TODAY_JOB_DATE}'
) ids 
on
(ids.mbr_id = att.mbr_id)
where att.part_date = '${LAST_JOB_DATE_NOSLA}';
" > solution_pre_final_${LAST_JOB_DATE_NOSLA}.hql



/app/di/qcshell/bin/qcshell -b eda-hive2 -f solution_pre_mkt_${LAST_JOB_DATE_NOSLA}.hql
/app/di/qcshell/bin/qcshell -b eda-hive2 -f solution_pre_mkt_all_${LAST_JOB_DATE_NOSLA}.hql
/app/di/qcshell/bin/qcshell -b eda-hive2 -f solution_pre_final_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='7');"
#run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.solution_pre_mkt drop partition (part_date <> '');"
#run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.solution_pre_mkt_all drop partition (part_date <> '');"

etl_stat 'DONE'

rm -f solution_pre_mkt_${LAST_JOB_DATE_NOSLA}.hql
rm -f solution_pre_mkt_all_${LAST_JOB_DATE_NOSLA}.hql
rm -f solution_pre_final_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log

