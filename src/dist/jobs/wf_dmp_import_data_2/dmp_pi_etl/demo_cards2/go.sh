#!/bin/bash

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


LAST_JOB_DAY=2
LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`


DROP_DAYS=5
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

echo "
set mapreduce.job.reduces=256;
set mapre.job.queue.name=COMMON;
insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='21')

select distinct
       
       mall.uid,
       mall.card_type,
       mall.issue_date,

       CASE WHEN mall.card_type = '01' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '01' THEN mall.card_name ELSE '' END,

       CASE WHEN mall.card_type = '02' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '02' THEN mall.card_name ELSE '' END,

       CASE WHEN mall.card_type = '03' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '03' THEN mall.card_name ELSE '' END,

       CASE WHEN mall.card_type = '04' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '04' THEN mall.card_name ELSE '' END,

       CASE WHEN mall.card_type = '05' THEN mall.card_code ELSE '' END,
       CASE WHEN mall.card_type = '05' THEN mall.card_name ELSE '' END,


'','','','','','','','',
'','','','','','','','','','',
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

     ma.ci_no           as uid,
     ma.card_gubun      as card_type,
     ma.card_dtl_grp_cd as card_code,
     ma.dtl_cd_nm       as card_name, 
     ma.card_iss_dt     as issue_date

from (
select  d.ci_no, a.mbr_id, a.card_sts_cd, a.card_dtl_grp_cd, a.iss_alcmpn_cd, a.card_iss_dt,
         (case when a.crdt_card_yn = '1' then '02'
               when a.chkcrd_yn    = '1' then '03' else '01' end)  card_gubun,
        c.dtl_cd_nm ,b.alcmpn_nm
  from ( select mbr_id, card_sts_cd, card_dtl_grp_cd, iss_alcmpn_cd, card_iss_dt, crdt_card_yn, chkcrd_yn
           from ocb.mart_card_mst
          where card_sts_cd = 'A'  ) a
  join ocb.mart_alcmpn_mst  b
    on (a.iss_alcmpn_cd = b.alcmpn_cd)
  join (select dtl_cd , dtl_cd_nm
          from ocb.dw_ocb_intg_cd
         where domn_id  = 'CARD_DTL_GRP_CD') c
     on (a.card_dtl_grp_cd = c.dtl_cd)
   join  ocb.mart_mbr_mst d
     on (a.mbr_id = d.mbr_id)
   where d.ci_no is not null and d.ci_no <> '' and d.ci_no not like '%\u0001%'
) ma
union all
select

     mb.ci        as uid, 
     mb.card_gubn as card_type, 
     mb.card_id   as card_code, 
     mb.card_name as card_name, 
     mb.issue_dt  as issue_date

from (
select
  aa.ci, aa.member_id, bb.card_id, cc.card_name, substr(bb.issue_dt,1,8) as issue_dt, substr(bb.issue_dt,9,2) as issue_time,
  case
    when cc.card_type_cd = '28' then '05'
    else '04'
  end as card_gubn
  from smartwallet.mt3_member aa
  join smartwallet.mt3_mem_card bb on aa.member_id = bb.member_id
  join smartwallet.mt3_card cc on bb.card_id = cc.card_id
  where bb.status_cd in ('3','6') and aa.ci is not null and aa.ci <> ''

) mb

) mall;
" > demo_cards2_${LAST_JOB_DATE_NOSLA}.hql


run /app/di/script/run_hivetl.sh -f demo_cards2_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='21');"

etl_stat 'DONE'

rm -f demo_cards2_*.hql

rm -f *.log

