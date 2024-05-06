#!/bin/sh
HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_demo\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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

LAST_JOB_DAY=2
LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`

TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

DROP_DAYS=5
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

etl_stat 'DOING'
print_bline
print_bstr "통합마케팅 디비"

echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${TARGET_JOB_DATE}00', data_source_id='10')
select distinct  b.mem_ci, a.segs,
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', ''
from
(
  select mbr_id, '01' as segs 
  from svc_cdpf.uspf_lfstg_evs_buy
  where dt ='${LAST_JOB_DATE_NOSLA}'  and marry_yn = 'Y'
  union all 
  select mbr_id, '04' as segs 
  from svc_cdpf.uspf_lfstg_evs_buy
  where dt ='${LAST_JOB_DATE_NOSLA}'  and oph_yn = 'Y'
  union all
  select mbr_id, '02' as segs 
  from svc_cdpf.uspf_lfstg_evs_buy
  where dt ='${LAST_JOB_DATE_NOSLA}'  and baby_yn = 'Y'
  union all
  select mbr_id, '03' as segs 
  from svc_cdpf.uspf_lfstg_evs_buy
  where dt ='${LAST_JOB_DATE_NOSLA}'  and kids_yn = 'Y'      
) a
join (select * from 11st.tb_evs_base_m_mb_mem where part_date='${LAST_JOB_DATE_NOSLA}') b
on (a.mbr_id = b.mem_no);
" > demo_mkt.hql 
 
run /app/di/script/run_hivetl.sh -f demo_mkt.hql
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='10');"

etl_stat 'DONE'


