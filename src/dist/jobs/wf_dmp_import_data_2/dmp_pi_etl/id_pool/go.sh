#!/bin/sh

TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`
DROP_DATE=`date +%Y%m%d -d "5 day ago"`

HDD=/app/yarn_dic/bin/hadoop
etl_stat() { 
    CTIME=`date +%Y%m%d%H%M%S`
    printf "$1\t$CTIME\t$2\t$SECONDS" > proc_$CTIME.log
    $HDD fs -put proc_$CTIME.log hdfs://skpds/user/dmp_pi/dmp_pi_etl_stat/
}

## 1. base 중단 함수. 에러가 발생하면 중단시킨다
die() { 
    etl_stat 'etl_id_pool' 'FAIL';
    echo >&2 -e "\nERROR: $@\n"; exit 1; 
}
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }
print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }
print_sstr() { printf "\e[93m$@\e[0m\n"; }

HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TODAY_JOB_DATE

LAST_JOB_DATE=`date +%Y/%m/%d -d "2 day ago"`

cd $DMP_PI_LOCAL_HOME/id_pool

run cp $DMP_PI_LOCAL_HOME/ocb_code.dat $DMP_PI_LOCAL_HOME/id_pool/

etl_stat 'etl_id_pool' 'DOING'


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.id_pool partition (part_date='${TODAY_JOB_DATE}')

select distinct

t.uid,
t.ocb_id,
t.sw_id,
t.elev_id,
t.mdnval

from

(


      SELECT
      ci_no                               as uid,
      mbr_id                              as ocb_id,
      ''                                  as sw_id,
      ''                                  as elev_id,
      clphn_no                            as mdnval 
      FROM
      ocb.mart_mbr_mst
      WHERE ci_no IS NOT NULL AND length(ci_no)>0 AND ci_no <> '\n' AND ci_no not like '%\u0001%' 
      AND mbr_sts_cd = 'A'


      UNION ALL

      SELECT
      ci                                  as uid,
      ''                                  as ocb_id,
      member_id                           as sw_id,
      ''                                  as elev_id,
      mdn                                 as mdnval    
      FROM
      smartwallet.mt3_member
      WHERE ci IS NOT NULL AND length(ci)>0 AND ci <> '\n' AND ci not like '%\u0001%' 
      AND wallet_accept = 1 and wallet_accept1 = 1 and wallet_accept2 = 1 and vm_state_cd = '9' and length(last_auth_dt) = 14


      UNION ALL

      SELECT
      mem_ci                              as uid,
      ''                                  as ocb_id,
      ''                                  as sw_id,
      mem_no                              as elev_id,
      ''                                  as mdnval 
      FROM
      11st.tb_evs_base_m_mb_mem
      WHERE part_date='${TODAY_JOB_DATE}'
      AND mem_ci IS NOT NULL AND length(mem_ci)>0 AND mem_ci <> '\n' AND mem_ci not like '%\u0001%' 
      AND mem_clf = '01' AND mem_typ_cd = '01' AND mem_stat_cd = '01'


) t
;" > id_pool_hive.hql


run /app/di/script/run_hivetl.sh -f id_pool_hive.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.id_pool drop partition (part_date < '$DROP_DATE');"

etl_stat 'DONE'

rm -f id_pool_hive.hql

rm -f *.log




