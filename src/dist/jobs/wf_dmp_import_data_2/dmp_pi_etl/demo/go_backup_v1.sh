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

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TODAY_JOB_DATE
ID_POOL=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE

run cp $DMP_PI_LOCAL_HOME/ocb_code.dat $DMP_PI_LOCAL_HOME/demo/
#run cp $DMP_PI_LOCAL_HOME/sw_card_detail.dat $DMP_PI_LOCAL_HOME/demo/
run cp $DMP_PI_LOCAL_HOME/zip_code.dat $DMP_PI_LOCAL_HOME/demo/

OUT_SW=$WORKDIR/temp_sw

etl_stat 'DOING'
print_bline
print_bstr "시럽 푸시 동의 가져오기"
run /app/di/script/run_hivetl.sh -f sw_agree.hql
run $HDD fs -getmerge hdfs://skpds/user/dmp_pi/dmp_pi_sw_agree/* sw_agree.dat


run $HD -files map_sw.py,reduce_sw.py,sw_agree.dat\
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=128 \
    -input hdfs://skpds/data_bis/smartwallet/raw/mt3_member \
    -input $ID_POOL \
    -output $OUT_SW \
    -mapper map_sw.py \
    -reducer reduce_sw.py

print_bline
print_bstr "11번가 OS 정보 추출"

echo "set hive.execution.engine=tez;
set tez.queue.name=COMMON; 
use 11st;
drop table if exists dmp_pi.elev_os;
create external table dmp_pi.elev_os(mem_no STRING, os_str STRING, update_dt STRING)
STORED AS TEXTFILE LOCATION 'hdfs://skpds/user/dmp_pi/dmp_pi_elev_os';
insert overwrite table dmp_pi.elev_os
select distinct bb.mem_no, aa.os_nm, aa.update_dt from 11st.tb_evs_ods_m_mo_cust_device_info as aa, 11st.tb_evs_ods_m_mo_app_push_info_new as bb where aa.device_id=bb.device_id and bb.mem_no <> '' and aa.part_date='$LAST_JOB_DATE_NOSLA' and bb.part_date='$LAST_JOB_DATE_NOSLA';" > elev_os.hql

run /app/di/script/run_hivetl.sh -f elev_os.hql

# hdfs://skpds/data_db/11st/raw/evs_mo_app_push_info ; 쇼핑 이벤트 동의 여부
# hdfs://skpds/product_bis/svc_cdpf/USPF/EVS/LFSTG/USPF_LFSTG_EVS_BUY : cdpf 세그
print_bstr "ETL : 11번가 배송지 추출"

OUT_11ST_TR=$WORKDIR/temp_11st_tr_place

run $HD -files map_11st_tr_last_order.py,reduce_11st_tr_last_order.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=64 \
    -input hdfs://skpds/data_bis/11st/raw/evs_tr_ord_prd/$LAST_JOB_DATE \
    -input hdfs://skpds/data_db/11st/raw/evs_mo_app_push_info/$LAST_JOB_DATE \
    -input $ID_POOL \
    -input hdfs://skpds/user/dmp_pi/dmp_pi_elev_os \
    -output $OUT_11ST_TR \
    -mapper map_11st_tr_last_order.py \
    -reducer reduce_11st_tr_last_order.py

print_bline
print_bstr "ETL : 데모그래피 - 11번가"
# mapper 1 : 배송지 일련번호, CI
# mapper 2 : 배송지 일련번호, -''-''- 시군, 구, 동
# reducer : CI, 시, 군, 동
OUT_11ST=$WORKDIR/temp_11st
OUT_11ST_TR=$WORKDIR/temp_11st_tr_place

run $HD -files map_11st.py,zip_code.dat,reduce_11st.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=128 \
    -input $OUT_11ST_TR \
    -input hdfs://skpds/data_db/11st/raw/evs_tr_ord_clm_delvplace_ogg/$LAST_JOB_DATE \
    -output $OUT_11ST \
    -mapper map_11st.py \
    -reducer reduce_11st.py

# OCB 적립/사용 정보 가져오기
print_bstr "ETL : OCB 적립/사용정보 가져오기"

#    -D mapred.output.compress=true -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
OUT_OCB=$WORKDIR/temp_ocb


run $HD -files ocb_code.dat,map_ocb.py,reduce_ocb.py \
    -libjars /app/hive/lib/hive-exec-1.1.0.jar \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=128 \
    -inputformat org.apache.hadoop.hive.ql.io.orc.OrcInputFormat \
    -input hdfs://skpds/data_bis/ocb/MART/MBR/mart_mbr_mst \
    -input hdfs://skpds/data_bis/ocb/DW/NXM_BIL/dw_mbr_pnt_info \
    -input hdfs://skpds/data_bis/ocb/MART/MBR/mart_tot_agrmt_mgmt_mst \
    -input hdfs://skpds/data_bis/ocb/MART/APP/mart_app_mbr_mst/$LAST_JOB_DATE \
    -output $OUT_OCB \
    -mapper map_ocb.py \
    -reducer reduce_ocb.py
print_bline

print_sstr "ETL : 데모그래피 통합"
OUT_ETL_DEMO=$WORKDIR/etl_demo

run $HD -files map_demo.py,reduce_demo.py,sw_agree.dat \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=256 \
    -input $ID_POOL \
    -input $OUT_OCB \
    -input $OUT_11ST \
    -input $OUT_SW \
    -output $OUT_ETL_DEMO \
    -mapper map_demo.py \
    -reducer reduce_demo.py
print_bline

HIVEDIR1=hdfs://skpds/user/dmp_pi/dmp_pi_store_db/part_hour=${TARGET_JOB_DATE}00
HIVEDIR2=$HIVEDIR1/data_source_id=1

$HDD fs -mkdir $HIVEDIR1
run $HDD fs -mv $OUT_ETL_DEMO $HIVEDIR2

run /app/di/script/run_hivetl.sh -e 'MSCK REPAIR TABLE dmp_pi.prod_data_source_store;'
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='1');"

etl_stat 'DONE'

rm -f *.log

