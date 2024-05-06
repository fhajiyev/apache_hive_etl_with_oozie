#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi

HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_ocb_act\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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

OUT_ETL_OCB_ACT=$WORKDIR/etl_ocb_act
etl_stat 'DOING'
print_bline
print_bstr "OCB 액티비티 = 4"

# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_push_rctn_ctnt/2017/07/30	푸쉬
# hdfs://skpds/data_bis/ocb/MART/APP/mart_app_feed_clk_ctnt/2017/07/31	피드


print_bline
print_bstr "OCB 푸쉬 추출"

echo "set hive.execution.engine=tez;
set tez.queue.name=COMMON; 
use ocb;
drop table if exists dmp_pi.ocb_push;
create external table dmp_pi.ocb_push(mem_no STRING, push_id STRING, dt string)
STORED AS TEXTFILE LOCATION 'hdfs://skpds/user/dmp_pi/dmp_pi_ocb_push';
insert overwrite table dmp_pi.ocb_push
select mbr_id as mem_no, tgt_push_id as push_id, base_dt as dt
from ocb.MART_APP_PUSH_RCTN_CTNT     
where base_dt     = '$LAST_JOB_DATE_NOSLA'      
and tgt_push_id <> ''
and push_rcv_yn = '1';" > push.hql

run /app/di/script/run_hivetl.sh -f push.hql

print_bline
print_bstr "OCB 피드 추출"

echo "set hive.execution.engine=tez;
set tez.queue.name=COMMON; 
use ocb;
drop table if exists dmp_pi.ocb_feed;
create external table dmp_pi.ocb_feed(mem_no STRING, feed_id STRING, dt string)
STORED AS TEXTFILE LOCATION 'hdfs://skpds/user/dmp_pi/dmp_pi_ocb_feed';
insert overwrite table dmp_pi.ocb_feed
select mbr_id as mem_no, feed_id, base_dt as dt
from ocb.MART_APP_FEED_CLK_CTNT    
where base_dt    = '$LAST_JOB_DATE_NOSLA'       
and feed_clk_yn = '1';" > feed.hql

run /app/di/script/run_hivetl.sh -f feed.hql


run $HD -files map_ocb_act.py,reduce_ocb_act.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=32 \
    -input hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE \
    -input hdfs://skpds/product/BIS_SERVICES/OCB/INTG/OCB_D_INTG_LOG/poc=01/dt=$LAST_JOB_DATE_NOSLA \
    -input hdfs://skpds/user/dmp_pi/dmp_pi_ocb_push \
    -input hdfs://skpds/user/dmp_pi/dmp_pi_ocb_feed \
    -output $OUT_ETL_OCB_ACT \
    -mapper map_ocb_act.py \
    -reducer reduce_ocb_act.py

HIVEDIR1=hdfs://skpds/user/dmp_pi/dmp_pi_store_db/part_hour=${TARGET_JOB_DATE}00
HIVEDIR2=$HIVEDIR1/data_source_id=4
$HDD fs -mkdir $HIVEDIR1
run $HDD fs -mv $OUT_ETL_OCB_ACT $HIVEDIR2

run /app/di/script/run_hivetl.sh -e 'MSCK REPAIR TABLE dmp_pi.prod_data_source_store;'
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='4');"

etl_stat 'DONE'

rm -f *.log

