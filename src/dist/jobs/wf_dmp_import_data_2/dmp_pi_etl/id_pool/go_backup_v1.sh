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

print_bstr "OCB 회원 정보 가져오기"
OUT_OCB_ID=$WORKDIR/temp_id_pool_ocb 
run $HD -files map_ocb_mbr.py,ocb_code.dat \
    -libjars /app/hive/lib/hive-exec-1.1.0.jar \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=0 \
    -inputformat org.apache.hadoop.hive.ql.io.orc.OrcInputFormat \
    -input hdfs://skpds/data_bis/ocb/MART/MBR/mart_mbr_mst  \
    -output $OUT_OCB_ID \
    -mapper map_ocb_mbr.py \

print_bstr "11번가, SW 회원 정보 가져와서 묶기"
OUT_ID_POOL=$WORKDIR/temp_id_pool
run $HD -files map_id_pool.py,reduce_id_pool.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=128 \
    -input $OUT_OCB_ID \
    -input hdfs://skpds/data_bis/11st/base/evs_mb_mem/$LAST_JOB_DATE \
    -input hdfs://skpds/data_bis/smartwallet/raw/mt3_member \
    -output $OUT_ID_POOL \
    -mapper map_id_pool.py \
    -reducer reduce_id_pool.py
HIVEDIR=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE
run $HDD fs -mv $OUT_ID_POOL $HIVEDIR 
run /app/di/script/run_hivetl.sh -e 'MSCK REPAIR TABLE dmp_pi.id_pool;'
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.id_pool drop partition (part_date < '$DROP_DATE');"
etl_stat 'etl_id_pool' 'DONE'
run $HDD fs -rm -r $OUT_OCB_ID
rm -f *.log





