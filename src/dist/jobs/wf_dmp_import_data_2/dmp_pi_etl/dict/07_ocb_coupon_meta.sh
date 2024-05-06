#!/bin/sh

HDD=/app/yarn_etl/bin/hadoop

die() { echo >&2 -e "\nERROR: $@\n"; exit 1; }
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }


DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl
WORKDIR=$DMP_PI_HDFS_HOME
HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"

LAST_JOB_DATE=`date +%Y/%m/%d -d "2 day ago"`


OUT_OCB_COUPON_TMP=$WORKDIR/temp_dw_cpn_mst

$HDD fs -rm -r $OUT_OCB_COUPON_TMP


run $HD \
    -libjars /app/hive/lib/hive-exec-1.1.0.jar \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=1 \
    -inputformat org.apache.hadoop.hive.ql.io.orc.OrcInputFormat \
    -input hdfs://skpds/data_bis/ocb/DW/NXM_PRD/dw_cpn_mst \
    -output $OUT_OCB_COUPON_TMP \
    -mapper cat
run $HDD fs -getmerge $OUT_OCB_COUPON_TMP /app/dmp_pi/dmp_pi_etl/ocb_coupon.dat
run ls -alh /app/dmp_pi/dmp_pi_etl/ocb_coupon.dat
print_bline

