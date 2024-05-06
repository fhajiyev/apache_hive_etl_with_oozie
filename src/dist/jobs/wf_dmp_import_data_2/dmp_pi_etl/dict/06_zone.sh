#!/bin/sh

HDD=/app/yarn_dic/bin/hadoop

die() { echo >&2 -e "\nERROR: $@\n"; exit 1; }
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl
WORKDIR=$DMP_PI_HDFS_HOME
HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"

LAST_JOB_DATE=`date +%Y/%m/%d -d "2 day ago"`

OUT_M2_ZONE=$WORKDIR/temp_m2zone

$HDD fs -rm -r $OUT_M2_ZONE

run $HD \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=1 \
    -input hdfs://skpds/data_db/imc/raw/m2_zone/$LAST_JOB_DATE \
    -output $OUT_M2_ZONE \
    -mapper cat
run $HDD fs -getmerge $OUT_M2_ZONE/*  $DMP_PI_LOCAL_HOME/m2zone.dat
run ls -alh $DMP_PI_LOCAL_HOME/m2zone.dat

run $HDD fs -getmerge hdfs://skpds/data_db/imc/raw/m2_merch_zone/$LAST_JOB_DATE/* $DMP_PI_LOCAL_HOME/m2_merch_zone.dat
run ls -alh $DMP_PI_LOCAL_HOME/m2_merch_zone.dat

OUT_MID_ADDR=$WORKDIR/temp_mid_addr
$HDD fs -rm -r $OUT_MID_ADDR
run $HD -files map_mid_addr.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=1 \
    -input hdfs://skpds/data_db/imc/raw/imc_store_basic/$LAST_JOB_DATE \
    -output $OUT_MID_ADDR \
    -mapper map_mid_addr.py
run $HDD fs -getmerge $OUT_MID_ADDR/* $DMP_PI_LOCAL_HOME/mid_addr.dat
run ls -alh $DMP_PI_LOCAL_HOME/mid_addr.dat
exit 0

