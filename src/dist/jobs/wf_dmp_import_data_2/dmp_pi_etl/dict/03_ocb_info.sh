#!/bin/sh

HDD=/app/yarn_etl/bin/hadoop

die() { echo >&2 -e "\nERROR: $@\n"; exit 1; }
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl
WORKDIR=$DMP_PI_HDFS_HOME/ocb_info
HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"

$HDD fs -rm -r hdfs://skpds/user/dmp_pi/dmp_pi_store/ocb_info

run cp $DMP_PI_LOCAL_HOME/ocb_code.dat $DMP_PI_LOCAL_HOME/dict/

print_bstr "OCB 카드 제휴사, 가맹점, 대중소 정보 가져오기"
# mapper mart_alcmpn_mst : 제휴사 코드 - A - 제휴사 이름 - 업종대 - 중 - 소 - 업종 코드
# mapper mart_mcnt_mst   : 제휴사 코드 - M - 가맹점 코드 - 가맹점 이름
OUT_OCB_INFO=$WORKDIR
run $HD -files map_ocb_info.py,ocb_code.dat \
    -libjars /app/hive/lib/hive-exec-1.1.0.jar \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=1 \
    -inputformat org.apache.hadoop.hive.ql.io.orc.OrcInputFormat \
    -input hdfs://skpds/data_bis/ocb/MART/CTR/mart_mcnt_mst \
    -input hdfs://skpds/data_bis/ocb/MART/CTR/mart_alcmpn_mst \
    -output $OUT_OCB_INFO \
    -mapper map_ocb_info.py

$HDD fs -getmerge $OUT_OCB_INFO $DMP_PI_LOCAL_HOME/ocb_info.dat
ls -alh $DMP_PI_LOCAL_HOME/ocb_info.dat
print_bline

