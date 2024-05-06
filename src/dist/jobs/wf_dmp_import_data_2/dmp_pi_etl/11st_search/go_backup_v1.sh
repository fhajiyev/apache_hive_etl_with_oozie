#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_11st_search\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`

TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

DROP_DAYS=`expr 182 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

OUT_ETL_11ST_SEARCH=$WORKDIR/etl_11st_search
etl_stat 'DOING'
print_bline
print_bstr "11번가 검색 = 2"

run $HD -files map_11st_search.py,reduce_generic.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=16 \
    -input hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE \
    -input hdfs://skpds/user/hive/warehouse/svc_cdpf.db/intgt_srch_keyword_mbr \
    -output $OUT_ETL_11ST_SEARCH \
    -mapper "map_11st_search.py $LAST_JOB_DATE_NOSLA"\
    -reducer reduce_generic.py

#    -input hdfs://skpds/data_bis/11st/dm/log_web_keywords/$LAST_JOB_DATE \
#    -input hdfs://skpds/data_bis/11st/dm/log_mobile_keywords/$LAST_JOB_DATE \

HIVEDIR1=hdfs://skpds/user/dmp_pi/dmp_pi_store_db/part_hour=${TARGET_JOB_DATE}00
HIVEDIR2=$HIVEDIR1/data_source_id=2
$HDD fs -mkdir $HIVEDIR1
run $HDD fs -mv $OUT_ETL_11ST_SEARCH $HIVEDIR2

run /app/di/script/run_hivetl.sh -e 'MSCK REPAIR TABLE dmp_pi.prod_data_source_store;'
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='2');"

etl_stat 'DONE'

rm -f *.log

