#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_ocb_tr\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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

DROP_DAYS=`expr 367 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE
ID_POOL=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE

run cp $DMP_PI_LOCAL_HOME/ocb_info.dat $DMP_PI_LOCAL_HOME/ocb_tr/
run cp $DMP_PI_LOCAL_HOME/ocb_coupon.dat $DMP_PI_LOCAL_HOME/ocb_tr/

etl_stat 'DOING'
print_bline
print_bstr "OCB TR = 5"

OUT_OCB_TR=$WORKDIR/temp_ocb_tra
LAST_JOB_MONTH=`date +%Y/%m -d "$LAST_JOB_DAY day ago"`
run $HD -files ocb_info.dat,ocb_coupon.dat,map_ocb_tr_mbr.py \
    -libjars /app/hive/lib/hive-exec-1.1.0.jar \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=0 \
    -inputformat org.apache.hadoop.hive.ql.io.orc.OrcInputFormat \
    -input hdfs://skpds/data_bis/ocb/MART/BIL/mart_anal_sale_info/$LAST_JOB_MONTH \
    -output $OUT_OCB_TR \
    -mapper "map_ocb_tr_mbr.py $LAST_JOB_DAY"

print_bline
print_sstr "ETL : OCB Transaction"


OUT_ETL_OCB_TR=$WORKDIR/etl_ocb_tr
run $HD -files map_ocb_tr.py,reduce_generic.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=32 \
    -input $ID_POOL \
    -input $OUT_OCB_TR \
    -output $OUT_ETL_OCB_TR \
    -mapper map_ocb_tr.py \
    -reducer reduce_generic.py

HIVEDIR1=hdfs://skpds/user/dmp_pi/dmp_pi_store_db/part_hour=${TARGET_JOB_DATE}00
HIVEDIR2=$HIVEDIR1/data_source_id=5
$HDD fs -mkdir $HIVEDIR1
run $HDD fs -mv $OUT_ETL_OCB_TR $HIVEDIR2

run /app/di/script/run_hivetl.sh -e 'MSCK REPAIR TABLE dmp_pi.prod_data_source_store;'
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='5');"

etl_stat 'DONE'

rm -f *.log

