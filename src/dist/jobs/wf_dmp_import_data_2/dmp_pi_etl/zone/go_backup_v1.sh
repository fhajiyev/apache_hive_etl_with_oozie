#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_etl/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_zone\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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
ID_POOL=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE


DROP_DAYS=`expr 92 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

run cp $DMP_PI_LOCAL_HOME/m2_merch_zone.dat .
run cp $DMP_PI_LOCAL_HOME/m2zone.dat .
run cp $DMP_PI_LOCAL_HOME/mid_addr.dat .
run cp $DMP_PI_LOCAL_HOME/ocb_code.dat .

OUT_BLE_TR=$WORKDIR/temp_ble_tr

run $HD -files=map_ble_mbr.py,m2zone.dat,m2_merch_zone.dat,reduce_generic.py,mid_addr.dat,ocb_code.dat \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=64 \
    -input hdfs://skpds/data/newtech/raw/server/$LAST_JOB_DATE/** \
    -input hdfs://skpds/product_bis/BIS_SERVICES/ISTORE/SS3/MA/SUM/PROD_BLE_BLEKEY_MAPPING \
    -output $OUT_BLE_TR \
    -mapper map_ble_mbr.py \
    -reducer reduce_generic.py

OUT_ETL_ZONE=$WORKDIR/etl_zone
run $HD -files=map_zone.py,reduce_generic.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=128 \
    -input $OUT_BLE_TR \
    -input $ID_POOL \
    -output $OUT_ETL_ZONE \
    -mapper map_zone.py \
    -reducer reduce_generic.py

HIVEDIR1=hdfs://skpds/user/dmp_pi/dmp_pi_store_db/part_hour=${TARGET_JOB_DATE}00
HIVEDIR2=$HIVEDIR1/data_source_id=8
$HDD fs -mkdir $HIVEDIR1
run $HDD fs -mv $OUT_ETL_ZONE $HIVEDIR2

run /app/di/script/run_hivetl.sh -e 'MSCK REPAIR TABLE dmp_pi.prod_data_source_store;'
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='8');"
etl_stat 'DONE'

rm -f *.log

