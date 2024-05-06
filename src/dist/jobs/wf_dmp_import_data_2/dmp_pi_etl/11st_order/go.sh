#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_11st_purchase\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
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

PR_DATA_JOB_DATE=`date +%Y/%m/%d -d "2 day ago"`
#PR_DATA_JOB_DATE="2017/11/30"
PR_DATA_JOB_DATE2=`date +%Y/%m/%d -d "1 day ago"`

DROP_DAYS=`expr 367 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

ID_POOL=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE

OUT_ETL_11ST_SEARCH=$WORKDIR/etl_11st_purchase
etl_stat 'DOING'
run cp $DMP_PI_LOCAL_HOME/11st_disp_ct_dic.dat $DMP_PI_LOCAL_HOME/11st_order/
#####run cp $DMP_PI_LOCAL_HOME/11st_brand_dic.dat $DMP_PI_LOCAL_HOME/11st_order/
print_bline
print_bstr "ETL : 11st 구매/장바구니 정보 취합"

# mapper 1 : print '%s\t01\t%s\t%s\t%s\t%s' % (prd_no, member_no, cate, brand_cd, dt)
# mapper 2 : print '%s\t02\t%s\t%s\t\t%s' % (prd_no, member_no, cate, dt)
# mapper 3 : print '%s\t%s' % (prd_no, brand_cd)

# hdfs://skpds/data_bis/11st/raw/evs_pd_prd_hits/2017/07/24 뷰 테이블

if [ "$1" -gt "182" ]; then
INPUT_ADD=
else
INPUT_ADD="-input hdfs://skpds/data_bis/11st/raw/evs_pd_prd_hits/$PR_DATA_JOB_DATE"
fi

# tb_evs_ods_f_sd_ord_detl_rslt
# tb_evs_ods_f_tr_bckt
# tb_evs_ods_m_pd_prd


OUT_11ST_ORDER=$WORKDIR/temp_11st_purchase
run $HD -files map_11st_order.py,reduce_11st_order.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=128 \
    -input hdfs://skpds/data_bis/11st/raw/evs_sd_ord_detl_rslt/$PR_DATA_JOB_DATE \
    -input hdfs://skpds/data_db/11st/raw/evs_tr_bckt/$LAST_JOB_DATE \
    -input hdfs://skpds/data_bis/11st/raw/evs_pd_prd/$PR_DATA_JOB_DATE \
    $INPUT_ADD \
    -output $OUT_11ST_ORDER \
    -mapper "map_11st_order.py $LAST_JOB_DATE_NOSLA" \
    -reducer reduce_11st_order.py

#    -input hdfs://skpds/data_bis/11st/raw/evs_dd_prd/$PR_DATA_JOB_DATE \

print_bline
print_bstr "ETL : 11st 구매/장바구니"

#mapper 1 : temp_11st_tr_place -> member_no - CI
#mapper 2 : temp_11st_order    -> member_no - 그대로 찍기 
OUT_ETL_11ST_ORDER=$WORKDIR/etl_11st_order
#run $HD -files map_11st_purchase.py,reduce_11st_purchase.py,11st_brand_dic.dat,11st_disp_ct_dic.dat \
run $HD -files map_11st_purchase.py,reduce_11st_purchase.py,11st_disp_ct_dic.dat \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=32 \
    -input $OUT_11ST_ORDER \
    -input $ID_POOL \
    -output $OUT_ETL_11ST_ORDER \
    -mapper map_11st_purchase.py \
    -reducer reduce_11st_purchase.py


HIVEDIR1=hdfs://skpds/user/dmp_pi/dmp_pi_store_db/part_hour=${TARGET_JOB_DATE}00
HIVEDIR2=$HIVEDIR1/data_source_id=3
$HDD fs -mkdir $HIVEDIR1
run $HDD fs -mv $OUT_ETL_11ST_ORDER $HIVEDIR2

run /app/di/script/run_hivetl.sh -e 'MSCK REPAIR TABLE dmp_pi.prod_data_source_store;'
run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='3');"

etl_stat 'DONE'

rm -f *.log

