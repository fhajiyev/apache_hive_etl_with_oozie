#!/bin/sh
die() {
    echo >&2 -e "\nERROR: $@\n"; exit 1;
}
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }


demo_alter()
{
 
    ORG=`date +%Y%m%d -d "3 day ago"`
    DST=`date +%Y%m%d -d "2 day ago"`

    hadoop fs -cp dmp_pi_store_db/part_hour=${ORG}00/data_source_id=1 dmp_pi_store_db/part_hour=${DST}00/


}


run_except() { "$@"; code=$?; [ $code -ne 0 ] && demo_alter; }

cd /app/dmp_pi/dmp_pi_etl

./go_demo.sh &

cd /app/dmp_pi/dmp_pi_etl/dict
run ./01_ocbNzip_dict.sh

#run ./02_sw_card_detail.sh

run ./03_ocb_info.sh
echo s1 > err.log
run ./04_11st.sh
echo s2 >> err.log
run ./05_sol.sh
echo s3 >> err.log
run ./06_zone.sh
echo s4 >> err.log
run ./07_ocb_coupon_meta.sh
echo s5 >> err.log
cd /app/dmp_pi/dmp_pi_etl/id_pool
run ./go.sh > err.log 2>&1
echo s6 >> err.log


cd /app/dmp_pi/dmp_pi_etl

./go_11st_purchase.sh &

cd /app/dmp_pi/dmp_pi_etl

./go_11st_basket.sh &


STATS_DIR=/app/dmp_pi/dmp_pi_etl/etl_stat/




END_TIME_PREV=`date '+%s'`

cd /app/dmp_pi/dmp_pi_etl/sw_act
./go.sh 0 &

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 6 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/cim_seg
./additional_import.sh > /app/dmp_pi/dmp_pi_etl/log_addit.dat 2>&1

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 12 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/demo
./go.sh

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 1 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/11st_search
./go.sh 0

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 2 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/ocb_act
./go.sh 0

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 4 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/ocb_tr
./go.sh 0 &

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 5 $END_TIME_PREV $END_TIME


#END_TIME_PREV=$END_TIME

#cd /app/dmp_pi/dmp_pi_etl/11st_order
#run ./go.sh 0

#END_TIME=`date '+%s'`
#cd $STATS_DIR
#run ./dmp_timing_meta.sh 3 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/zone
./go.sh 0 &

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 8 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/11st_act
./go.sh 0

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 9 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/demo_mkt
./go.sh

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 10 $END_TIME_PREV $END_TIME


#END_TIME_PREV=$END_TIME

#cd /app/dmp_pi/dmp_pi_etl/demo_cards
#run ./go.sh

#END_TIME=`date '+%s'`
#cd $STATS_DIR
#run ./dmp_timing_meta.sh 11 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/cim_visit
./go.sh 0

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 13 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/d15
./go.sh 0

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 15 $END_TIME_PREV $END_TIME


#END_TIME_PREV=$END_TIME

#cd /app/dmp_pi/dmp_pi_etl/d16
#./go.sh 0

#END_TIME=`date '+%s'`
#cd $STATS_DIR
#run ./dmp_timing_meta.sh 16 $END_TIME_PREV $END_TIME


#END_TIME_PREV=$END_TIME

#cd /app/dmp_pi/dmp_pi_etl/d17
#./go.sh 0

#END_TIME=`date '+%s'`
#cd $STATS_DIR
#run ./dmp_timing_meta.sh 17 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/d21
./go.sh

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 21 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/sol
./go.sh 0

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 7 $END_TIME_PREV $END_TIME


END_TIME_PREV=$END_TIME

cd /app/dmp_pi/dmp_pi_etl/uid_lifetime
./uidlifetime_import.sh

END_TIME=`date '+%s'`
cd $STATS_DIR
run ./dmp_timing_meta.sh 23 $END_TIME_PREV $END_TIME



###stats DB load
cd $STATS_DIR
run ./dmp_timing_dbload.sh >> /app/dmp_pi/dmp_pi_etl/log_addit.dat




sleep 10

/app/yarn_etl/bin/hadoop fs -rm -r dmp_pi_store/*

exit


