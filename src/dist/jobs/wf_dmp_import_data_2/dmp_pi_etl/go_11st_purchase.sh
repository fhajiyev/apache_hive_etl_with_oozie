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







STATS_DIR=/app/dmp_pi/dmp_pi_etl/etl_stat/




END_TIME_PREV=`date '+%s'`

cd /app/dmp_pi/dmp_pi_etl/d16
./go.sh 0

END_TIME=`date '+%s'`
cd $STATS_DIR
./dmp_timing_meta.sh 16 $END_TIME_PREV $END_TIME






sleep 10


exit


