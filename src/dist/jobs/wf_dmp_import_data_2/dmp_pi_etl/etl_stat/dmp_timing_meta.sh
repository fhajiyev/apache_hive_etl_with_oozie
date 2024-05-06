#!/bin/sh

WORKDIR=/app/dmp_pi/dmp_pi_etl/etl_stat
TODAY_DT=`date '+%Y%m%d'`
EXPIRED_DT=`date '+%Y%m%d' -d '7 day ago'`
TODAY_DT_FORMATTED=`date '+%Y-%m-%d'`


cd $WORKDIR

rm -rf $EXPIRED_DT
mkdir -p $TODAY_DT

DATA_SOURCE_ID=$1

START_TIME=`date -d @$2`
END_TIME=`date -d @$3`
START_TIME_HHMMSS=`echo $START_TIME | cut -d' ' -f 5`
END_TIME_HHMMSS=`echo $END_TIME | cut -d' ' -f 5`

DURATION=$(($3-$2))
DURATION_MIN=`bc <<< "scale=2; $DURATION/60"`
DURATION_MIN_ROUND=`awk "BEGIN { printf \"%.0f\n\", $DURATION_MIN }"`


printf "'$TODAY_DT',$DATA_SOURCE_ID,'$TODAY_DT_FORMATTED $START_TIME_HHMMSS','$TODAY_DT_FORMATTED $END_TIME_HHMMSS',$DURATION_MIN_ROUND\n" >> $TODAY_DT/start_stop_times.dat


