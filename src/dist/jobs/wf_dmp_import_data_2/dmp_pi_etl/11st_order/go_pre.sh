#!/bin/sh

HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"

$HD -files map_pre.py \
    -D dfs.replication=2 \
    -D mapreduce.job.queuename=COMMON \
    -D mapreduce.job.reduces=128 \
    -input hdfs://skpds/data_bis/11st/raw/evs_sd_ord_detl_rslt/2017/06/28 \
    -output dmp_11st_pre \
    -mapper map_pre.py 

