#!/bin/sh

export JAVA_HOME=/app/jdk
export JAVA=$JAVA_HOME/bin/java


/app/di/qcshell/bin/qcshell -b eda-impala -n "PS03721" -p "Pe958319" -e "refresh dmp.prod_data_source_store_parquet"
/app/di/qcshell/bin/qcshell -b eda-impala -n "PS03721" -p "Pe958319" -e "compute stats dmp.prod_data_source_store_parquet"

sleep 3

#/app/hdfs/bin/hadoop fs -rm -r /user/hive/warehouse/dmp.db/prod_data_source_store/part_hour=temp_*

sleep 3

#/app/di/script/run_hivetl.sh -e 'MSCK REPAIR TABLE dmp.prod_data_source_store;'

sleep 3


