#!/bin/sh

export JAVA_HOME=/app/jdk
export JAVA=$JAVA_HOME/bin/java


/app/di/qcshell/bin/qcshell -b eda-impala -n "PS05153" -p "Yx438828" -e "refresh dmp_pi.prod_data_source_store_parquet"
/app/di/qcshell/bin/qcshell -b eda-impala -n "PS05153" -p "Yx438828" -e "compute incremental stats dmp_pi.prod_data_source_store_parquet"

sleep 3

