#!/bin/sh

export LANG=ko_KR.UTF-8

DMP_PREPARE_HOME=/app/dmp/dmp/jobs/wf_dmp_import_data/syrupad_dmp
HIVE_PATH=/app/di/hive_etl/bin/hive

cd $DMP_PREPARE_HOME

DMP_TERM=1
DMP_FROM_DT=`date --date "$DMP_TERM days ago" '+%Y%m%d'`
DMP_TO_DT=`date --date "$DMP_TERM days ago" '+%Y%m%d'`

$HIVE_PATH --hivevar db="dmp" --hivevar dmp_site_id="syrupad" --hivevar dmp_data_source_id="service_activity_log" --hivevar from_dt="$DMP_FROM_DT" --hivevar to_dt="$DMP_TO_DT"  < $DMP_PREPARE_HOME/import_syrupad_dmp.hql
