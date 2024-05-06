#!/bin/sh

IMPALA_CLIENT=/app/di/qcshell-pnet/bin/qcshell

oozie_job_name=$1

$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "INVALIDATE METADATA svc_ds_dmp.svc_event_log_$oozie_job_name"
$IMPALA_CLIENT -b daas-impala -n PS02181 -p Jy_n=RPp -e "INVALIDATE METADATA tad.log_server_event";
$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "INVALIDATE METADATA svc_ds_dmp.svc_syrupad_recommend_$oozie_job_name";

