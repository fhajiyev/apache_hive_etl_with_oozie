#!/bin/sh

IMPALA_CLIENT=/app/di/qcshell-pnet/bin/qcshell


oozie_job_name=$1

$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "INVALIDATE METADATA svc_ds_dmp.segment";