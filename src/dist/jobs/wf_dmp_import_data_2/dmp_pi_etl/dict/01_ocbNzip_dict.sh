#!/bin/sh

HDD=/app/yarn_etl/bin/hadoop

die() { echo >&2 -e "\nERROR: $@\n"; exit 1; }
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }

LAST_JOB_DATE=`date +%Y/%m/%d -d "1 day ago"`

print_bline
print_bstr "OCB 코드를 가져옵니다"
run $HDD fs -getmerge hdfs://skpds/data_bis/ocb/DW/NXM_COD/dw_ocb_intg_cd /app/dmp_pi/dmp_pi_etl/ocb_code.dat
run ls -alh /app/dmp_pi/dmp_pi_etl/ocb_code.dat
print_bline

print_bstr "우편번호 사전을 가져옵니다"
$HDD fs -getmerge hdfs://skpds/data_db/smartwallet/raw/dbm_mt3_zip_code/$LAST_JOB_DATE/*  /app/dmp_pi/dmp_pi_etl/zip_code.dat
ls -alh  /app/dmp_pi/dmp_pi_etl/zip_code.dat
print_bline

