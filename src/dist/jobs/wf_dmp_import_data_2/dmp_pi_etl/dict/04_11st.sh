#!/bin/sh

HDD=/app/yarn_etl/bin/hadoop

die() { echo >&2 -e "\nERROR: $@\n"; exit 1; }
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }

LAST_JOB_DATE=`date +%Y/%m/%d -d "2 day ago"`

#print_bline
#print_bstr "11st 브랜드 코드 사전 가져오기"
#$HDD fs -getmerge hdfs://skpds/data_db/11st/raw/evs_pd_brand_mng_dic/$LAST_JOB_DATE/* /app/dmp_pi/dmp_pi_etl/11st_brand_dic.dat
#ls -alh /app/dmp_pi/dmp_pi_etl/11st_brand_dic.dat

# tb_evs_ods_m_dd_disp_ctgr

print_bline
print_bstr "11st 전시 카테고리 사전 가져오기"
$HDD fs -getmerge hdfs://skpds/data_bis/11st/raw/evs_dd_disp_ctgr/$LAST_JOB_DATE/* /app/dmp_pi/dmp_pi_etl/11st_disp_ct_dic.dat
#$HDD fs -getmerge hdfs://skpds/user/hive/warehouse/11st.db/tb_evs_ods_m_dd_disp_ctgr/$LAST_JOB_DATE/* /app/dmp_pi/dmp_pi_etl/11st_disp_ct_dic.dat
ls -alh /app/dmp_pi/dmp_pi_etl/11st_disp_ct_dic.dat


