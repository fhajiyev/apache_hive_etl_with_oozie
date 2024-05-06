#!/bin/sh

HDD=/app/yarn_etl/bin/hadoop

die() { echo >&2 -e "\nERROR: $@\n"; exit 1; }
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl
WORKDIR=$DMP_PI_HDFS_HOME/ocb_info
HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"

LAST_JOB_DATE=`date +%Y/%m/%d -d "2 day ago"`

print_bline
print_bstr 'mid -> taid 사전 가져오기 : mid - taid'
run $HDD fs -cat hdfs://skpds/data_db/imc/raw/m2_merch_alliance/$LAST_JOB_DATE/* | awk -F'\001' '{print $1"\t"$2}' > $DMP_PI_LOCAL_HOME/mid_taid.dat
run ls -alh $DMP_PI_LOCAL_HOME/mid_taid.dat
print_bline

print_bstr 'taid 사전 가져오기 :taid0, 제휴사 이름2, 카테고리 번호4'
run $HDD fs -getmerge hdfs://skpds/data_db/imc/raw/m2_alliance_basic/$LAST_JOB_DATE/* $DMP_PI_LOCAL_HOME/taid_dic.dat
run ls -alh $DMP_PI_LOCAL_HOME/taid_dic.dat
print_bline

print_bline
print_bstr "마케팅 코드 - 서비스 솔루션 ID 가져오기"
run $HDD fs -cat hdfs://skpds/data_db/ipc/tsv/prd_mkt_set-ods/$LAST_JOB_DATE/* | awk -F'\t' '{ if ($2!="" && $3!="") print $2"\t"$3}' > $DMP_PI_LOCAL_HOME/sol_mkt_svcsol.dat
run ls -alh $DMP_PI_LOCAL_HOME/sol_mkt_svcsol.dat

print_bline
print_bstr "서비스 솔루션 ID - 마케팅 이름 가져오기"
# 3 - 서비스 솔루션 ID
# 2 - 쿠폰 이름
run $HDD fs -cat hdfs://skpds/data_db/intgcpn/raw/sp_coupon/$LAST_JOB_DATE/* | awk -F'\001' '{ if ($3 !="" && $2 != "") print $3"\t"$2 }' > $DMP_PI_LOCAL_HOME/sol_svcsol_mktname.dat
run ls -alh $DMP_PI_LOCAL_HOME/sol_svcsol_mktname.dat

run cp $DMP_PI_LOCAL_HOME/sol_svcsol_mktname.dat .
run cp $DMP_PI_LOCAL_HOME/sol_mkt_svcsol.dat .

print_bstr "마케팅 코드 - 마케팅 이름 사전 만들기"
run ./gen_mkt_name.py > $DMP_PI_LOCAL_HOME/mkt_name.dat

print_bline
print_bstr '피카소 이벤트 사전 가져오기 : 마케팅코드 - 마케팅이름'
# 19 = 마케팅 ID
# 23 = 솔루션 ID
# 10 = 마케팅 이름
run $HDD fs -cat hdfs://skpds/data_db/picaso/raw/dbm_e_brand_service/$LAST_JOB_DATE/* | awk -F'\001' '{ if ($19 != "" && $23 == "0107" && $10 != "") print $19"\t"$10 }' > $DMP_PI_LOCAL_HOME/picaso_mkt.dat
run ls -alh $DMP_PI_LOCAL_HOME/picaso_mkt.dat
print_bline

run cat $DMP_PI_LOCAL_HOME/picaso_mkt.dat >> $DMP_PI_LOCAL_HOME/mkt_name.dat

print_bstr "사전 생성 : SID(OCB 가맹점코드) -> OCB 가맹점 명"
run $HDD fs -cat hdfs://skpds/data_db/imc/raw/m2_merch_source/$LAST_JOB_DATE/* | awk -F'\001' '{ if ($3 != "" && $39 != "") print $3"\t"$39 }' > $DMP_PI_LOCAL_HOME/sid2ocb_merch_name.dat
run ls -alh $DMP_PI_LOCAL_HOME/sid2ocb_merch_name.dat

print_bline
print_bstr "사전 생성 : mid(통합 가맹점 코드) -> 통합 가맹점 명"
run $HDD fs -cat hdfs://skpds/data_db/imc/raw/m2_merch_basic/$LAST_JOB_DATE/* | awk -F'\001' '{ if ($1 != "" && $52 != "") print $1"\t"$52 }' > $DMP_PI_LOCAL_HOME/mid2merch_name.dat
run ls -alh $DMP_PI_LOCAL_HOME/mid2merch_name.dat
print_bline

print_bstr "제휴사 업종 코드 - 업종 이름 가져오기"
run $HDD fs -cat hdfs://skpds/data_db/imc/raw/m2_cate_is/$LAST_JOB_DATE/* | awk -F'\001' '{ if ($1!="") print $1"\t"$2 }' > $DMP_PI_LOCAL_HOME/sol_cate.dat
#run ls -alh $DMP_PI_LOCAL_HOME/sol_cate.dat

