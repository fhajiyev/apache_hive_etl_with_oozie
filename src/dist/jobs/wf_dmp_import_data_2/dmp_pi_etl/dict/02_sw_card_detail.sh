#!/bin/sh

HDD=/app/yarn_etl/bin/hadoop

die() { echo >&2 -e "\nERROR: $@\n"; exit 1; }
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }

print_bstr "시럽 카드 상세 정보를 가져옵니다"
run $HDD fs -getmerge hdfs://skpds/data_bis/smartwallet/raw/mt3_card /app/dmp_pi/dmp_pi_etl/sw_card_detail.dat
run ls -alh /app/dmp_pi/dmp_pi_etl/sw_card_detail.dat
print_bline

