#!/bin/bash

#die() {
#    echo >&2 -e "\nERROR: $@\n"; exit 1;
#}
#run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

cd /app/dmp_pi/dmp_pi_etl/sw_act

rrr=`expr $1 + 19`

for i in $(eval echo "{$1..$rrr}")
do
./go_reload.sh $i
done

