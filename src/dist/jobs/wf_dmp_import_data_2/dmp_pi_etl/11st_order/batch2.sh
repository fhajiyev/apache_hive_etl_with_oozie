#!/bin/bash
die() {
    echo >&2 -e "\nERROR: $@\n"; exit 1;
}
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

rrr=`expr $1 + 20`

for i in $(eval echo "{$1..$rrr}")
do 
#    echo $i
    run ./go.sh $i
done

