#!/bin/bash
die() {
    echo >&2 -e "\nERROR: $@\n"; exit 1;
}
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }

for i in {164..180} 
do 
#    echo $i
    run ./go.sh $i
done

