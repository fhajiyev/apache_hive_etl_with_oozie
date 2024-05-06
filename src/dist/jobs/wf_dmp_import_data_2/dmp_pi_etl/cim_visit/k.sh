#!/bin/bash

cd /app/dmp_pi/dmp_pi_etl/cim_visit

./batch.sh 1 &
sleep 2
./batch.sh 21 &
sleep 2
./batch.sh 41 &
sleep 2
./batch.sh 61 &
sleep 2
./go_reload.sh 81 &
sleep 2
./go_reload.sh 82 &
sleep 2
./go_reload.sh 83 &
sleep 2
./go_reload.sh 84 &
sleep 2
./go_reload.sh 85 &
sleep 2
./go_reload.sh 86 &
sleep 2
./go_reload.sh 87 &
sleep 2
./go_reload.sh 88 &
sleep 2
./go_reload.sh 89 &
sleep 2
./go_reload.sh 90 &
sleep 2



