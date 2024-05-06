#!/bin/bash

cd /app/dmp_pi/dmp_pi_etl/11st_new_order

./batch.sh 1 &
sleep 2
./batch.sh 21 &
sleep 2
./batch.sh 41 &
sleep 2
./batch.sh 61 &
sleep 2
./batch.sh 81 &
sleep 2
./batch.sh 101 &
sleep 2
./batch.sh 121 &
sleep 2
./batch.sh 141 &
sleep 2
./batch.sh 161 &
sleep 2
./batch.sh 181 &
sleep 2
./batch.sh 201 &
sleep 2
./batch.sh 221 &
sleep 2
./batch.sh 241 &
sleep 2
./batch.sh 261 &
sleep 2
./batch.sh 281 &
sleep 2
./batch.sh 301 &
sleep 2
./batch.sh 321 &
sleep 2
./batch.sh 341 &
sleep 2
./go_reload.sh 361 &
sleep 2
./go_reload.sh 362 &
sleep 2
./go_reload.sh 363 &
sleep 2
./go_reload.sh 364 &
sleep 2
./go_reload.sh 365 &
sleep 2



