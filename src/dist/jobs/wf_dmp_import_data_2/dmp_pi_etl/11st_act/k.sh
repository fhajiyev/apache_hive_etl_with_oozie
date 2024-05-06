#!/bin/bash

cd /app/dmp_pi/dmp_pi_etl/11st_act

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



