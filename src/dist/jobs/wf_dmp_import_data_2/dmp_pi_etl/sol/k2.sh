#!/bin/bash

cd /app/dmp_pi/dmp_pi_etl/sol

./batch2.sh 311 320 &
sleep 2
./batch2.sh 133 140 &
sleep 2
./batch2.sh 65 80 &
sleep 2


