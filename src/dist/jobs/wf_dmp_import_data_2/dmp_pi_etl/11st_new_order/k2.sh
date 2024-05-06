#!/bin/bash

cd /app/dmp_pi/dmp_pi_etl/11st_new_order

./batch2.sh 346 364 &
sleep 2
./batch2.sh 326 344 &
sleep 2
./batch2.sh 315 324 &
sleep 2
./batch2.sh 306 314 &
sleep 2
./batch2.sh 301 304 &
sleep 2
./batch2.sh 277 284 &
sleep 2
./batch2.sh 258 264 &
sleep 2
./batch2.sh 233 244 &
sleep 2
./batch2.sh 177 184 &
sleep 2
./batch2.sh 137 144 &
sleep 2
./batch2.sh  76  84 &
sleep 2
./batch2.sh  16  24 &
sleep 2


