#!/bin/bash

cd /app/dmp_pi/dmp_pi_etl/11st_basket

./batch2.sh 352 369 &
sleep 2
./batch2.sh 337 351 &
sleep 2
./batch2.sh 321 336 &
sleep 2
./batch2.sh 306 320 &
sleep 2
./batch2.sh 291 305 &
sleep 2
./batch2.sh 276 290 &
sleep 2
./batch2.sh 260 275 &
sleep 2

./go_reload.sh 259
sleep 2
./go_reload.sh 258
sleep 2
./go_reload.sh 213
sleep 2
./go_reload.sh 163
sleep 2
./go_reload.sh 161
sleep 2
./go_reload.sh 151
sleep 2
./go_reload.sh 134
sleep 2
./go_reload.sh 133
sleep 2
./go_reload.sh 103
sleep 2

