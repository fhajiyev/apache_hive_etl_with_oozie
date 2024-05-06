#!/bin/bash

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
./go_reload.sh 366 &

sleep 2
./go_reload.sh 273 &
sleep 2
./go_reload.sh 274 &
sleep 2
./go_reload.sh 275 &
sleep 2
./go_reload.sh 276 &
sleep 2
./go_reload.sh 277 &
sleep 2
./go_reload.sh 278 &
sleep 2
./go_reload.sh 279 &
sleep 2
./go_reload.sh 280 &


