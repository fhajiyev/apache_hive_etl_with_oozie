#!/bin/sh
echo "set tez.queue.name=COMMON; 
drop table if exists dmp_pi.ocb_push;
create external table dmp_pi.ocb_push(attribute STRING)
STORED AS TEXTFILE LOCATION 'hdfs://skpds/user/dmp_pi/dmp_pi_ocb_push';
insert overwrite table dmp_pi.ocb_push
select mbr_id, tgt_push_id
from ocb.MART_APP_PUSH_RCTN_CTNT     
where base_dt     = '$1'         - 매일날짜로 업데이트  
and tgt_push_id <> ''                - 푸쉬아이디가  없는 건도 있음
and push_rcv_yn = '1';" 
