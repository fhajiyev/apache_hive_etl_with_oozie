set hive.execution.engine=tez;
set tez.queue.name=COMMON;

drop table if exists dmp_pi.sw_agree;

create external table dmp_pi.sw_agree(attribute STRING)
STORED AS TEXTFILE LOCATION 'hdfs://skpds/user/dmp_pi/dmp_pi_sw_agree';

insert overwrite table dmp_pi.sw_agree
Select
m.member_id
from smartwallet.mt3_member m
join smartwallet.mt3_device_list d on m.device_model = d.device_model
where m.wallet_accept is not null
and m.wallet_accept3 = 1
and m.vm_state_cd = '9'
and m.vm_ver >= '1301'
and m.os_version is not null
and m.token is not null
and m.push_server_type in ('3', '4', '5')
and m.noti_use_yn = 'Y';
