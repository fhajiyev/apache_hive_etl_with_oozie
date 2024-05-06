set hive.execution.engine=tez;
set tez.queue.name=COMMON; 
use 11st;
drop table if exists dmp_pi.elev_os;
create external table dmp_pi.elev_os(mem_no STRING, os_str STRING, update_dt STRING)
STORED AS TEXTFILE LOCATION 'hdfs://skpds/user/dmp_pi/dmp_pi_elev_os';
insert overwrite table dmp_pi.elev_os
select distinct bb.mem_no, aa.os_nm, aa.update_dt from 11st.tb_evs_ods_m_mo_cust_device_info as aa, 11st.tb_evs_ods_m_mo_app_push_info_new as bb where aa.device_id=bb.device_id and bb.mem_no <> '' and aa.part_date='20180108' and bb.part_date='20180108';
