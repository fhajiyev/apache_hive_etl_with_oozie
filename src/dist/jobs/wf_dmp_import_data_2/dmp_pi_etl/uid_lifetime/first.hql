set hive.execution.engine=tez;
set tez.queue.name=COMMON;
insert overwrite table dmp_pi.prod_uid_life partition (part_hour='2018020900')
select
    uuid,
    begin2,
    latest2,
    crc32(uuid) % 100,
    '',
    ''
from (
select uid as uuid, substr(min(part_hour), 0, 8) as begin2, substr(max(part_hour), 0, 8) as latest2
from (
    select uid, part_hour from dmp_pi.prod_data_source_store lll
    join dmp_pi.uid_life_srcs ooo on lll.data_source_id = ooo.src_id
    where part_hour>2017021800 and part_hour<=2018021800 
) cc
group by uid
) kk;

