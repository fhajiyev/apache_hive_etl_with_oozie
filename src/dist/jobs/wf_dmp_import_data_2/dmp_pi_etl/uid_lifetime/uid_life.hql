set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dmp_pi.prod_uid_life partition (part_hour)
select
    uuid,
    begin3,
    latest3,
    crc32(uuid) % 100,
    int((unix_timestamp() - unix_timestamp(begin3, 'yyyyMMdd') + 1 * 3600 * 24) / 3600 / 24),
    int((unix_timestamp() - unix_timestamp(latest3, 'yyyyMMdd') + 1 * 3600 * 24) / 3600 / 24),
    from_unixtime(unix_timestamp() - 2 * 3600 * 24, 'yyyyMMdd00') as part_hour
from (

select uid as uuid, min(begin2) as begin3, max(latest2) as latest3
from (
    select uid, substr(part_hour, 0, 8) as begin2, substr(part_hour, 0, 8) as latest2 from dmp_pi.prod_data_source_store lll
    join dmp_pi.uid_life_srcs ooo on lll.data_source_id = ooo.src_id
    where part_hour = from_unixtime(unix_timestamp() - 2 * 3600 *24, 'yyyyMMdd00')

    union all

    select uid, begin as begin2, latest as latest2 from dmp_pi.prod_uid_life
    where part_hour=from_unixtime(unix_timestamp() - 3 * 3600 *24, 'yyyyMMdd00')
) cc
group by uid
) kk;


