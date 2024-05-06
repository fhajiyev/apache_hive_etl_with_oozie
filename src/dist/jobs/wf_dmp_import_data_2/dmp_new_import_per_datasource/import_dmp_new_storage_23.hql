

set hivevar:day2before;
set hivevar:day5before;
set hivevar:day367before;
set hivevar:daycurr;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table dmp_pi.prod_uid_life partition (part_hour)
select
    uuid,
    begin3,
    latest3,
    crc32(uuid) % 100,
    int((unix_timestamp() - unix_timestamp(begin3, 'yyyyMMdd') + 1 * 3600 * 24) / 3600 / 24),
    int((unix_timestamp() - unix_timestamp(latest3, 'yyyyMMdd') + 1 * 3600 * 24) / 3600 / 24),
    '${hivevar:day2before}00' as part_hour
from (

select uid as uuid, min(begin2) as begin3, max(latest2) as latest3
from (
    select uid, substr(part_hour, 0, 8) as begin2, substr(part_hour, 0, 8) as latest2 from dmp_pi.prod_data_source_store lll
    join dmp_pi.uid_life_srcs ooo on lll.data_source_id = ooo.src_id
    where part_hour = '${hivevar:day2before}00'

    union all

    select uid, begin as begin2, latest as latest2 from dmp_pi.prod_uid_life
    where part_hour=from_unixtime(unix_timestamp() - 3 * 3600 *24, 'yyyyMMdd00')
) cc
group by uid
) kk;



insert overwrite table dmp_pi.prod_data_source_store partition (part_hour, data_source_id)
select uid, abgroup, begin, latest, aged_days, active_days,
'', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
part_hour,
part_hour,
part_hour,
'23' as data_source_id
from dmp_pi.prod_uid_life
where part_hour = '${hivevar:day2before}00'
and active_days <=366;


alter table dmp_pi.prod_uid_life drop partition (part_hour < '${hivevar:day5before}00');


set hive.exec.max.dynamic.partitions=10000;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dmp_pi.prod_data_source_store_parquet PARTITION (part_hour,data_source_id)
select * from dmp_pi.prod_data_source_store a where (a.part_hour = '${hivevar:day2before}00' OR a.part_hour = '${hivevar:daycurr}00');

alter table dmp_pi.prod_data_source_store drop if exists partition (part_hour < '${hivevar:day367before}00');
alter table dmp_pi.prod_data_source_store_parquet drop if exists partition (part_hour < '${hivevar:day367before}00');


