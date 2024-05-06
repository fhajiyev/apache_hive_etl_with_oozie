
USE DMP;

set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day5before;
set hivevar:day91before;
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


insert overwrite table dmp.prod_uid_life partition (part_hour)
select
    uuid,
    begin3,
    if(latest3 = '00000000', '${hivevar:daybefore}', latest3),
    crc32(uuid) % 100,
    int((unix_timestamp() - unix_timestamp(begin3, 'yyyyMMdd') + 1 * 3600 * 24) / 3600 / 24),
    int((unix_timestamp() - unix_timestamp(if(latest3 = '00000000', '${hivevar:daybefore}', latest3), 'yyyyMMdd') + 1 * 3600 * 24) / 3600 / 24),
    seg_ids,
    buy,
    pay,
    visit,
    '${hivevar:daybefore}00' as part_hour
from (

select uid as uuid, min(begin2) as begin3, max(latest2) as latest3, max(seg_ids) as seg_ids, max(buy) as buy, max(pay) as pay, max(visit) as visit
from (
    select
    uid,
    substr(part_hour, 0, 8) as begin2,
    substr(part_hour, 0, 8) as latest2,
    '' as seg_ids,
    '' as buy,
    '' as pay,
    '' as visit
    from
    dmp.prod_data_source_store lll
    join
    dmp.uid_life_srcs ooo on
    lll.data_source_id = ooo.src_id
    where (SUBSTR(part_hour,1,8) = '${hivevar:day2before}' OR SUBSTR(part_hour,1,8) = '${hivevar:daybefore}' OR SUBSTR(part_hour,1,8) = '${hivevar:daycurr}')

    union all

    select
    uid,
    begin  as begin2,
    latest as latest2,
    '' as seg_ids,
    '' as buy,
    '' as pay,
    '' as visit
    from
    dmp.prod_uid_life
    where part_hour = '${hivevar:day2before}00'

    union all

    select
    uid,
    '${hivevar:daybefore}' as begin2,
    '00000000' as latest2,
    '' as seg_ids,
    buy,
    pay,
    visit
    from
    dmp.scoring_pivot

    union all

    select
    uid,
    '${hivevar:daybefore}' as begin2,
    '00000000' as latest2,
    concat('-', concat_ws('-', seg_ids), '-') as seg_ids,
    '' as buy,
    '' as pay,
    '' as visit
    from
    SVC_DS_DMP.PROD_SEG_USER_VIEW a
    WHERE
    (uid <> '' AND INSTR(uid, ',') <= 0 AND INSTR(uid, '\;') <= 0) AND uid not in ('(DMPC)00000000-0000-0000-0000-000000000000','(GAID)00000000-0000-0000-0000-000000000000','(IDFA)00000000-0000-0000-0000-000000000000')


) cc
group by uid
) kk;

insert overwrite table dmp.prod_data_source_store partition (part_hour, data_source_id)
select uid, abgroup, begin, latest, aged_days, active_days, seg_ids, buy, pay, '' as model_id, '' as inference_value, '' as inference_level, visit,
'', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'${hivevar:daycurr}00',
'${hivevar:daycurr}00',
'${hivevar:daycurr}00',
'90' as data_source_id
from dmp.prod_uid_life
where part_hour = '${hivevar:daybefore}00'
and active_days <=90
;

insert into table dmp.prod_data_source_store partition (part_hour, data_source_id)
select uid, '' as abgroup, '' as begin, '' as latest, '' as aged_days, '' as active_days, '' as seg_ids, '' as buy, '' as pay, model_id, inference_value, inference_level, '' as visit,
'', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'${hivevar:daycurr}00',
'${hivevar:daycurr}00',
'${hivevar:daycurr}00',
'90' as data_source_id
from dmp.prod_ml_inference_data
;


alter table dmp.prod_uid_life drop partition (part_hour < '${hivevar:day5before}00');


set hive.exec.max.dynamic.partitions=1000000;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dmp.prod_data_source_store_parquet PARTITION (part_hour,data_source_id)
select * from dmp.prod_data_source_store a where (a.part_hour = '${hivevar:day2before}00' OR a.part_hour = '${hivevar:daycurr}00') and a.data_source_id NOT IN ('63','64','65','67','68','71','72','77','82','89','97','117','118','113','114','115','116','123','124','93','110','111','122','137');




