





set hivevar:day2before;
set hivevar:day182before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='2')

select
   AA.ci                          as uid
 , BB.dt                          as dateval
 , ''                             as timeval 
 , CASE from_unixtime(unix_timestamp(BB.dt, 'yyyyMMdd'), 'u')
     WHEN 1 THEN 'mon'
     WHEN 2 THEN 'tue'
     WHEN 3 THEN 'wed'
     WHEN 4 THEN 'thu'
     WHEN 5 THEN 'fri'
     WHEN 6 THEN 'sat'
     ELSE 'sun'
   END as weekday
 , BB.action
 , BB.word,

   '','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '','','','','','','','','','',
   '${hivevar:day2before}00',
   '${hivevar:day2before}00'


 from
 (

    select
    ci,
    elev_id
    from
    dmp_pi.id_pool
    where part_date = '${hivevar:day2before}'

 ) AA

join

 (

    select
    mbr_id,
    word,
    dt,
    'search' as action
 
    from
    svc_cdpf.intgt_srch_keyword_mbr   
    where
    dt = '${hivevar:day2before}'
    and mbr_id not in ('-1', '', '\n', '0')
    and word not in ('', '\n')

 ) BB

 ON AA.elev_id = BB.mbr_id

;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day182before}00', data_source_id='2');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day182before}00', data_source_id='2');
