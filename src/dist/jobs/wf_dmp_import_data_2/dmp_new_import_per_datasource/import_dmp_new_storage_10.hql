

set hivevar:day2before;
set hivevar:day5before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='10')
select distinct
t.ci,
t.segs,
-- t.job_kind,
-- t.hobby_kind,
-- t.hobby_freq,
-- t.personal_val1,
-- cast(t.personal_val2 as decimal),
-- cast(t.personal_val3 as decimal),
-- t.personal_val4,

'','', '','', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'','', '', '', '', '', '', '', '', '',
'${hivevar:day2before}00',
'${hivevar:day2before}00'
from
(

select
ci       as ci,
segs     as segs,
''       as job_kind,
''       as hobby_kind,
''       as hobby_freq,
''       as personal_val1,
''       as personal_val2,
''       as personal_val3,
''       as personal_val4

from

(

 select ci, '01' as segs
 from svc_custpfdb.pdb_mp_com_f_info
 where marry_yn = 'Y'
 union all
 select ci, '04' as segs
 from svc_custpfdb.pdb_mp_com_f_info
 where oph_yn = 'Y'
 union all
 select ci, '02' as segs
 from svc_custpfdb.pdb_mp_com_f_info
 where baby_yn = 'Y'
 union all
 select ci, '03' as segs
 from svc_custpfdb.pdb_mp_com_f_info
 where kids_yn = 'Y'

) a


-- (
--   select mbr_id, '01' as segs
--   from svc_cdpf.uspf_lfstg_evs_buy
--   where dt ='${hivevar:day2before}'  and marry_yn = 'Y'
--   union all
--   select mbr_id, '04' as segs
--   from svc_cdpf.uspf_lfstg_evs_buy
--   where dt ='${hivevar:day2before}'  and oph_yn = 'Y'
--   union all
--   select mbr_id, '02' as segs
--   from svc_cdpf.uspf_lfstg_evs_buy
--   where dt ='${hivevar:day2before}'  and baby_yn = 'Y'
--   union all
--   select mbr_id, '03' as segs
--   from svc_cdpf.uspf_lfstg_evs_buy
--   where dt ='${hivevar:day2before}'  and kids_yn = 'Y'
-- ) a
-- join (
--         select
--         ci,
--         elev_id
--         from
--         dmp_pi.id_pool
--         where part_date = '${hivevar:day2before}'
--      ) b
-- on (a.mbr_id = b.elev_id)

-- union all

-- select

-- ci       as ci,
-- ''       as segs,
-- t1.kind     as job_kind,
-- ''       as hobby_kind,
-- ''       as hobby_freq,
-- ''       as personal_val1,
-- ''       as personal_val2,
-- ''       as personal_val3,
-- ''       as personal_val4

-- from

-- svc_cim.prod_behavior_job t1

--   INNER JOIN
--   (
--    SELECT
--    KIND,
--    MAX(PART_DT) as maxdate
--    FROM
--    svc_cim.PROD_BEHAVIOR_JOB
--    GROUP BY KIND
--   ) b

--   ON t1.kind = b.kind AND t1.part_dt = b.maxdate

-- union all

-- select

-- ci       as ci,
-- ''       as segs,
-- ''       as job_kind,
-- t2.kind     as hobby_kind,
-- freq     as hobby_freq,
-- ''       as personal_val1,
-- ''       as personal_val2,
-- ''       as personal_val3,
-- ''       as personal_val4

-- from

-- svc_cim.prod_behavior_hobby t2

--   INNER JOIN
--   (
--    SELECT
--    KIND,
--    MAX(PART_DT) as maxdate
--    FROM
--    svc_cim.PROD_BEHAVIOR_HOBBY
--    GROUP BY KIND
--   ) b

--   ON t2.kind = b.kind AND t2.part_dt = b.maxdate

-- union all

-- select

-- ci       as ci,
-- ''       as segs,
-- ''       as job_kind,
-- ''       as hobby_kind,
-- ''       as hobby_freq,
-- if(t3.kind = 'commuter_yn' AND value = 'Y', 'commuter', '')       as personal_val1,
-- if(t3.kind = 'annual_income',                 value,       '')       as personal_val2,
-- if(t3.kind = 'estimated_house_price',        value,       '')       as personal_val3,
-- if(t3.kind = 'moved_person',                   value,       '')       as personal_val4

-- from

-- svc_cim.prod_demographic_personal t3

--   INNER JOIN
--   (
--    SELECT
--    KIND,
--    MAX(PART_DT) as maxdate
--    FROM
--    svc_cim.PROD_DEMOGRAPHIC_PERSONAL
--    GROUP BY KIND
--   ) b

--   ON t3.kind = b.kind AND t3.part_dt = b.maxdate

-- where
-- (
--  (t3.kind = 'commuter_yn' AND value = 'Y')
--  OR
--  (t3.kind = 'annual_income')
--  OR
--  (t3.kind = 'estimated_house_price')
--  OR
--  (t3.kind = 'moved_person')
-- )


) t;




