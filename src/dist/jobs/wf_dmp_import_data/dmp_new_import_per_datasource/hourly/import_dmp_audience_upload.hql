USE dmp;

set hivevar:hourbefore;
set hivevar:hourcurrent;
set hivevar:hour91before;


set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

set hive.exec.dynamic.partition.mode=true;

  INSERT INTO TABLE dmp.prod_data_source_store PARTITION (part_hour, data_source_id)
  SELECT
  uid,
  case when columnindex = 1 then value else '' end, case when columnindex = 2 then value else '' end, case when columnindex = 3 then value else '' end, case when columnindex = 4 then value else '' end, case when columnindex = 5 then value else '' end, case when columnindex = 6 then value else '' end, case when columnindex = 7 then value else '' end, case when columnindex = 8 then value else '' end, case when columnindex = 9 then value else '' end, case when columnindex = 10 then value else '' end, case when columnindex = 11 then value else '' end, case when columnindex = 12 then value else '' end, case when columnindex = 13 then value else '' end, case when columnindex = 14 then value else '' end, case when columnindex = 15 then value else '' end, case when columnindex = 16 then value else '' end, case when columnindex = 17 then value else '' end, case when columnindex = 18 then value else '' end, case when columnindex = 19 then value else '' end, case when columnindex = 20 then value else '' end, case when columnindex = 21 then value else '' end, case when columnindex = 22 then value else '' end, case when columnindex = 23 then value else '' end, case when columnindex = 24 then value else '' end, case when columnindex = 25 then value else '' end, case when columnindex = 26 then value else '' end, case when columnindex = 27 then value else '' end, case when columnindex = 28 then value else '' end, case when columnindex = 29 then value else '' end, case when columnindex = 30 then value else '' end, case when columnindex = 31 then value else '' end, case when columnindex = 32 then value else '' end, case when columnindex = 33 then value else '' end, case when columnindex = 34 then value else '' end, case when columnindex = 35 then value else '' end, case when columnindex = 36 then value else '' end, case when columnindex = 37 then value else '' end, case when columnindex = 38 then value else '' end, case when columnindex = 39 then value else '' end, case when columnindex = 40 then value else '' end, case when columnindex = 41 then value else '' end, case when columnindex = 42 then value else '' end, case when columnindex = 43 then value else '' end, case when columnindex = 44 then value else '' end, case when columnindex = 45 then value else '' end, case when columnindex = 46 then value else '' end, case when columnindex = 47 then value else '' end, case when columnindex = 48 then value else '' end, case when columnindex = 49 then value else '' end, case when columnindex = 50 then value else '' end, case when columnindex = 51 then value else '' end, case when columnindex = 52 then value else '' end, case when columnindex = 53 then value else '' end, case when columnindex = 54 then value else '' end, case when columnindex = 55 then value else '' end, case when columnindex = 56 then value else '' end, case when columnindex = 57 then value else '' end, case when columnindex = 58 then value else '' end, case when columnindex = 59 then value else '' end, case when columnindex = 60 then value else '' end, case when columnindex = 61 then value else '' end, case when columnindex = 62 then value else '' end, case when columnindex = 63 then value else '' end, case when columnindex = 64 then value else '' end, case when columnindex = 65 then value else '' end, case when columnindex = 66 then value else '' end, case when columnindex = 67 then value else '' end, case when columnindex = 68 then value else '' end, case when columnindex = 69 then value else '' end, case when columnindex = 70 then value else '' end, case when columnindex = 71 then value else '' end, case when columnindex = 72 then value else '' end, case when columnindex = 73 then value else '' end, case when columnindex = 74 then value else '' end, case when columnindex = 75 then value else '' end, case when columnindex = 76 then value else '' end, case when columnindex = 77 then value else '' end, case when columnindex = 78 then value else '' end, case when columnindex = 79 then value else '' end, case when columnindex = 80 then value else '' end, case when columnindex = 81 then value else '' end, case when columnindex = 82 then value else '' end, case when columnindex = 83 then value else '' end, case when columnindex = 84 then value else '' end, case when columnindex = 85 then value else '' end, case when columnindex = 86 then value else '' end, case when columnindex = 87 then value else '' end, case when columnindex = 88 then value else '' end, case when columnindex = 89 then value else '' end, case when columnindex = 90 then value else '' end, case when columnindex = 91 then value else '' end, case when columnindex = 92 then value else '' end, case when columnindex = 93 then value else '' end, case when columnindex = 94 then value else '' end, case when columnindex = 95 then value else '' end, case when columnindex = 96 then value else '' end, case when columnindex = 97 then value else '' end, case when columnindex = 98 then value else '' end, case when columnindex = 99 then value else '' end, case when columnindex = 100 then value else '' end,
  '${hivevar:hourbefore}',
  '${hivevar:hourbefore}',
  '${hivevar:hourbefore}' as part_hour, datasourceid as data_source_id
  FROM svc_ds_dmp.prod_audience_upload
  WHERE parthour = '${hivevar:hourbefore}'

  AND
  INSTR(uid, ',') <= 0 AND INSTR(uid, '\;') <= 0 AND INSTR(uid, '\t') <= 0 AND INSTR(uid, ' ') <= 0
  AND
  (
                  (
                         uid IS NOT NULL
                           AND
                         SUBSTR(uid,1,6) = '(DMPC)'
                           AND
                         uid <> '(DMPC)00000000-0000-0000-0000-000000000000'
                  )
                  OR
                  (
                        (uid IS NOT NULL AND LENGTH(uid)=42 AND SUBSTR(uid,1,6) = '(GAID)' AND SUBSTR(uid, 7)= LOWER(SUBSTR(uid, 7)) AND uid <> '(GAID)00000000-0000-0000-0000-000000000000')
                           OR
                        (uid IS NOT NULL AND LENGTH(uid)=42 AND SUBSTR(uid,1,6) = '(IDFA)' AND SUBSTR(uid, 7)<>LOWER(SUBSTR(uid, 7)) AND uid <> '(IDFA)00000000-0000-0000-0000-000000000000')
                  )
  )
;




