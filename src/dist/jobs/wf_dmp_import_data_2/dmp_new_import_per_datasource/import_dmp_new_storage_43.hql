set hivevar:day2before;
set hivevar:day5before;
set hivevar:day367before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='43')


SELECT CI  -- CI번호
, if(ALL_ORD_CNT        is null, '', ALL_ORD_CNT)         -- 리바이스 및 경쟁사 주문수
, if(LEVIS_ORD_CNT      is null, '', LEVIS_ORD_CNT)       -- 리바이스 주문수
, if(CALVIN_ORD_CNT     is null, '', CALVIN_ORD_CNT)      -- 캘빈클라인 주문수
, if(GUESS_ORD_CNT      is null, '', GUESS_ORD_CNT)       -- 게스 주문수
, if(BUCKAROO_ORD_CNT   is null, '', BUCKAROO_ORD_CNT)    -- 버커루 주문수
, if(BANGBANG_ORD_CNT   is null, '', BANGBANG_ORD_CNT)    -- 뱅뱅 주문수
, if(ALL_JEAN_ORD_CNT   is null, '', ALL_JEAN_ORD_CNT)    -- 청바지 주문수
, if(LEVIS_JEAN_ORD_CNT is null, '', LEVIS_JEAN_ORD_CNT)  -- 리바이스 청바지 주문수

,'', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
 '', '', '', '', '', '', '', '', '', '',
'${hivevar:day2before}00',
'${hivevar:day2before}00'


FROM SVC_CUSTPFDB.PDB_MP_LVS_F_INFO
;

