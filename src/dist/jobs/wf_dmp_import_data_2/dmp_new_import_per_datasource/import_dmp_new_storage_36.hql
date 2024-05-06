

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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='36')

SELECT DISTINCT
ci,
'',
'',
'',
if(nm1  is null, '', nm1),
if(nm2  is null, '', nm2),
if(nm3  is null, '', nm3),
if(bscr is null, '', cast(bscr as decimal)),
if(cscr is null, '', cast(cscr as decimal)),

'','',
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

FROM

(

      SELECT
      ci,
      disp_ctgr_nm_1 as nm1,
      disp_ctgr_nm_2 as nm2,
      disp_ctgr_nm_3 as nm3,
      buy_scr        as bscr,
      ''            as cscr

      FROM
      svc_custpfdb.pdb_mp_com_h_buyer_interest

      UNION ALL

      SELECT
      ci,
      disp_ctgr_nm_1 as nm1,
      disp_ctgr_nm_2 as nm2,
      disp_ctgr_nm_3 as nm3,
      ''            as bscr,
      clk_scr        as cscr

      FROM
      svc_custpfdb.pdb_mp_com_h_click_interest

)
t
;


