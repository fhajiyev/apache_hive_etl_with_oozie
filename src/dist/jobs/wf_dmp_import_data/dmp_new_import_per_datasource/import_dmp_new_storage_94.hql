
USE dmp;



set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day90before;
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

-- temp table for keeping the most recent 3 dates
CREATE TABLE IF NOT EXISTS SVC_DS_DMP.PROD_ISP_PRE
(
 DATEVAL STRING
)
;

INSERT OVERWRITE TABLE SVC_DS_DMP.PROD_ISP_PRE
SELECT MAXPART
FROM
    (
       select
       substr(part_dt,1,6),
       max(part_dt) as maxpart
       from
       svc_context_ano.itm_wired_isp
       group by substr(part_dt,1,6)
    ) a
ORDER BY MAXPART DESC LIMIT 3
;




  -- Yuseon ISP

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:daycurr}00', data_source_id='94')
  SELECT

  if(t.OS = 'adr', CONCAT('(GAID)', t.AD_ID), CONCAT('(IDFA)', t.AD_ID)) as uid,

  if(t.COMPANY_KR is null, '', t.COMPANY_KR),

  if(t.COMPANY_EN is null, '', t.COMPANY_EN),


  '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '${hivevar:daycurr}00',
  '${hivevar:daycurr}00'

  FROM
  (

    SELECT
    t1.ad_id,
    t1.os,
    t1.company_kr,
    t1.company_en

    FROM

        (
           select
           ad_id,
           os,
           company_kr,
           company_en,
           part_dt

           from
           svc_context_ano.itm_wired_isp a
           where
           a.part_dt in (select dateval from svc_ds_dmp.prod_isp_pre)

        ) t1

    INNER JOIN

        (
           select
           ad_id,
           max(part_dt) as maxdate

           from
           svc_context_ano.itm_wired_isp a
           where
           a.part_dt in (select dateval from svc_ds_dmp.prod_isp_pre)
           group by ad_id

        ) t2

    ON t1.ad_id = t2.ad_id AND t1.part_dt = t2.maxdate

  )
  t
  WHERE t.os IN ('adr','ios');


