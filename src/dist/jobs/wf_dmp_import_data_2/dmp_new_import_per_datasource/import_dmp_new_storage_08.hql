
set hivevar:day2before;
set hivevar:day92before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='8')

select distinct

t.ci,
t.dateval,
t.timeval,
t.weekday,
t.type_ch,
t.loc_code,
t.loc_name,

t.lm,

CASE
   WHEN t.lm <> '' AND t.mm <> '' THEN CONCAT(t.lm, ' ', t.mm)
   ELSE ''
END,

CASE
   WHEN t.lm <> '' AND t.mm <> '' AND t.sm <> '' THEN CONCAT(t.lm, ' ', t.mm, ' ', t.sm)
   ELSE ''
END,


  '',
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



AA.ci,

CC.dateval,
CC.timeval,
CASE from_unixtime(unix_timestamp(CC.dateval, 'yyyyMMdd'), 'u')
     WHEN 1 THEN 'mon'
     WHEN 2 THEN 'tue'
     WHEN 3 THEN 'wed'
     WHEN 4 THEN 'thu'
     WHEN 5 THEN 'fri'
     WHEN 6 THEN 'sat'
     ELSE 'sun'
END as weekday,

CC.type_ch,
CASE WHEN CC.type_ch = '01' THEN CC.zone_id   ELSE CC.mid_id                                END as loc_code,
CASE WHEN CC.type_ch = '01' THEN CC.zone_name ELSE if(DD.mid_name is null, '', DD.mid_name) END as loc_name,

if(DD.lm is null, '', DD.lm) as lm,
if(DD.mm is null, '', DD.mm) as mm,
if(DD.sm is null, '', DD.sm) as sm


from
(
    select
    ci,
    mdn
    from
    dmp_pi.id_pool
    where part_date = '${hivevar:day2before}'

) AA

JOIN

(


   SELECT


   BB.dateval,
   BB.timeval,
 
   CASE 
      WHEN BB.type_ch = '01' THEN m2zone2mid.maxmid
      ELSE BB.mid
   END as mid_id,
  
   BB.type_ch,
   BB.mdn,
   if(m2zone.zone_id is null, '', m2zone.zone_id) as zone_id,  
   if(m2zone.name    is null, '', m2zone.name   ) as zone_name

   FROM 

   (
      select
     
      substr(a.part_hour, 1,8) as dateval,
      substr(a.part_hour, 9,2) as timeval,
      a.mid,
      b.mdn,

      CASE WHEN a.log_id = 'PXG0101' THEN '01' ELSE '02' END as type_ch,
  

      CASE WHEN a.log_id = 'PXG0101' THEN if(get_json_object(a.body, '$.tech_id') is null, '', get_json_object(a.body, '$.tech_id')) ELSE '' END as tech_id



      from
      proximity.log_server_newtech a
      join
      istore.prod_ble_blekey_map b
      on
      a.key = b.ble_key 

      where
      a.part_hour between '${hivevar:day2before}00' and '${hivevar:day2before}24'
      and
      a.key NOT IN ('', '\n')
      and
      a.log_id IN ('PXG0101', 'PXB0101', 'PXW0101')


   ) BB


  left join

   (

      select

      tech_id,
      zone_id,
      name

      from

      imc.dbm_imc_m2_zone
      where
      part_date = '${hivevar:day2before}'
      and
      tech_id IS NOT NULL and tech_id <> ''


   ) m2zone

   on
   BB.tech_id = m2zone.tech_id


  left join

   (

      select
  
      zone_id,
      max(mid) as maxmid

      from
      imc.dbm_imc_m2_merch_zone
      where
      part_date = '${hivevar:day2before}'

      group by zone_id

   ) m2zone2mid

   on
   m2zone.zone_id = m2zone2mid.zone_id



) CC

ON
AA.mdn = CC.mdn

left join
(

   SELECT   
  
   mid_addr.mid as mid_id,
   mid_addr.exp_name as mid_name,
   if(loc_l.dtl_cd_nm IS NULL, '', loc_l.dtl_cd_nm) as lm,
   if(loc_m.dtl_cd_nm IS NULL, '', loc_m.dtl_cd_nm) as mm,
   CASE
      WHEN loc_s1.dtl_cd_nm IS NOT NULL AND loc_s1.dtl_cd_nm <> '' THEN loc_s1.dtl_cd_nm
      WHEN loc_s2.dtl_cd_nm IS NOT NULL AND loc_s2.dtl_cd_nm <> '' THEN loc_s2.dtl_cd_nm
      WHEN loc_s3.dtl_cd_nm IS NOT NULL AND loc_s3.dtl_cd_nm <> '' THEN loc_s3.dtl_cd_nm
      WHEN loc_s4.dtl_cd_nm IS NOT NULL AND loc_s4.dtl_cd_nm <> '' THEN loc_s4.dtl_cd_nm
      WHEN loc_s5.dtl_cd_nm IS NOT NULL AND loc_s5.dtl_cd_nm <> '' THEN loc_s5.dtl_cd_nm
      ELSE ''
   END as sm

   FROM
   (
  
     SELECT

     mid,
     exp_name,
     address_code

     from
     imc.dbm_imc_imc_store_basic
     where
     part_date = '${hivevar:day2before}'

   ) mid_addr


   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_LGRP_CD'

   ) loc_l

   ON SUBSTR(mid_addr.address_code, 1, 2) = loc_l.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_MGRP_CD'

   ) loc_m

   ON SUBSTR(mid_addr.address_code, 1, 5) = loc_m.dtl_cd

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s1

   ON (
       CONCAT(mid_addr.address_code, '00')                                              = loc_s1.dtl_cd
      )

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s2

   ON (
       mid_addr.address_code                                                            = loc_s2.dtl_cd 
      )

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s3

   ON (
       CONCAT(SUBSTR(mid_addr.address_code, 1, length(mid_addr.address_code)-1), '0')   = loc_s3.dtl_cd
      )

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s4

   ON (
       CONCAT(SUBSTR(mid_addr.address_code, 1, length(mid_addr.address_code)-1), '000') = loc_s4.dtl_cd
      )

   LEFT JOIN

   (

     SELECT
     dtl_cd,
     dtl_cd_nm
     FROM
     ocb.dw_ocb_intg_cd
     WHERE
     domn_id = 'HJD_SGRP_CD'

   ) loc_s5

   ON (
       CONCAT(SUBSTR(mid_addr.address_code, 1, length(mid_addr.address_code)-3), '000') = loc_s5.dtl_cd
      )



)DD
ON
CC.mid_id = DD.mid_id

  where
  (CC.type_ch = '01' and CC.zone_id is not null and CC.zone_id <> '')
  or
  (CC.type_ch = '02' and CC.mid_id  is not null and CC.mid_id  <> '')


)t
;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day92before}00', data_source_id='8');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day92before}00', data_source_id='8');


