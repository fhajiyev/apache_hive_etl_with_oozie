
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;






select count(distinct key)


from

(


   SELECT


   BB.dateval,
   BB.timeval,
 
   CASE 
      WHEN BB.type_ch = '01' THEN m2zone2mid.maxmid
      ELSE BB.mid
   END as mid_id,
  
   BB.type_ch,
   -- BB.mdn,
   BB.key,
   if(m2zone.zone_id is null, '', m2zone.zone_id) as zone_id,  
   if(m2zone.name    is null, '', m2zone.name   ) as zone_name

   FROM 

   (
      select
     
      substr(a.part_hour, 1,8) as dateval,
      substr(a.part_hour, 9,2) as timeval,
      a.mid,
      a.key,
      -- b.mdn,

      CASE WHEN a.log_id = 'PXG0101' THEN '01' ELSE '02' END as type_ch,
  

      CASE WHEN a.log_id = 'PXG0101' THEN if(get_json_object(a.body, '$.tech_id') is null, '', get_json_object(a.body, '$.tech_id')) ELSE '' END as tech_id



      from
      proximity.log_server_newtech a
      -- join
      -- istore.prod_ble_blekey_map b
      -- on
      -- a.key = b.ble_key 

      where
      a.part_hour between '2018031400' and '2018031424'
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
      part_date = '20180314'
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
      part_date = '20180314'

      group by zone_id

   ) m2zone2mid

   on
   m2zone.zone_id = m2zone2mid.zone_id



) CC


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
     part_date = '20180314'

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
(
  (CC.type_ch = '01' and CC.zone_id is not null and CC.zone_id <> '')
  or
  (CC.type_ch = '02' and CC.mid_id  is not null and CC.mid_id  <> '')
)
and
DD.lm is not null and DD.lm = '서울'

;
