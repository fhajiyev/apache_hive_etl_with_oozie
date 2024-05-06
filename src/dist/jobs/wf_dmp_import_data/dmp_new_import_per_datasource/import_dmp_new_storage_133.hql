USE dmp;


set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day90before;
set hivevar:day91before;



set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

-- 11st 찜/공유

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:day2before}00', data_source_id='133')

  SELECT DISTINCT

uid,
dt,
hour,
day,
action,
gender_code,
 case
                      when birthyear is null or birthyear = '' then ''
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 18 and 19 then '18-19'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 20 and 24 then '20-24'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 25 and 29 then '25-29'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 30 and 34 then '30-34'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 35 and 39 then '35-39'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 40 and 44 then '40-44'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 45 and 49 then '45-49'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 50 and 54 then '50-54'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 55 and 59 then '55-59'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 60 and 64 then '60-64'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 65 and 69 then '65-69'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 between 70 and 74 then '70-74'
                      when cast(substr('${hivevar:day2before}', 1, 4)-birthyear as int)+1 >= 75               then '75+'
                      else ''
 end AS AGE,
poc_clf,
pf_clf,
bm_clf,
device_model,
manufacturer,
carrier_name,
os_name,
os_version,
screen_width,
screen_height,
language_code,
advrt_no,
advrt_detail,
ad_nm,
alnc_cmpn_clsf_nm,
alnc_cmpn_nm,
alnc_cmpn_tp_nm,
partner_cd,
partner_nm,
partner_referrer,
'',
'',
'',
'',
'',
'',
prd_no,
prd_nm,
'',
'',
'',
'',
'',
'',
'',
'',
'',
'',
'',
disp_ctgr_nm_1,
disp_ctgr_nm_2,
disp_ctgr_nm_3,
disp_ctgr_nm_4,
cat_union,
brand_cd,
brand_nm,

'', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',
'', '', '', '', '', '', '', '', '', '',

'${hivevar:day2before}00',
base_time

FROM
(



  SELECT

      CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(IDFA)',IDFA)
         ELSE DMPC
      END as uid,

part_date as dt,
hour,
day,
'like' as action,
gender_code,
birthyear,
poc_clf,
pf_clf,
bm_clf,
device_model,
manufacturer,
carrier_name,
os_name,
os_version,
screen_width,
screen_height,
language_code,
advrt_no,
advrt_detail,
ad_nm,
alnc_cmpn_clsf_nm,
alnc_cmpn_nm,
alnc_cmpn_tp_nm,
partner_cd,
partner_nm,
partner_referrer,
prd_no,
prd_nm,
disp_ctgr_nm_1,
disp_ctgr_nm_2,
disp_ctgr_nm_3,
disp_ctgr_nm_4,
TRIM(
        CONCAT
        (
         CASE WHEN disp_ctgr_nm_1 IS NULL OR TRIM(disp_ctgr_nm_1) IN ('', 'null') THEN ''
         ELSE TRIM(disp_ctgr_nm_1)
         END,
         CASE WHEN disp_ctgr_nm_2 IS NULL OR TRIM(disp_ctgr_nm_2) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(disp_ctgr_nm_2))
         END,
         CASE WHEN disp_ctgr_nm_3 IS NULL OR TRIM(disp_ctgr_nm_3) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(disp_ctgr_nm_3))
         END,
         CASE WHEN disp_ctgr_nm_4 IS NULL OR TRIM(disp_ctgr_nm_4) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(disp_ctgr_nm_4))
         END
        )
     ) as cat_union,
brand_cd,
brand_nm,
base_time

FROM
11st_npi_skp.11st_npi_log_dmp_data_source_prdlike

WHERE part_date = '${hivevar:day2before}'
AND
  (
              (
                     DMPC IS NOT NULL
                       AND
                     SUBSTR(DMPC,1,6) = '(DMPC)'
                       AND
                     DMPC <> '(DMPC)00000000-0000-0000-0000-000000000000'
              )
              OR

                    (GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID= LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000')
              OR
                    (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000')

  )


  UNION ALL

  SELECT

      CASE
         WHEN GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID=LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(GAID)',GAID)
         WHEN IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000' THEN CONCAT('(IDFA)',IDFA)
         ELSE DMPC
      END as uid,

part_date as dt,
hour,
day,
'share' as action,
gender_code,
birthyear,
poc_clf,
pf_clf,
bm_clf,
device_model,
manufacturer,
carrier_name,
os_name,
os_version,
screen_width,
screen_height,
language_code,
advrt_no,
advrt_detail,
ad_nm,
alnc_cmpn_clsf_nm,
alnc_cmpn_nm,
alnc_cmpn_tp_nm,
partner_cd,
partner_nm,
partner_referrer,
prd_no,
prd_nm,
disp_ctgr_nm_1,
disp_ctgr_nm_2,
disp_ctgr_nm_3,
disp_ctgr_nm_4,
TRIM(
        CONCAT
        (
         CASE WHEN disp_ctgr_nm_1 IS NULL OR TRIM(disp_ctgr_nm_1) IN ('', 'null') THEN ''
         ELSE TRIM(disp_ctgr_nm_1)
         END,
         CASE WHEN disp_ctgr_nm_2 IS NULL OR TRIM(disp_ctgr_nm_2) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(disp_ctgr_nm_2))
         END,
         CASE WHEN disp_ctgr_nm_3 IS NULL OR TRIM(disp_ctgr_nm_3) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(disp_ctgr_nm_3))
         END,
         CASE WHEN disp_ctgr_nm_4 IS NULL OR TRIM(disp_ctgr_nm_4) IN ('', 'null') THEN ''
         ELSE CONCAT('\||',TRIM(disp_ctgr_nm_4))
         END
        )
     ) as cat_union,
brand_cd,
brand_nm,
base_time

FROM
11st_npi_skp.11st_npi_log_dmp_data_source_prdshare

WHERE part_date = '${hivevar:day2before}'
AND
  (
              (
                     DMPC IS NOT NULL
                       AND
                     SUBSTR(DMPC,1,6) = '(DMPC)'
                       AND
                     DMPC <> '(DMPC)00000000-0000-0000-0000-000000000000'
              )
              OR

                    (GAID IS NOT NULL AND LENGTH(GAID)=36 AND GAID= LOWER(GAID) AND GAID <> '00000000-0000-0000-0000-000000000000')
              OR
                    (IDFA IS NOT NULL AND LENGTH(IDFA)=36 AND IDFA<>LOWER(IDFA) AND IDFA <> '00000000-0000-0000-0000-000000000000')

  )


)
t
;
