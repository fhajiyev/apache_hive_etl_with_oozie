

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


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='41')

SELECT

   D.CI

, if(A.CPN_CD         is null, '', A.CPN_CD)
, if(A.CPN_NM         is null, '', A.CPN_NM)
, if(A.CPN_STS_CD     is null, '', A.CPN_STS_CD)
, if(A.ISSU_DT        is null, '', A.ISSU_DT)
, if(A.CPN_STS_UPD_DT is null, '', A.CPN_STS_UPD_DT)

, if(C.TAID is null, '', C.TAID)
, if(C.ALNCE_NM is null, '', C.ALNCE_NM)
, if(C.LCL_CTG_NM is null, '', C.LCL_CTG_NM)
, if(C.MCL_CTG_NM is null, '', C.MCL_CTG_NM)
, if(C.SCL_CTG_NM is null, '', C.SCL_CTG_NM)

,TRIM
 (
 CONCAT
 (
     CASE WHEN C.LCL_CTG_NM IS NULL OR TRIM(C.LCL_CTG_NM) IN ('') THEN ''
     ELSE TRIM(C.LCL_CTG_NM)
     END,
     CASE WHEN C.MCL_CTG_NM IS NULL OR TRIM(C.MCL_CTG_NM) IN ('') THEN ''
     ELSE CONCAT('\||',TRIM(C.MCL_CTG_NM))
     END,
     CASE WHEN C.SCL_CTG_NM IS NULL OR TRIM(C.SCL_CTG_NM) IN ('') THEN ''
     ELSE CONCAT('\||',TRIM(C.SCL_CTG_NM))
     END
 )
 ),

 '', '', '', '', '', '', '', '', '',
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

FROM

(

   SELECT
   ISSU_NO,
   SVC_ID AS MBR_ID
   FROM
   INTGCPN.DBM_INTGCPN_INTG_CPN_ISSU_SVC
   WHERE
   PART_DATE = '${hivevar:day2before}'
   AND SVC_FG_CD IN ('01', '03')
   GROUP BY ISSU_NO, SVC_ID

)
B

INNER JOIN
(
   SELECT
   CI,
   SW_ID as MBR_ID
   FROM
   DMP_PI.ID_POOL
   WHERE PART_DATE = '${hivevar:day2before}'

   UNION ALL

   SELECT
   CI,
   OCB_ID as MBR_ID
   FROM
   DMP_PI.ID_POOL
   WHERE PART_DATE = '${hivevar:day2before}'
)
D
ON B.MBR_ID = D.MBR_ID

INNER JOIN

(

   SELECT
   ISSU_NO,
   CPN_CD,
   CPN_NM,
   CPN_STS_CD,
   SUBSTR(ISSU_DT, 1, 8) AS ISSU_DT,
   CASE
      WHEN CPN_STS_CD='01' THEN SUBSTR(CPN_STS_UPD_DT, 1, 8)
      ELSE ''
   END AS CPN_STS_UPD_DT

   FROM
   INTGCPN.DBM_INTGCPN_INTG_CPN_ISSU
   WHERE
   PART_DATE = '${hivevar:day2before}'
   AND CPN_STS_CD IN ('01', '02')
   AND SUBSTR(ISSU_DT, 1, 8) BETWEEN '${hivevar:day367before}' AND '${hivevar:day2before}'

)
A

ON B.ISSU_NO = A.ISSU_NO

LEFT JOIN
(
            SELECT A.*, B.LCL_CTG_NM, B.MCL_CTG_NM, B.SCL_CTG_NM
            FROM
            (
                      SELECT
                       A.BENEFIT_CD
                     , MAX(B.TAID) AS TAID
                     , MAX(B.ALLIANCE_NM) AS ALNCE_NM
                     , MAX(B.CATE) AS SCL_CTG_CD
                    FROM

                    (
                          SELECT
                          A_ID,
                          BENEFIT_CD
                          FROM
                          INTGCPN.DBM_INTGCPN_TBL_IS_BENEFIT_USEPLACE
                          WHERE PART_DATE = '${hivevar:day2before}'
                    )
                    A

                    INNER JOIN
                    (
                          select
                          taid,
                          alliance_nm,
                          cate
                          from IMC.dbm_imc_m2_alliance_basic_v
                    )
                    B
                    ON A.A_ID = B.TAID
                    GROUP BY A.BENEFIT_CD
            ) A

            LEFT OUTER JOIN

            (
                    select
                    wb.cate_name LCL_CTG_NM,
                    wc.cate_name MCL_CTG_NM,
                    wa.cate_name SCL_CTG_NM,
                    wa.cate_is_cd SCL_CTG_CD

                    from IMC.dbm_imc_m2_cate_is_v wa
                    join IMC.dbm_imc_m2_cate_is_v wb
                    on substr(wa.cate_is_cd,1,2) = wb.cate_is_cd
                    join IMC.dbm_imc_m2_cate_is_v wc
                    on substr(wa.cate_is_cd,1,4) = wc.cate_is_cd
            ) B
            ON A.SCL_CTG_CD = B.SCL_CTG_CD
)
C
ON A.CPN_CD = C.BENEFIT_CD


;

