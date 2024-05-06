
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


insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='44')


SELECT
       A.CI
    -- 회원정보
     , if(A.SYR_MBR_YN is null,'', A.SYR_MBR_YN)                      -- Syrup KFC 카드 발급 회원 여부
     , if(A.OCB_MBR_YN is null,'', A.OCB_MBR_YN)                      -- OCB TR 유실적 회원 여부
     , if(A.SYR_CARD_ISS_DT is null,'', A.SYR_CARD_ISS_DT)            -- Syrup KFC 카드 발급일자
     , if(A.OCB_TR_FST_OCC_DT is null,'', A.OCB_TR_FST_OCC_DT)        -- OCB TR 유실적 최초 발생일자
    -- User Profile : 최근기준
     , if(A.SYR_CARD_ISS_YN is null,'', A.SYR_CARD_ISS_YN)            -- Syrup KFC 전일 KFC브랜드카드 발급 여부
     , if(A.SYR_CARD_ISS_YN_M1 is null,'', A.SYR_CARD_ISS_YN_M1)      -- Syrup KFC 최근 1개월 KFC브랜드카드 발급 여부
     , if(A.OCB_TR_OCC_YN is null,'', A.OCB_TR_OCC_YN)                -- OCB TR 전일 발생여부
     , if(A.OCB_TR_OCC_YN_M1 is null,'', A.OCB_TR_OCC_YN_M1)          -- OCB TR 최근1개월 발생여부 M1
    -- User Profile
     , if(A.CUST_APPR_CNT is null,'', A.CUST_APPR_CNT)                -- 고객별 승인건수
     , if(A.CUST_APPR_CNT_M1 is null,'', A.CUST_APPR_CNT_M1)          -- 고객별 최근 1개월 승인건수
     , if(A.CUST_APPR_CNT_M3 is null,'', A.CUST_APPR_CNT_M3)          -- 고객별 최근 3개월 승인건수
     , if(A.CUST_SALE_AMT is null,'', cast(A.CUST_SALE_AMT as decimal))                  -- 고객별 매출금액
     , if(A.CUST_SALE_AMT_M1 is null,'', cast(A.CUST_SALE_AMT_M1 as decimal))            -- 고객별 최근 1개월 매출금액
     , if(A.CUST_SALE_AMT_M3 is null,'', cast(A.CUST_SALE_AMT_M3 as decimal))            -- 고객별 최근 3개월 매출금액
     , if(A.CUST_BUY_INTERVAL is null,'', A.CUST_BUY_INTERVAL)                            -- 고객별 구매주기
     , if(A.CUST_BUY_UNIT is null,'', A.CUST_BUY_UNIT)                                    -- 고객별 구매기간 (가장 최근 구매일까지의 구매기간)
    -- 마케팅 활동
     , if(A.ALCMPN_M3_ERNG_CUST_YN is null,'', A.ALCMPN_M3_ERNG_CUST_YN)                  -- 타 제휴사 최근 3개월 실적 고객 여부 (경쟁브랜드)
     , if(A.CPN_GIFTC_BUY_INTR_CUST_YN is null,'', A.CPN_GIFTC_BUY_INTR_CUST_YN)          -- E쿠폰/상품권 구매관심 고객 여부
    -- 선호 요일 1/2/3 순위
     , if(B1.DAY_RN_1 is null,'', B1.DAY_RN_1)
     , if(B1.DAY_RN_2 is null,'', B1.DAY_RN_2)
     , if(B1.DAY_RN_3 is null,'', B1.DAY_RN_3)
    -- 선호 시간대 1/2/3 순위
     , if(B1.TM_RN_1 is null,'', B1.TM_RN_1)
     , if(B1.TM_RN_2 is null,'', B1.TM_RN_2)
     , if(B1.TM_RN_3 is null,'', B1.TM_RN_3)
    -- 선호 매장코드 1/2/3 순위

     , if(B1.MCNT_RN_1 is null,'', B1.MCNT_RN_1)

     , if(D1.MCNT_NM is null,'', D1.MCNT_NM)   AS MCNT_NM1

     , if(D1.SIDO is null,'', D1.SIDO)         AS SIDO1

     , CASE
           WHEN D1.SIDO IS NULL OR TRIM(D1.SIDO)='' THEN ''
           ELSE
           CASE
               WHEN D1.SIGUNGU IS NULL OR TRIM(D1.SIGUNGU)='' THEN ''
               ELSE CONCAT(D1.SIDO, ' ', D1.SIGUNGU)
           END
       END AS SIGUNGU1

     , CASE
           WHEN D1.SIDO IS NULL OR TRIM(D1.SIDO)='' THEN ''
           ELSE
           CASE
               WHEN D1.SIGUNGU IS NULL OR TRIM(D1.SIGUNGU)='' THEN ''
               ELSE
               CASE
                   WHEN D1.ADM_DONG IS NULL OR TRIM(D1.ADM_DONG)='' THEN ''
                   ELSE CONCAT(D1.SIDO, ' ', D1.SIGUNGU, ' ', D1.ADM_DONG)
               END
           END
       END AS ADM_DONG1

     , if(B1.MCNT_RN_2 is null,'', B1.MCNT_RN_2)

     , if(D2.MCNT_NM is null,'', D2.MCNT_NM)   AS MCNT_NM2

     , if(D2.SIDO is null,'', D2.SIDO)         AS SIDO2

     , CASE
           WHEN D2.SIDO IS NULL OR TRIM(D2.SIDO)='' THEN ''
           ELSE
           CASE
               WHEN D2.SIGUNGU IS NULL OR TRIM(D2.SIGUNGU)='' THEN ''
               ELSE CONCAT(D2.SIDO, ' ', D2.SIGUNGU)
           END
       END AS SIGUNGU2

     , CASE
           WHEN D2.SIDO IS NULL OR TRIM(D2.SIDO)='' THEN ''
           ELSE
           CASE
               WHEN D2.SIGUNGU IS NULL OR TRIM(D2.SIGUNGU)='' THEN ''
               ELSE
               CASE
                   WHEN D2.ADM_DONG IS NULL OR TRIM(D2.ADM_DONG)='' THEN ''
                   ELSE CONCAT(D2.SIDO, ' ', D2.SIGUNGU, ' ', D2.ADM_DONG)
               END
           END
       END AS ADM_DONG2

     , if(B1.MCNT_RN_3 is null,'', B1.MCNT_RN_3)

     , if(D3.MCNT_NM is null,'', D3.MCNT_NM)   AS MCNT_NM3

     , if(D3.SIDO is null,'', D3.SIDO)         AS SIDO3

     , CASE
           WHEN D3.SIDO IS NULL OR TRIM(D3.SIDO)='' THEN ''
           ELSE
           CASE
               WHEN D3.SIGUNGU IS NULL OR TRIM(D3.SIGUNGU)='' THEN ''
               ELSE CONCAT(D3.SIDO, ' ', D3.SIGUNGU)
           END
       END AS SIGUNGU3

     , CASE
           WHEN D3.SIDO IS NULL OR TRIM(D3.SIDO)='' THEN ''
           ELSE
           CASE
               WHEN D3.SIGUNGU IS NULL OR TRIM(D3.SIGUNGU)='' THEN ''
               ELSE
               CASE
                   WHEN D3.ADM_DONG IS NULL OR TRIM(D3.ADM_DONG)='' THEN ''
                   ELSE CONCAT(D3.SIDO, ' ', D3.SIGUNGU, ' ', D3.ADM_DONG)
               END
           END
       END AS ADM_DONG3

    -- 두툼 반응성
     , if(C1.DUTUM_ACTV_CD is null,'', C1.DUTUM_ACTV_CD)
    -- SYR CPN 반응성
     , if(C1.SYR_CPN_ACTV_CD is null,'', C1.SYR_CPN_ACTV_CD)
    -- OCB CPN 반응성
     , if(C1.OCB_CPN_ACTV_CD is null,'', C1.OCB_CPN_ACTV_CD),

      '', '', '', '', '', '', '', '',
      '','', '', '', '', '', '', '', '', '',
      '','', '', '', '', '', '', '', '', '',
      '','', '', '', '', '', '', '', '', '',
      '','', '', '', '', '', '', '', '', '',
      '','', '', '', '', '', '', '', '', '',
     '${hivevar:day2before}00',
     '${hivevar:day2before}00'

  FROM SVC_CUSTPFDB.PDB_MP_KFC_F_INFO A              -- KFC 회원기준 관련 정보
  LEFT OUTER JOIN
  (
   SELECT CI
        , MAX(CASE WHEN PFMC_CL_CD = 'DAY' AND RN = 1 THEN PFMC_DTL_CL_CD END) AS DAY_RN_1
        , MAX(CASE WHEN PFMC_CL_CD = 'DAY' AND RN = 2 THEN PFMC_DTL_CL_CD END) AS DAY_RN_2
        , MAX(CASE WHEN PFMC_CL_CD = 'DAY' AND RN = 3 THEN PFMC_DTL_CL_CD END) AS DAY_RN_3

        , MAX(CASE WHEN PFMC_CL_CD = 'TM' AND RN = 1 THEN PFMC_DTL_CL_CD END) AS TM_RN_1
        , MAX(CASE WHEN PFMC_CL_CD = 'TM' AND RN = 2 THEN PFMC_DTL_CL_CD END) AS TM_RN_2
        , MAX(CASE WHEN PFMC_CL_CD = 'TM' AND RN = 3 THEN PFMC_DTL_CL_CD END) AS TM_RN_3

        , MAX(CASE WHEN PFMC_CL_CD = 'MCNT' AND RN = 1 THEN PFMC_DTL_CL_CD END) AS MCNT_RN_1
        , MAX(CASE WHEN PFMC_CL_CD = 'MCNT' AND RN = 2 THEN PFMC_DTL_CL_CD END) AS MCNT_RN_2
        , MAX(CASE WHEN PFMC_CL_CD = 'MCNT' AND RN = 3 THEN PFMC_DTL_CL_CD END) AS MCNT_RN_3
     FROM
     (
      SELECT A.CI                 -- CI
              , A.PFMC_CL_CD         -- 실적구분코드 : 요일(DAY), 시간(TM), 매장(MCNT)
              , A.PFMC_DTL_CL_CD     -- 실적상세구분코드 : 요일(DAY)일요일 : 0월요일 : 1화요일 : 2수요일 : 3목요일 : 4금요일 : 5토요일 : 6  // 시간(TM) 00~23 // 매장 매장코드
              , A.CUST_APPR_CNT      -- 고객별 승인건수
              , A.CUST_APPR_CNT_M1   -- 고객별 최근 1개월 승인건수
              , A.CUST_APPR_CNT_M3   -- 고객별 최근 3개월 승인건수
              , A.CUST_SALE_AMT      -- 고객별 매출금액
              , A.CUST_SALE_AMT_M1   -- 고객별 최근 1개월 매출금액
              , A.CUST_SALE_AMT_M3   -- 고객별 최근 3개월 매출금액
           , ROW_NUMBER () OVER (PARTITION BY CI, PFMC_CL_CD ORDER BY CUST_APPR_CNT_M1 DESC, CUST_APPR_CNT_M3 DESC, CUST_APPR_CNT DESC, CUST_SALE_AMT_M1 DESC, CUST_SALE_AMT_M3 DESC, CUST_SALE_AMT DESC) AS RN
        FROM SVC_CUSTPFDB.PDB_MP_KFC_H_UNIT_PFMC A   -- KFC 기간별 실적
    ) A
    GROUP BY CI
  ) B1
  ON A.CI = B1.CI
  LEFT OUTER JOIN
  (
   SELECT CI
        , MIN(CASE WHEN INTR_CL_CD = '01' THEN ACTV_PER_CD END) AS DUTUM_ACTV_CD
        , MIN(CASE WHEN INTR_CL_CD = '02' THEN ACTV_PER_CD END) AS SYR_CPN_ACTV_CD
        , MIN(CASE WHEN INTR_CL_CD = '03' THEN ACTV_PER_CD END) AS OCB_CPN_ACTV_CD
     FROM
     (
      SELECT C.CI                  -- CI
          , C.INTR_CL_CD          -- 관심구분코드 : 01 : DUTUM, 02 : SYRUP_쿠폰, 03 : OCB_쿠폰   PUSH는 미적용
          , C.MAX_ISS_DT          -- 최대발급일자
          , C.MAX_USE_DT          -- 최대사용일자
          , C.ISS_EVNT_CNT_M1     -- 1개월 발급이벤트수
          , C.ISS_EVNT_CNT_M3     -- 3개월 발급이벤트수
          , C.ISS_EVNT_CNT_M6     -- 6개월 발급이벤트수
          , C.ISS_EVNT_CNT_M12    -- 12개월 발급이벤트수
          , C.ISS_CNT_M1          -- 1개월 발급일수
          , C.ISS_CNT_M3          -- 3개월 발급일수
          , C.ISS_CNT_M6          -- 6개월 발급일수
          , C.ISS_CNT_M12         -- 12개월 발급일수
          , C.USE_EVNT_CNT_M1     -- 1개월 사용이벤트수
          , C.USE_EVNT_CNT_M3     -- 3개월 사용이벤트수
          , C.USE_EVNT_CNT_M6     -- 6개월 사용이벤트수
          , C.USE_EVNT_CNT_M12    -- 12개월 사용이벤트수
          , C.USE_CNT_M1          -- 1개월 사용일수
          , C.USE_CNT_M3          -- 3개월 사용일수
          , C.USE_CNT_M6          -- 6개월 사용일수
          , C.USE_CNT_M12         -- 12개월 사용일수
          , C.ACTV_PER_CD         -- 활동분위코드
        FROM SVC_CUSTPFDB.PDB_MP_KFC_M_INTR C   -- KFC 관심
    ) C
    GROUP BY CI
  ) C1
  ON A.CI = C1.CI
  LEFT OUTER JOIN
  (
   SELECT B.MCNT_CD, B.MCNT_NM, B.MCNT_BASIC_ADDR, B.MCNT_DTL_ADDR
        , F.ADM_DONG_LST_CD, F.SIDO, F.SIGUNGU, F.ADM_DONG, F.ADM_DONG_CD
     FROM OCB.DW_MCNT_MST B
     LEFT OUTER JOIN
     (
       SELECT ADM_DONG_LST_CD

                   ,CASE
                            WHEN SIDO is null THEN ''

                            WHEN SIDO like '서울%'        THEN REGEXP_REPLACE(SIDO,'서울특별시','서울')

                            WHEN SIDO like '인천%'        THEN REGEXP_REPLACE(SIDO,'인천광역시','인천')
                            WHEN SIDO like '울산%'        THEN REGEXP_REPLACE(SIDO,'울산광역시','울산')
                            WHEN SIDO like '부산%'        THEN REGEXP_REPLACE(SIDO,'부산광역시','부산')
                            WHEN SIDO like '대전%'        THEN REGEXP_REPLACE(SIDO,'대전광역시','대전')
                            WHEN SIDO like '대구%'        THEN REGEXP_REPLACE(SIDO,'대구광역시','대구')
                            WHEN SIDO like '광주%'        THEN REGEXP_REPLACE(SIDO,'광주광역시','광주')

                            WHEN SIDO like '세종%'        THEN REGEXP_REPLACE(SIDO,'세종특별자치시','세종')
                            WHEN SIDO like '제주%'        THEN REGEXP_REPLACE(SIDO,'제주특별자치도','제주')

                            WHEN SIDO like '강원%'        THEN REGEXP_REPLACE(SIDO,'강원도','강원')
                            WHEN SIDO like '경기%'        THEN REGEXP_REPLACE(SIDO,'경기도','경기')

                            WHEN SIDO = '전라남도'        THEN REGEXP_REPLACE(SIDO,'전라남도','전남')
                            WHEN SIDO = '전라북도'        THEN REGEXP_REPLACE(SIDO,'전라북도','전북')
                            WHEN SIDO = '충청남도'        THEN REGEXP_REPLACE(SIDO,'충청남도','충남')
                            WHEN SIDO = '충청북도'        THEN REGEXP_REPLACE(SIDO,'충청북도','충북')
                            WHEN SIDO = '경상남도'        THEN REGEXP_REPLACE(SIDO,'경상남도','경남')
                            WHEN SIDO = '경상북도'        THEN REGEXP_REPLACE(SIDO,'경상북도','경북')
                            ELSE ''
                   END AS SIDO

       , SIGUNGU, ADM_DONG, ADM_DONG_CD
         FROM svc_custpfdb.intgt_adm_dong
       -- WHERE ADM_DONG_LST_CD = '31140570'
     ) F
     ON B.HJD_SGRP_CD = F.ADM_DONG_LST_CD

  ) D1
  ON B1.MCNT_RN_1 = D1.MCNT_CD

  LEFT OUTER JOIN
  (
   SELECT B.MCNT_CD, B.MCNT_NM, B.MCNT_BASIC_ADDR, B.MCNT_DTL_ADDR
        , F.ADM_DONG_LST_CD, F.SIDO, F.SIGUNGU, F.ADM_DONG, F.ADM_DONG_CD
     FROM OCB.DW_MCNT_MST B
     LEFT OUTER JOIN
     (
       SELECT ADM_DONG_LST_CD

                   ,CASE
                                   WHEN SIDO is null THEN ''

                                   WHEN SIDO like '서울%'        THEN REGEXP_REPLACE(SIDO,'서울특별시','서울')

                                   WHEN SIDO like '인천%'        THEN REGEXP_REPLACE(SIDO,'인천광역시','인천')
                                   WHEN SIDO like '울산%'        THEN REGEXP_REPLACE(SIDO,'울산광역시','울산')
                                   WHEN SIDO like '부산%'        THEN REGEXP_REPLACE(SIDO,'부산광역시','부산')
                                   WHEN SIDO like '대전%'        THEN REGEXP_REPLACE(SIDO,'대전광역시','대전')
                                   WHEN SIDO like '대구%'        THEN REGEXP_REPLACE(SIDO,'대구광역시','대구')
                                   WHEN SIDO like '광주%'        THEN REGEXP_REPLACE(SIDO,'광주광역시','광주')

                                   WHEN SIDO like '세종%'        THEN REGEXP_REPLACE(SIDO,'세종특별자치시','세종')
                                   WHEN SIDO like '제주%'        THEN REGEXP_REPLACE(SIDO,'제주특별자치도','제주')

                                   WHEN SIDO like '강원%'        THEN REGEXP_REPLACE(SIDO,'강원도','강원')
                                   WHEN SIDO like '경기%'        THEN REGEXP_REPLACE(SIDO,'경기도','경기')

                                   WHEN SIDO = '전라남도'        THEN REGEXP_REPLACE(SIDO,'전라남도','전남')
                                   WHEN SIDO = '전라북도'        THEN REGEXP_REPLACE(SIDO,'전라북도','전북')
                                   WHEN SIDO = '충청남도'        THEN REGEXP_REPLACE(SIDO,'충청남도','충남')
                                   WHEN SIDO = '충청북도'        THEN REGEXP_REPLACE(SIDO,'충청북도','충북')
                                   WHEN SIDO = '경상남도'        THEN REGEXP_REPLACE(SIDO,'경상남도','경남')
                                   WHEN SIDO = '경상북도'        THEN REGEXP_REPLACE(SIDO,'경상북도','경북')
                                   ELSE ''
                          END AS SIDO

       , SIGUNGU, ADM_DONG, ADM_DONG_CD
         FROM svc_custpfdb.intgt_adm_dong
       -- WHERE ADM_DONG_LST_CD = '31140570'
     ) F
     ON B.HJD_SGRP_CD = F.ADM_DONG_LST_CD

  ) D2
  ON B1.MCNT_RN_2 = D2.MCNT_CD

  LEFT OUTER JOIN
  (
   SELECT B.MCNT_CD, B.MCNT_NM, B.MCNT_BASIC_ADDR, B.MCNT_DTL_ADDR
        , F.ADM_DONG_LST_CD, F.SIDO, F.SIGUNGU, F.ADM_DONG, F.ADM_DONG_CD
     FROM OCB.DW_MCNT_MST B
     LEFT OUTER JOIN
     (
       SELECT ADM_DONG_LST_CD

                   ,CASE
                                   WHEN SIDO is null THEN ''

                                   WHEN SIDO like '서울%'        THEN REGEXP_REPLACE(SIDO,'서울특별시','서울')

                                   WHEN SIDO like '인천%'        THEN REGEXP_REPLACE(SIDO,'인천광역시','인천')
                                   WHEN SIDO like '울산%'        THEN REGEXP_REPLACE(SIDO,'울산광역시','울산')
                                   WHEN SIDO like '부산%'        THEN REGEXP_REPLACE(SIDO,'부산광역시','부산')
                                   WHEN SIDO like '대전%'        THEN REGEXP_REPLACE(SIDO,'대전광역시','대전')
                                   WHEN SIDO like '대구%'        THEN REGEXP_REPLACE(SIDO,'대구광역시','대구')
                                   WHEN SIDO like '광주%'        THEN REGEXP_REPLACE(SIDO,'광주광역시','광주')

                                   WHEN SIDO like '세종%'        THEN REGEXP_REPLACE(SIDO,'세종특별자치시','세종')
                                   WHEN SIDO like '제주%'        THEN REGEXP_REPLACE(SIDO,'제주특별자치도','제주')

                                   WHEN SIDO like '강원%'        THEN REGEXP_REPLACE(SIDO,'강원도','강원')
                                   WHEN SIDO like '경기%'        THEN REGEXP_REPLACE(SIDO,'경기도','경기')

                                   WHEN SIDO = '전라남도'        THEN REGEXP_REPLACE(SIDO,'전라남도','전남')
                                   WHEN SIDO = '전라북도'        THEN REGEXP_REPLACE(SIDO,'전라북도','전북')
                                   WHEN SIDO = '충청남도'        THEN REGEXP_REPLACE(SIDO,'충청남도','충남')
                                   WHEN SIDO = '충청북도'        THEN REGEXP_REPLACE(SIDO,'충청북도','충북')
                                   WHEN SIDO = '경상남도'        THEN REGEXP_REPLACE(SIDO,'경상남도','경남')
                                   WHEN SIDO = '경상북도'        THEN REGEXP_REPLACE(SIDO,'경상북도','경북')
                                   ELSE ''
                          END AS SIDO

       , SIGUNGU, ADM_DONG, ADM_DONG_CD
         FROM svc_custpfdb.intgt_adm_dong
       -- WHERE ADM_DONG_LST_CD = '31140570'
     ) F
     ON B.HJD_SGRP_CD = F.ADM_DONG_LST_CD

  ) D3
  ON B1.MCNT_RN_3 = D3.MCNT_CD
;