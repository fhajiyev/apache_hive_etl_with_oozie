

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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='38')

SELECT       A0.CI
            , if(C1.SALE_CNT03M is null, '', C1.SALE_CNT03M)
            , if(C1.SALE_AMT03M is null, '', cast(C1.SALE_AMT03M AS decimal))
            , if(C1.SALE_QTY03M is null, '', cast(C1.SALE_QTY03M AS decimal))
            , if(C1.DNT_MTH03M is null, '', C1.DNT_MTH03M)
            , if(C1.DNT_DT03M is null, '', C1.DNT_DT03M)
            , if(C1.DNT_MID03M is null, '', C1.DNT_MID03M)
            , if(C1.RSVNG_PNT03M is null, '', cast(C1.RSVNG_PNT03M as decimal))
            , if(C1.USE_PNT03M is null, '', cast(C1.USE_PNT03M as decimal))
            , if(C2.SALE_CNT01M is null, '', C2.SALE_CNT01M)
            , if(C2.SALE_AMT01M is null, '', cast(C2.SALE_AMT01M as decimal))
            , if(C2.SALE_QTY01M is null, '', cast(C2.SALE_QTY01M as decimal))
            , if(C2.DNT_MTH01M is null, '', C2.DNT_MTH01M)
            , if(C2.DNT_DT01M is null, '', C2.DNT_DT01M)
            , if(C2.DNT_MID01M is null, '', C2.DNT_MID01M)
            , if(C2.RSVNG_PNT01M is null, '', cast(C2.RSVNG_PNT01M as decimal))
            , if(C2.USE_PNT01M is null, '', cast(C2.USE_PNT01M as decimal))
            , if(A2.LTV_AVG_SALE_CNT is null, '', cast(A2.LTV_AVG_SALE_CNT as decimal))
            , if(A2.LTV_AVG_SALE_AMT is null, '', cast(A2.LTV_AVG_SALE_AMT as decimal))
            , if(A2.LTV_AVG_SALE_QTY is null, '', cast(A2.LTV_AVG_SALE_QTY as decimal))
            , if(A2.LTV_AVG_PROFIT is null, '', cast(A2.LTV_AVG_PROFIT as decimal))
            , if(A2.PATTERN is null, '', A2.PATTERN)
            , if(A2.AVG_INTERVAL is null, '', cast(A2.AVG_INTERVAL as decimal))
            , if(A2.MIN_INTERVAL is null, '', cast(A2.MIN_INTERVAL as decimal))
            , if(A2.MAX_INTERVAL is null, '', cast(A2.MAX_INTERVAL as decimal))
            , if(A2.DIFF_LATEST is null, '', A2.DIFF_LATEST)
            , if(B.MAX_SALE_QTY is null, '', cast(B.MAX_SALE_QTY as decimal))
            , if(A2.PCT_SALE_CNT_10TS is null, '', cast(A2.PCT_SALE_CNT_10TS as decimal))
            , if(A2.PCT_SALE_CNT_CASH is null, '', cast(A2.PCT_SALE_CNT_CASH as decimal))
            , if(A2.PCT_M1 is null, '', cast(A2.PCT_M1 as decimal))
            , if(A2.PCT_M2 is null, '', cast(A2.PCT_M2 as decimal))
            , if(A2.PCT_M3 is null, '', cast(A2.PCT_M3 as decimal))
            , if(A2.LTV_BUY_RT is null, '', cast(A2.LTV_BUY_RT as decimal))
            , if(A2.LTV is null, '', cast(A2.LTV as decimal))
            , if(A2.LTV_GRD is null, '', A2.LTV_GRD)

            , if(D0.MCNT_CD1 is null, '', D0.MCNT_CD1)
            , if(D1.MID_NM is null, '', D1.MID_NM)
            ,CASE
                     WHEN D1.SIDO_NM is null THEN ''
            
                     WHEN D1.SIDO_NM like '서울%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'서울특별시','서울')
            
                     WHEN D1.SIDO_NM like '인천%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'인천광역시','인천')
                     WHEN D1.SIDO_NM like '울산%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'울산광역시','울산')
                     WHEN D1.SIDO_NM like '부산%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'부산광역시','부산')
                     WHEN D1.SIDO_NM like '대전%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'대전광역시','대전')
                     WHEN D1.SIDO_NM like '대구%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'대구광역시','대구')
                     WHEN D1.SIDO_NM like '광주%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'광주광역시','광주')
            
                     WHEN D1.SIDO_NM like '세종%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'세종특별자치시','세종')
                     WHEN D1.SIDO_NM like '제주%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'제주특별자치도','제주')
            
                     WHEN D1.SIDO_NM like '강원%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'강원도','강원')
                     WHEN D1.SIDO_NM like '경기%'        THEN REGEXP_REPLACE(D1.SIDO_NM,'경기도','경기')
            
                     WHEN D1.SIDO_NM = '전라남도'        THEN REGEXP_REPLACE(D1.SIDO_NM,'전라남도','전남')
                     WHEN D1.SIDO_NM = '전라북도'        THEN REGEXP_REPLACE(D1.SIDO_NM,'전라북도','전북')
                     WHEN D1.SIDO_NM = '충청남도'        THEN REGEXP_REPLACE(D1.SIDO_NM,'충청남도','충남')
                     WHEN D1.SIDO_NM = '충청북도'        THEN REGEXP_REPLACE(D1.SIDO_NM,'충청북도','충북')
                     WHEN D1.SIDO_NM = '경상남도'        THEN REGEXP_REPLACE(D1.SIDO_NM,'경상남도','경남')
                     WHEN D1.SIDO_NM = '경상북도'        THEN REGEXP_REPLACE(D1.SIDO_NM,'경상북도','경북')
                     ELSE ''
            END           
            
            ,CASE
                     WHEN D1.SIGUNGU_NM is null THEN ''
            
                     WHEN D1.SIGUNGU_NM like '서울%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'서울특별시','서울')
            
                     WHEN D1.SIGUNGU_NM like '인천%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'인천광역시','인천')
                     WHEN D1.SIGUNGU_NM like '울산%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'울산광역시','울산')
                     WHEN D1.SIGUNGU_NM like '부산%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'부산광역시','부산')
                     WHEN D1.SIGUNGU_NM like '대전%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'대전광역시','대전')
                     WHEN D1.SIGUNGU_NM like '대구%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'대구광역시','대구')
                     WHEN D1.SIGUNGU_NM like '광주%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'광주광역시','광주')
            
                     WHEN D1.SIGUNGU_NM like '세종%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'세종특별자치시','세종')
                     WHEN D1.SIGUNGU_NM like '제주%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'제주특별자치도','제주')
            
                     WHEN D1.SIGUNGU_NM like '강원%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'강원도','강원')
                     WHEN D1.SIGUNGU_NM like '경기%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'경기도','경기')
            
                     WHEN D1.SIGUNGU_NM like '전라남도%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'전라남도','전남')
                     WHEN D1.SIGUNGU_NM like '전라북도%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'전라북도','전북')
                     WHEN D1.SIGUNGU_NM like '충청남도%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'충청남도','충남')
                     WHEN D1.SIGUNGU_NM like '충청북도%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'충청북도','충북')
                     WHEN D1.SIGUNGU_NM like '경상남도%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'경상남도','경남')
                     WHEN D1.SIGUNGU_NM like '경상북도%'        THEN REGEXP_REPLACE(D1.SIGUNGU_NM,'경상북도','경북')
                     ELSE ''
            END
            
            ,CASE
                     WHEN D1.UPMYUNDONG_NM is null THEN ''

                     WHEN D1.UPMYUNDONG_NM like '서울%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'서울특별시','서울')

                     WHEN D1.UPMYUNDONG_NM like '인천%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'인천광역시','인천')
                     WHEN D1.UPMYUNDONG_NM like '울산%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'울산광역시','울산')
                     WHEN D1.UPMYUNDONG_NM like '부산%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'부산광역시','부산')
                     WHEN D1.UPMYUNDONG_NM like '대전%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'대전광역시','대전')
                     WHEN D1.UPMYUNDONG_NM like '대구%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'대구광역시','대구')
                     WHEN D1.UPMYUNDONG_NM like '광주%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'광주광역시','광주')

                     WHEN D1.UPMYUNDONG_NM like '세종%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'세종특별자치시','세종')
                     WHEN D1.UPMYUNDONG_NM like '제주%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'제주특별자치도','제주')

                     WHEN D1.UPMYUNDONG_NM like '강원%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'강원도','강원')
                     WHEN D1.UPMYUNDONG_NM like '경기%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'경기도','경기')

                     WHEN D1.UPMYUNDONG_NM like '전라남도%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'전라남도','전남')
                     WHEN D1.UPMYUNDONG_NM like '전라북도%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'전라북도','전북')
                     WHEN D1.UPMYUNDONG_NM like '충청남도%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'충청남도','충남')
                     WHEN D1.UPMYUNDONG_NM like '충청북도%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'충청북도','충북')
                     WHEN D1.UPMYUNDONG_NM like '경상남도%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'경상남도','경남')
                     WHEN D1.UPMYUNDONG_NM like '경상북도%'        THEN REGEXP_REPLACE(D1.UPMYUNDONG_NM,'경상북도','경북')
                     ELSE ''
            END
            
            , if(D0.MCNT_CD2 is null, '', D0.MCNT_CD2)
            , if(D2.MID_NM is null, '', D2.MID_NM)
            ,CASE
                     WHEN D2.SIDO_NM is null THEN ''

                     WHEN D2.SIDO_NM like '서울%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'서울특별시','서울')

                     WHEN D2.SIDO_NM like '인천%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'인천광역시','인천')
                     WHEN D2.SIDO_NM like '울산%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'울산광역시','울산')
                     WHEN D2.SIDO_NM like '부산%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'부산광역시','부산')
                     WHEN D2.SIDO_NM like '대전%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'대전광역시','대전')
                     WHEN D2.SIDO_NM like '대구%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'대구광역시','대구')
                     WHEN D2.SIDO_NM like '광주%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'광주광역시','광주')

                     WHEN D2.SIDO_NM like '세종%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'세종특별자치시','세종')
                     WHEN D2.SIDO_NM like '제주%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'제주특별자치도','제주')

                     WHEN D2.SIDO_NM like '강원%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'강원도','강원')
                     WHEN D2.SIDO_NM like '경기%'        THEN REGEXP_REPLACE(D2.SIDO_NM,'경기도','경기')

                     WHEN D2.SIDO_NM = '전라남도'        THEN REGEXP_REPLACE(D2.SIDO_NM,'전라남도','전남')
                     WHEN D2.SIDO_NM = '전라북도'        THEN REGEXP_REPLACE(D2.SIDO_NM,'전라북도','전북')
                     WHEN D2.SIDO_NM = '충청남도'        THEN REGEXP_REPLACE(D2.SIDO_NM,'충청남도','충남')
                     WHEN D2.SIDO_NM = '충청북도'        THEN REGEXP_REPLACE(D2.SIDO_NM,'충청북도','충북')
                     WHEN D2.SIDO_NM = '경상남도'        THEN REGEXP_REPLACE(D2.SIDO_NM,'경상남도','경남')
                     WHEN D2.SIDO_NM = '경상북도'        THEN REGEXP_REPLACE(D2.SIDO_NM,'경상북도','경북')
                     ELSE ''
            END

            ,CASE
                     WHEN D2.SIGUNGU_NM is null THEN ''

                     WHEN D2.SIGUNGU_NM like '서울%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'서울특별시','서울')

                     WHEN D2.SIGUNGU_NM like '인천%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'인천광역시','인천')
                     WHEN D2.SIGUNGU_NM like '울산%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'울산광역시','울산')
                     WHEN D2.SIGUNGU_NM like '부산%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'부산광역시','부산')
                     WHEN D2.SIGUNGU_NM like '대전%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'대전광역시','대전')
                     WHEN D2.SIGUNGU_NM like '대구%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'대구광역시','대구')
                     WHEN D2.SIGUNGU_NM like '광주%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'광주광역시','광주')

                     WHEN D2.SIGUNGU_NM like '세종%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'세종특별자치시','세종')
                     WHEN D2.SIGUNGU_NM like '제주%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'제주특별자치도','제주')

                     WHEN D2.SIGUNGU_NM like '강원%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'강원도','강원')
                     WHEN D2.SIGUNGU_NM like '경기%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'경기도','경기')

                     WHEN D2.SIGUNGU_NM like '전라남도%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'전라남도','전남')
                     WHEN D2.SIGUNGU_NM like '전라북도%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'전라북도','전북')
                     WHEN D2.SIGUNGU_NM like '충청남도%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'충청남도','충남')
                     WHEN D2.SIGUNGU_NM like '충청북도%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'충청북도','충북')
                     WHEN D2.SIGUNGU_NM like '경상남도%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'경상남도','경남')
                     WHEN D2.SIGUNGU_NM like '경상북도%'        THEN REGEXP_REPLACE(D2.SIGUNGU_NM,'경상북도','경북')
                     ELSE ''
            END

            ,CASE
                     WHEN D2.UPMYUNDONG_NM is null THEN ''

                     WHEN D2.UPMYUNDONG_NM like '서울%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'서울특별시','서울')

                     WHEN D2.UPMYUNDONG_NM like '인천%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'인천광역시','인천')
                     WHEN D2.UPMYUNDONG_NM like '울산%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'울산광역시','울산')
                     WHEN D2.UPMYUNDONG_NM like '부산%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'부산광역시','부산')
                     WHEN D2.UPMYUNDONG_NM like '대전%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'대전광역시','대전')
                     WHEN D2.UPMYUNDONG_NM like '대구%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'대구광역시','대구')
                     WHEN D2.UPMYUNDONG_NM like '광주%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'광주광역시','광주')

                     WHEN D2.UPMYUNDONG_NM like '세종%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'세종특별자치시','세종')
                     WHEN D2.UPMYUNDONG_NM like '제주%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'제주특별자치도','제주')

                     WHEN D2.UPMYUNDONG_NM like '강원%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'강원도','강원')
                     WHEN D2.UPMYUNDONG_NM like '경기%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'경기도','경기')

                     WHEN D2.UPMYUNDONG_NM like '전라남도%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'전라남도','전남')
                     WHEN D2.UPMYUNDONG_NM like '전라북도%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'전라북도','전북')
                     WHEN D2.UPMYUNDONG_NM like '충청남도%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'충청남도','충남')
                     WHEN D2.UPMYUNDONG_NM like '충청북도%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'충청북도','충북')
                     WHEN D2.UPMYUNDONG_NM like '경상남도%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'경상남도','경남')
                     WHEN D2.UPMYUNDONG_NM like '경상북도%'        THEN REGEXP_REPLACE(D2.UPMYUNDONG_NM,'경상북도','경북')
                     ELSE ''
            END


            , if(D0.MCNT_CD3 is null, '', D0.MCNT_CD3)
            , if(D3.MID_NM is null, '', D3.MID_NM)
            ,CASE
                     WHEN D3.SIDO_NM is null THEN ''

                     WHEN D3.SIDO_NM like '서울%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'서울특별시','서울')

                     WHEN D3.SIDO_NM like '인천%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'인천광역시','인천')
                     WHEN D3.SIDO_NM like '울산%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'울산광역시','울산')
                     WHEN D3.SIDO_NM like '부산%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'부산광역시','부산')
                     WHEN D3.SIDO_NM like '대전%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'대전광역시','대전')
                     WHEN D3.SIDO_NM like '대구%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'대구광역시','대구')
                     WHEN D3.SIDO_NM like '광주%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'광주광역시','광주')

                     WHEN D3.SIDO_NM like '세종%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'세종특별자치시','세종')
                     WHEN D3.SIDO_NM like '제주%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'제주특별자치도','제주')

                     WHEN D3.SIDO_NM like '강원%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'강원도','강원')
                     WHEN D3.SIDO_NM like '경기%'        THEN REGEXP_REPLACE(D3.SIDO_NM,'경기도','경기')

                     WHEN D3.SIDO_NM = '전라남도'        THEN REGEXP_REPLACE(D3.SIDO_NM,'전라남도','전남')
                     WHEN D3.SIDO_NM = '전라북도'        THEN REGEXP_REPLACE(D3.SIDO_NM,'전라북도','전북')
                     WHEN D3.SIDO_NM = '충청남도'        THEN REGEXP_REPLACE(D3.SIDO_NM,'충청남도','충남')
                     WHEN D3.SIDO_NM = '충청북도'        THEN REGEXP_REPLACE(D3.SIDO_NM,'충청북도','충북')
                     WHEN D3.SIDO_NM = '경상남도'        THEN REGEXP_REPLACE(D3.SIDO_NM,'경상남도','경남')
                     WHEN D3.SIDO_NM = '경상북도'        THEN REGEXP_REPLACE(D3.SIDO_NM,'경상북도','경북')
                     ELSE ''
            END

            ,CASE
                     WHEN D3.SIGUNGU_NM is null THEN ''
            
                     WHEN D3.SIGUNGU_NM like '서울%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'서울특별시','서울')
            
                     WHEN D3.SIGUNGU_NM like '인천%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'인천광역시','인천')
                     WHEN D3.SIGUNGU_NM like '울산%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'울산광역시','울산')
                     WHEN D3.SIGUNGU_NM like '부산%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'부산광역시','부산')
                     WHEN D3.SIGUNGU_NM like '대전%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'대전광역시','대전')
                     WHEN D3.SIGUNGU_NM like '대구%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'대구광역시','대구')
                     WHEN D3.SIGUNGU_NM like '광주%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'광주광역시','광주')
            
                     WHEN D3.SIGUNGU_NM like '세종%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'세종특별자치시','세종')
                     WHEN D3.SIGUNGU_NM like '제주%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'제주특별자치도','제주')
            
                     WHEN D3.SIGUNGU_NM like '강원%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'강원도','강원')
                     WHEN D3.SIGUNGU_NM like '경기%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'경기도','경기')
            
                     WHEN D3.SIGUNGU_NM like '전라남도%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'전라남도','전남')
                     WHEN D3.SIGUNGU_NM like '전라북도%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'전라북도','전북')
                     WHEN D3.SIGUNGU_NM like '충청남도%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'충청남도','충남')
                     WHEN D3.SIGUNGU_NM like '충청북도%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'충청북도','충북')
                     WHEN D3.SIGUNGU_NM like '경상남도%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'경상남도','경남')
                     WHEN D3.SIGUNGU_NM like '경상북도%'        THEN REGEXP_REPLACE(D3.SIGUNGU_NM,'경상북도','경북')
                     ELSE ''
            END

            ,CASE
                     WHEN D3.UPMYUNDONG_NM is null THEN ''

                     WHEN D3.UPMYUNDONG_NM like '서울%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'서울특별시','서울')

                     WHEN D3.UPMYUNDONG_NM like '인천%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'인천광역시','인천')
                     WHEN D3.UPMYUNDONG_NM like '울산%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'울산광역시','울산')
                     WHEN D3.UPMYUNDONG_NM like '부산%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'부산광역시','부산')
                     WHEN D3.UPMYUNDONG_NM like '대전%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'대전광역시','대전')
                     WHEN D3.UPMYUNDONG_NM like '대구%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'대구광역시','대구')
                     WHEN D3.UPMYUNDONG_NM like '광주%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'광주광역시','광주')

                     WHEN D3.UPMYUNDONG_NM like '세종%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'세종특별자치시','세종')
                     WHEN D3.UPMYUNDONG_NM like '제주%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'제주특별자치도','제주')

                     WHEN D3.UPMYUNDONG_NM like '강원%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'강원도','강원')
                     WHEN D3.UPMYUNDONG_NM like '경기%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'경기도','경기')

                     WHEN D3.UPMYUNDONG_NM like '전라남도%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'전라남도','전남')
                     WHEN D3.UPMYUNDONG_NM like '전라북도%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'전라북도','전북')
                     WHEN D3.UPMYUNDONG_NM like '충청남도%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'충청남도','충남')
                     WHEN D3.UPMYUNDONG_NM like '충청북도%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'충청북도','충북')
                     WHEN D3.UPMYUNDONG_NM like '경상남도%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'경상남도','경남')
                     WHEN D3.UPMYUNDONG_NM like '경상북도%'        THEN REGEXP_REPLACE(D3.UPMYUNDONG_NM,'경상북도','경북')
                     ELSE ''
            END,
            

            '',
            '','','','','','','','','','',
            '','','','','','','','','','',
            '','','','','','','','','','',
            '','','','','','','','','','',
            '','','','','','','','','','',
            '${hivevar:day2before}00',
            '${hivevar:day2before}00'

         FROM
         (
                SELECT CI FROM SVC_CUSTPFDB.PDB_MP_SKG_F_INFO WHERE M3_SALE_YN='Y'
                GROUP BY CI
         ) A0
         LEFT OUTER JOIN
              SVC_CUSTPFDB.PDB_MP_SKG_F_INFO
              A2 ON A0.CI=A2.CI
         LEFT OUTER JOIN
              ( SELECT CI
                     , MAX_SALE_QTY
                  FROM SVC_CUSTPFDB.PDB_MP_SKG_M_UNIT_PFMC
                 WHERE UNIT_CD='MONTH'
              ) B ON A0.CI=B.CI
         LEFT OUTER JOIN
              ( SELECT CI
                     , MAX(CASE WHEN TRADE_CD='03' THEN SALE_CNT ELSE 0 END) AS SALE_CNT03M
                     , MAX(CASE WHEN TRADE_CD='03' THEN SALE_AMT ELSE 0 END) AS SALE_AMT03M
                     , MAX(CASE WHEN TRADE_CD='03' THEN SALE_QTY ELSE 0 END) AS SALE_QTY03M
                     , MAX(CASE WHEN TRADE_CD='03' THEN DNT_MTH  ELSE 0 END) AS DNT_MTH03M
                     , MAX(CASE WHEN TRADE_CD='03' THEN DNT_DT   ELSE 0 END) AS DNT_DT03M
                     , MAX(CASE WHEN TRADE_CD='03' THEN DNT_MID  ELSE 0 END) AS DNT_MID03M
                     , MAX(CASE WHEN TRADE_CD='01' THEN SALE_PNT ELSE 0 END) AS RSVNG_PNT03M
                     , MAX(CASE WHEN TRADE_CD='02' THEN SALE_PNT ELSE 0 END) AS USE_PNT03M
                  FROM SVC_CUSTPFDB.PDB_MP_SKG_H_MCNT_PFMC
                 WHERE MCNT_CD='99'
                   AND UNIT_CD='QUARTER'
                   AND TIME_UNIT='03M'
                GROUP BY CI
              ) C1 ON A0.CI=C1.CI
         LEFT OUTER JOIN
              ( SELECT CI
                     , MAX(CASE WHEN TRADE_CD='03' THEN SALE_CNT ELSE 0 END) AS SALE_CNT01M
                     , MAX(CASE WHEN TRADE_CD='03' THEN SALE_AMT ELSE 0 END) AS SALE_AMT01M
                     , MAX(CASE WHEN TRADE_CD='03' THEN SALE_QTY ELSE 0 END) AS SALE_QTY01M
                     , MAX(CASE WHEN TRADE_CD='03' THEN DNT_MTH  ELSE 0 END) AS DNT_MTH01M
                     , MAX(CASE WHEN TRADE_CD='03' THEN DNT_DT   ELSE 0 END) AS DNT_DT01M
                     , MAX(CASE WHEN TRADE_CD='03' THEN DNT_MID  ELSE 0 END) AS DNT_MID01M
                     , MAX(CASE WHEN TRADE_CD='01' THEN SALE_PNT ELSE 0 END) AS RSVNG_PNT01M
                     , MAX(CASE WHEN TRADE_CD='02' THEN SALE_PNT ELSE 0 END) AS USE_PNT01M
                  FROM SVC_CUSTPFDB.PDB_MP_SKG_H_MCNT_PFMC
                 WHERE MCNT_CD='99'
                   AND UNIT_CD='MONTH'
                   AND TIME_UNIT='M1'
                GROUP BY CI
              ) C2 ON A0.CI=C2.CI
         LEFT OUTER JOIN
              ( SELECT CI
                     , MAX(CASE WHEN RNK=1 THEN MCNT_CD END) AS MCNT_CD1
                     , MAX(CASE WHEN RNK=2 THEN MCNT_CD END) AS MCNT_CD2
                     , MAX(CASE WHEN RNK=3 THEN MCNT_CD END) AS MCNT_CD3
                  FROM SVC_CUSTPFDB.PDB_MP_SKG_M_MAIN_MCNT
                GROUP BY CI
              ) D0 ON A0.CI=D0.CI
         LEFT OUTER JOIN
              SVC_CUSTPFDB.PDB_MP_SKG_F_MCNT
              D1 ON D0.MCNT_CD1=D1.MCNT_CD
         LEFT OUTER JOIN
              SVC_CUSTPFDB.PDB_MP_SKG_F_MCNT
              D2 ON D0.MCNT_CD2=D2.MCNT_CD
         LEFT OUTER JOIN
              SVC_CUSTPFDB.PDB_MP_SKG_F_MCNT
              D3 ON D0.MCNT_CD3=D3.MCNT_CD
       ;


