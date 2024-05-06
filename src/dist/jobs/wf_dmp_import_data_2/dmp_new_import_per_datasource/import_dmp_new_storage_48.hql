

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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='48')

SELECT
                CI,
                if(REG_DT is null, '', REG_DT),
                if(MBRSHP_ID is null, '', MBRSHP_ID),
                if(MBRSHP_NM is null, '', MBRSHP_NM),
                if(TAID is null, '', TAID),
                if(ALLIANCE_NM is null, '', ALLIANCE_NM),
                if(MILEAGE_TP is null, '', MILEAGE_TP),
                if(MILEAGE_KEY is null, '', MILEAGE_KEY),
                if(CARD_CD is null, '', CARD_CD),
                if(GRD_TP is null, '', GRD_TP),
                if(GRD_RULE_TP is null, '', GRD_RULE_TP),
                if(GRD_RULE_ID is null, '', GRD_RULE_ID),
                if(GRD_NM is null, '', GRD_NM),
                if(POC_CD is null, '', POC_CD),
                if(POC_NM is null, '', POC_NM),
                if(TEL_CO is null, '', TEL_CO),
                if(FOREIGNER_YN is null, '', FOREIGNER_YN),
                if(GENDER is null, '', GENDER),
                if(AGE is null, '', AGE),
                if(ADDR_TP is null, '', ADDR_TP),
                if(ZIPCODE is null, '', ZIPCODE),
                if(CAR_TP is null, '', CAR_TP),
                if(CAR_BRND is null, '', CAR_BRND),
                if(CAR_CTGR is null, '', CAR_CTGR),
                if(FV_STATION is null, '', FV_STATION),
                if(CAR_OWN_TP is null, '', CAR_OWN_TP),
                if(PURPOSE is null, '', PURPOSE),

                '','','','',
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
                CI,
                REG_DT,
                MBRSHP_ID,
                MBRSHP_NM,
                TAID,
                ALLIANCE_NM,
                MILEAGE_TP,
                MILEAGE_KEY,
                CARD_CD,
                GRD_TP,
                GRD_RULE_TP,
                GRD_RULE_ID,
                GRD_NM,
                POC_CD,
                POC_NM,
                TEL_CO,
                FOREIGNER_YN,
                GENDER,

                CASE WHEN
                 SUBSTR(BIRTHDAY,1,4) BETWEEN '1900' AND YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP()))
                THEN
                 YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) - SUBSTR(BIRTHDAY,1,4) + 1
                END AS AGE,

                ADDR_TP,
                ZIPCODE,
                CAR_TP,
                CAR_BRND,
                CAR_CTGR,
                FV_STATION,
                CAR_OWN_TP,
                PURPOSE

                FROM
                SVC_CUSTPFDB.PDB_MP_SKG_MBRSHP_INFO



         ) A0
         ;


