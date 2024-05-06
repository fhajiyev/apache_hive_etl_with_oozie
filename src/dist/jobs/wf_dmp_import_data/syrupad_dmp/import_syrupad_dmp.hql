USE ${db};

set hivevar:dmp_site_id;
set hivevar:dmp_data_source_id;
set hivevar:from_dt;
set hivevar:to_dt;




set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.execution.engine=tez;
set mapreduce.job.reduces=128;


-- DROP TABLE IF EXISTS ${db}.data_store;

CREATE TABLE IF NOT EXISTS ${db}.data_store
(
  dmp_uid   string,
  site_uid   string,

  col001    string,
  col002    string,
  col003    string,
  col004    string,
  col005    string,
  col006    string,
  col007    string,
  col008    string,
  col009    string,
  col010    string,

  col011    string,
  col012    string,
  col013    string,
  col014    string,
  col015    string,
  col016    string,
  col017    string,
  col018    string,
  col019    string,
  col020    string,

  col021    string,
  col022    string,
  col023    string,
  col024    string,
  col025    string,
  col026    string,
  col027    string,
  col028    string,
  col029    string,
  col030    string,

  col031    string,
  col032    string,
  col033    string,
  col034    string,
  col035    string,
  col036    string,
  col037    string,
  col038    string,
  col039    string,
  col040    string,

  col041    string,
  col042    string,
  col043    string,
  col044    string,
  col045    string,
  col046    string,
  col047    string,
  col048    string,
  col049    string,
  col050    string,

  col051    string,
  col052    string,
  col053    string,
  col054    string,
  col055    string,
  col056    string,
  col057    string,
  col058    string,
  col059    string,
  col060    string,

  col061    string,
  col062    string,
  col063    string,
  col064    string,
  col065    string,
  col066    string,
  col067    string,
  col068    string,
  col069    string,
  col070    string,

  col071    string,
  col072    string,
  col073    string,
  col074    string,
  col075    string,
  col076    string,
  col077    string,
  col078    string,
  col079    string,
  col080    string,

  col081    string,
  col082    string,
  col083    string,
  col084    string,
  col085    string,
  col086    string,
  col087    string,
  col088    string,
  col089    string,
  col090    string,

  col091    string,
  col092    string,
  col093    string,
  col094    string,
  col095    string,
  col096    string,
  col097    string,
  col098    string,
  col099    string,
  col100    string
)
PARTITIONED BY (dmp_site_id string, dmp_data_source_id string, dmp_part_date string);

ALTER TABLE ${db}.data_store DROP IF EXISTS PARTITION (dmp_site_id='${hivevar:dmp_site_id}', dmp_data_source_id='${hivevar:dmp_data_source_id}', dmp_part_date='${hivevar:from_dt}');

set hive.exec.dynamic.partition.mode=true;

INSERT INTO TABLE ${db}.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT
  COL008 as dmp_uid, uid as site_uid,

  action
  , REQUEST_DATE date
  , REQUEST_TIME time
  , null, null, null, null, null, null, null

  , null
  , COL010 channel
  , null, null, null, null
  , CONCAT('11ST',IDLENGTH) idlength
  , SITE site
  , CASE WHEN ACTION =  'search' THEN REGEXP_REPLACE(ID,'\\^',' ') 
    ELSE null
    END searchkeyword
  , CASE WHEN ACTION IN ('basket', 'order', 'wish', 'search') THEN NULL
    ELSE REGEXP_REPLACE(PROD_NM,'\\^',' ')
    END prdname

  , CASE WHEN ACTION IN ('basket', 'order', 'wish') THEN null
    ELSE
    TRIM(
      CONCAT(
       CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
       ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
       END,
       CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
       END
       )
       )
    END prdcatname
  , CASE WHEN PRDPRICE IN ('', ' ')                          THEN '-1'
           WHEN PRDPRICE = 0                                 THEN '0'
           WHEN (PRDPRICE > 0       AND PRDPRICE <= 9999)    THEN '1'
           WHEN (PRDPRICE >= 10000  AND PRDPRICE <= 19999)   THEN '2'
           WHEN (PRDPRICE >= 20000  AND PRDPRICE <= 29999)   THEN '3'
           WHEN (PRDPRICE >= 30000  AND PRDPRICE <= 39999)   THEN '4'
           WHEN (PRDPRICE >= 40000  AND PRDPRICE <= 49999)   THEN '5'
           WHEN (PRDPRICE >= 50000  AND PRDPRICE <= 59999)   THEN '6'
           WHEN (PRDPRICE >= 60000  AND PRDPRICE <= 69999)   THEN '7'
           WHEN (PRDPRICE >= 70000  AND PRDPRICE <= 79999)   THEN '8'
           WHEN (PRDPRICE >= 80000  AND PRDPRICE <= 89999)   THEN '9'
           WHEN (PRDPRICE >= 90000  AND PRDPRICE <= 99999)   THEN '10'
           WHEN (PRDPRICE >= 100000 AND PRDPRICE <= 199999)  THEN '20'
           WHEN (PRDPRICE >= 200000 AND PRDPRICE <= 299999)  THEN '30'
           WHEN (PRDPRICE >= 300000 AND PRDPRICE <= 399999)  THEN '40'
           WHEN (PRDPRICE >= 400000 AND PRDPRICE <= 499999)  THEN '50'
           WHEN (PRDPRICE >= 500000 AND PRDPRICE <= 599999)  THEN '60'
           WHEN (PRDPRICE >= 600000 AND PRDPRICE <= 699999)  THEN '70'
           WHEN (PRDPRICE >= 700000 AND PRDPRICE <= 799999)  THEN '80'
           WHEN (PRDPRICE >= 800000 AND PRDPRICE <= 899999)  THEN '90'
           WHEN (PRDPRICE >= 900000 AND PRDPRICE <= 999999)  THEN '100'
           WHEN PRDPRICE >= 1000000                          THEN '200'
    ELSE '_'
    END prdprice
  , null, null, null, null, null
  , CASE WHEN ACTION =  'basket' THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
     ELSE null
     END prdname_basket
  , CASE WHEN ACTION =  'order' THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
       ELSE null
       END prdname_order
  , CASE WHEN ACTION =  'wish' THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
       ELSE null
       END prdname_wish

  , CASE WHEN ACTION IN ('basket') THEN
    TRIM(
      CONCAT(
       CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
       ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
       END,
       CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
       END
       )
         )
    ELSE null END prdcatname_basket
  , CASE WHEN ACTION IN ('order') THEN
    TRIM(
      CONCAT(
       CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
       ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
       END,
       CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
       END
       )
         )
    ELSE null END prdcatname_order
  , CASE WHEN ACTION IN ('wish') THEN
    TRIM(
      CONCAT(
       CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
       ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
       END,
       CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
       END,
       CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
       ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
       END
       )
         )
    ELSE null END prdcatname_wish
  , null, null, null, null, null, null, null,
  null, null
  , COL002 gaid
  , COL003 idfa
  , null, null, null, null, null, null,
  null
  , COL004 tdid
  , COL005 mid
  , COL006 recopick_id
  , COL007 dxid,
  null, null, null, null, null,
  null, null
  , COL009 pcid,
  null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,

  '${hivevar:dmp_site_id}' as dmp_site_id, '${hivevar:dmp_data_source_id}' as dmp_data_source_id, part_date as dmp_part_date
 FROM svc_ds_dmp.dmp_log_server_tracking
WHERE (part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}') AND site = '2';

INSERT INTO TABLE ${db}.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT
  COL008 as dmp_uid, uid as site_uid,

    CASE WHEN ACTION = 'view'     THEN 'couponView'
         WHEN ACTION = 'cdown'    THEN 'couponDown'
         WHEN ACTION = 'search'   THEN 'couponSearch'
         WHEN ACTION = 'welcome'  THEN 'welcome'
    ELSE null
    END act
  , REQUEST_DATE date
  , REQUEST_TIME time
  , null, null, null, null, null, null, null

  , null
  , COL010 channel
  , null, null, null, null, null
  , CASE WHEN ACTION = 'welcome' THEN '9a'
         ELSE SITE
    END site
  , CASE WHEN ACTION = 'search'  THEN REGEXP_REPLACE(ID,'\\^',' ')
         ELSE null
    END searchkeyword
  , null

  , null, null
  , CASE WHEN ACTION = 'view'  THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
         ELSE null
    END couponviewname
  , CASE WHEN ACTION = 'cdown'  THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
         ELSE null
    END coupondownname
  , null age
  , CASE WHEN ACTION IN ('view') THEN
    TRIM(
          CONCAT(
           CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
           ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
           END,
           CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
           END
           )
      )
         ELSE null
    END couponviewcat
  , CASE WHEN ACTION IN ('cdown') THEN
    TRIM(
          CONCAT(
           CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
           ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
           END,
           CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
           END
           )
       )
         ELSE null 
    END coupondowncat
  , null, null, null,

  null, null, null, null, null, null, null, null, null, null,
  null, null
  , COL002 gaid
  , COL003 idfa
  , null, null, null, null, null, null,
  null, null, null
  , COL006 recopick_id
  , null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,

  '${hivevar:dmp_site_id}' as dmp_site_id, '${hivevar:dmp_data_source_id}' as dmp_data_source_id, part_date as dmp_part_date
 FROM svc_ds_dmp.dmp_log_server_tracking
WHERE (part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}') AND site = '9';

INSERT INTO TABLE ${db}.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT
  COL008 as dmp_uid, uid as site_uid,

    CASE WHEN ACTION = 'search'  THEN 'couponSearch'
         WHEN ACTION = 'welcome'  THEN 'welcome'
         WHEN ACTION = 'couponview' THEN 'couponView'
         WHEN ACTION = 'coupondown' THEN 'couponDown'
         WHEN ACTION = 'membershipview' THEN 'membershipView'
         WHEN ACTION = 'membershipdown' THEN 'membershipDown'
    ELSE null
    END act
  , REQUEST_DATE date
  , REQUEST_TIME time
  , null, null, null, null, null, null, null

  , null
  , COL010 channel
  , null, null, null, null, null
  , CASE WHEN ACTION = 'welcome' THEN '10a'
         ELSE SITE
    END site
  , CASE WHEN ACTION = 'search'  THEN REGEXP_REPLACE(ID,'\\^',' ')
         ELSE null
    END searchkeyword
  , null

  , null, null
  , CASE WHEN ACTION = 'couponview'  THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
         ELSE null
    END couponviewname
  , CASE WHEN ACTION = 'coupondown'  THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
         ELSE null
    END coupondownname
  , null age
  , CASE WHEN ACTION IN ('couponview') THEN
    TRIM(
          CONCAT(
           CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
           ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
           END,
           CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
           END
           )
         )
         ELSE null
    END couponviewcat
  , CASE WHEN ACTION IN ('coupondown') THEN
    TRIM(
          CONCAT(
           CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
           ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
           END,
           CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
           END
           )
    )
         ELSE null
    END coupondowncat
  , null, null, null,

  null, null, null
  , CASE WHEN ACTION = 'membershipview'  THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
      ELSE null
      END membershipviewname
  , CASE WHEN ACTION = 'membershipdown'  THEN REGEXP_REPLACE(PROD_NM,'\\^',' ')
      ELSE null
      END membershipdownname
  , CASE WHEN ACTION IN ('membershipview') THEN
    TRIM(
          CONCAT(
           CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
           ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
           END,
           CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
           END,
           CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
           ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
           END
           )
    )
         ELSE null
    END membershipviewcat
    , CASE WHEN ACTION IN ('membershipdown') THEN
      TRIM(
            CONCAT(
             CASE WHEN CATEGORY_CD1 IN (' ','') OR CATEGORY_CD1 IS NULL THEN ''
             ELSE SUBSTR(CATEGORY_CD1,(INSTR(CATEGORY_CD1, ':')+1))
             END,
             CASE WHEN CATEGORY_CD2 IN (' ','') OR CATEGORY_CD2 IS NULL THEN ''
             ELSE CONCAT('\||',SUBSTR(CATEGORY_CD2,(INSTR(CATEGORY_CD2, ':')+1)))
             END,
             CASE WHEN CATEGORY_CD3 IN (' ','') OR CATEGORY_CD3 IS NULL THEN ''
             ELSE CONCAT('\||',SUBSTR(CATEGORY_CD3,(INSTR(CATEGORY_CD3, ':')+1)))
             END,
             CASE WHEN CATEGORY_CD4 IN (' ','') OR CATEGORY_CD4 IS NULL THEN ''
             ELSE CONCAT('\||',SUBSTR(CATEGORY_CD4,(INSTR(CATEGORY_CD4, ':')+1)))
             END,
             CASE WHEN CATEGORY_CD5 IN (' ','') OR CATEGORY_CD5 IS NULL THEN ''
             ELSE CONCAT('\||',SUBSTR(CATEGORY_CD5,(INSTR(CATEGORY_CD5, ':')+1)))
             END,
             CASE WHEN CATEGORY_CD6 IN (' ','') OR CATEGORY_CD6 IS NULL THEN ''
             ELSE CONCAT('\||',SUBSTR(CATEGORY_CD6,(INSTR(CATEGORY_CD6, ':')+1)))
             END
             )
      )
           ELSE null
      END membershipdowncat
      , null, null, null,
  null, null
  , COL002 gaid
  , COL003 idfa
  , null, null, null, null, null, null,
  null, null, null
  , COL006 recopick_id
  , null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,

  '${hivevar:dmp_site_id}' as dmp_site_id, '${hivevar:dmp_data_source_id}' as dmp_data_source_id, part_date as dmp_part_date
 FROM svc_ds_dmp.dmp_log_server_tracking
WHERE (part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}') AND site = '10';

INSERT INTO TABLE ${db}.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT
  COL008 as dmp_uid, uid as site_uid,

  null, null, null, null, null, null, null, null, null, null,
  null
  , COL010 channel,
  null, null, null, null, null, null, null, null,
  null, null, null, null, id age, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null
  , COL002 gaid
  , COL003 idfa
  , null, null, null, null, null, null,
  null, null, null
  , COL006 recopick_id
  , null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,

  '${hivevar:dmp_site_id}' as dmp_site_id, '${hivevar:dmp_data_source_id}' as dmp_data_source_id, part_date as dmp_part_date
 FROM 
  (
  SELECT
         COL008
       , uid
       , part_date
       , id
       , COL002
       , COL003
       , COL006
       , COL010
       , ROW_NUMBER() OVER(PARTITION BY uid ORDER BY part_hour DESC ) AS RN
   FROM svc_ds_dmp.dmp_log_server_tracking
  WHERE (part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}') AND site = '10' AND action = 'welcome'
  ) a
WHERE RN = 1;


INSERT INTO TABLE ${db}.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT
  COL008 as dmp_uid
, uid as site_uid
, CASE WHEN ACTION = 'search' THEN 'tmapsearch' WHEN ACTION = 'rdest' THEN 'tmap_recent_dest' WHEN ACTION = 'fdest' THEN 'tmap_favorite_dest' WHEN ACTION = 'sdest' THEN 'tmap_search_dest' WHEN ACTION = 'welcome' THEN 'tmapwelcome' END act
, REQUEST_DATE date
, REQUEST_TIME time
, null, null, null, null, null, null, null
, null
, COL010 channel
, null, null, null, null
, null
, SITE site

, null
, null
, null
, null, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null
, null
, null
, COL002 gaid
, COL003 idfa
, null
, null
, null
, null
, null
, null
, null
, null, null
, COL006 recopick_id
, null

, CASE WHEN ACTION IN ('sdest','rdest','fdest') THEN REGEXP_REPLACE(COL001,'\\^',' ')
            ELSE null
       END tmapref

, CATEGORY_CD1 tmappoicategory1
, CATEGORY_CD2 tmappoicategory2
, CATEGORY_CD3 tmappoicategory3

, PROD_NM tmapfavoritetitle

, CASE WHEN ACTION = 'search'  THEN REGEXP_REPLACE(ID,'\\^',' ')
            ELSE null
       END tmapkeyword

, CASE WHEN ACTION IN ('sdest','rdest','fdest')  THEN REGEXP_REPLACE(ID,'\\^',' ')
            ELSE null
       END tmapdestination

, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null
, '${hivevar:dmp_site_id}' as dmp_site_id
, '${hivevar:dmp_data_source_id}' as dmp_data_source_id
, part_date as dmp_part_date

 FROM svc_ds_dmp.dmp_log_server_tracking
WHERE (part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}') AND (site = '11');









INSERT INTO TABLE ${db}.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT
  COL008 as dmp_uid
, uid as site_uid
, CASE WHEN ACTION = 'welcome' THEN 'taxiwelcome' WHEN ACTION = 'search' THEN 'taxikeyword' WHEN ACTION = 'sdest' THEN 'taxidestination' END act
, REQUEST_DATE date
, REQUEST_TIME time
, null, null, null, null, null, null, null
, null
, COL010 channel
, null, null, null, null
, null
, SITE site

, null
, null
, null
, null, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null

, CASE WHEN ACTION = 'sdest'  THEN REGEXP_REPLACE(ID,'\\^',' ')
          ELSE null
     END taxidestination

, CASE WHEN ACTION = 'sdest'  THEN REGEXP_REPLACE(COL001,'\\^',' ')
          ELSE null
     END taxiref

, COL002 gaid
, COL003 idfa
, CASE WHEN ACTION = 'search'  THEN REGEXP_REPLACE(ID,'\\^',' ')
          ELSE null
     END taxikeyword

, CATEGORY_CD1 taxipoicategory1
, CATEGORY_CD2 taxipoicategory2
, CATEGORY_CD3 taxipoicategory3
, CATEGORY_CD4 taxipoicategory4
, CATEGORY_CD5 taxipoicategory5
, CATEGORY_CD6 taxipoicategory6

, null, null
, COL006 recopick_id
, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null
, null, null, null, null, null, null, null, null, null, null
, '${hivevar:dmp_site_id}' as dmp_site_id
, '${hivevar:dmp_data_source_id}' as dmp_data_source_id
, part_date as dmp_part_date

 FROM svc_ds_dmp.dmp_log_server_tracking
WHERE (part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}') AND (site = '17');




INSERT INTO TABLE ${db}.data_store PARTITION (dmp_site_id, dmp_data_source_id, dmp_part_date)
SELECT
  DMP_UID as dmp_uid, UID as site_uid,

  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, AGE, null, null, null, null, null,
  null, null, null, null, null, null, null
  , POINT ocbpoint
  , CASE WHEN GENCLASS = '7' THEN 'N'
         WHEN GENCLASS = '4' THEN 'A'
         WHEN GENCLASS = '8' THEN 'B'
    ELSE ''
    END genclass
  , null,
  null, null, null, null, null, null, null, null, null, null,

  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,
  null, null, null, null, null, null, null, null, null, null,

  '${hivevar:dmp_site_id}' as dmp_site_id, '${hivevar:dmp_data_source_id}' as dmp_data_source_id, part_date as dmp_part_date
  FROM svc_ds_dmp.dmp_log_server_tracking_user_mode
 WHERE (part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}');



