
USE SVC_DS_DMP_PI;
set hive.execution.engine=tez;


-- common tables

CREATE TABLE IF NOT EXISTS SVC_DS_DMP_PI.PROD_USER_SEGMENT_MAP_TODAY
(
DMP_ID       STRING,
SKP_SEG_ID  ARRAY<STRING>
)

CLUSTERED BY (DMP_ID) SORTED BY (DMP_ID,SKP_SEG_ID) INTO 256 BUCKETS
ROW FORMAT
DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n'
;

CREATE TABLE IF NOT EXISTS SVC_DS_DMP_PI.PROD_USER_SEGMENT_MAP_YESTD
(
DMP_ID       STRING,
SKP_SEG_ID  ARRAY<STRING>
)

CLUSTERED BY (DMP_ID) SORTED BY (DMP_ID,SKP_SEG_ID) INTO 256 BUCKETS
ROW FORMAT
DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n'
;

DROP TABLE IF EXISTS SVC_DS_DMP_PI.PROD_DMP_ID_MATCHING;
CREATE TABLE SVC_DS_DMP_PI.PROD_DMP_ID_MATCHING
(
DMP_ID              STRING,
DSP_COOKIE_ID       STRING
)
PARTITIONED BY (ID_TYPE STRING)
;

DROP TABLE IF EXISTS SVC_DS_DMP_PI.DMP_DAILY_DATASET_DELTA_TEMP;
CREATE TABLE SVC_DS_DMP_PI.DMP_DAILY_DATASET_DELTA_TEMP
(
DMP_ID STRING,
SEG_ID STRING,
DELTA_TYPE STRING
)
;

DROP TABLE IF EXISTS SVC_DS_DMP_PI.DMP_DAILY_DATASET_FULL_TEMP;
CREATE TABLE SVC_DS_DMP_PI.DMP_DAILY_DATASET_FULL_TEMP
(
DMP_ID STRING,
SEG_ID STRING,
DELTA_TYPE STRING
)
;


-- TTD tables



-- DataXU tables



-- AppNexus tables



-- Mediamath tables

