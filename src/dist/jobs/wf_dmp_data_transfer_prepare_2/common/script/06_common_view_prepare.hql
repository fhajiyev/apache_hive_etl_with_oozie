
USE SVC_DS_DMP_PI;
set hive.execution.engine=tez;

set hivevar:current_hhmm;

DROP VIEW IF EXISTS SVC_DS_DMP_PI.PROD_TRANSFER_DATASET_DIFF_HIVE_VIEW;
CREATE VIEW SVC_DS_DMP_PI.PROD_TRANSFER_DATASET_DIFF_HIVE_VIEW AS
SELECT
DMP_ID,
SKP_SEG_ID,
DELTA_TYPE,
USER_TYPE
FROM
SVC_DS_DMP_PI.PROD_TRANSFER_DATASET_DIFF_NEW
WHERE
PART_HOUR = '${hivevar:current_hhmm}'
;

DROP VIEW IF EXISTS SVC_DS_DMP_PI.PROD_TRANSFER_DATASET_FULL_HIVE_VIEW;
CREATE VIEW SVC_DS_DMP_PI.PROD_TRANSFER_DATASET_FULL_HIVE_VIEW AS
SELECT
DMP_ID,
SKP_SEG_ID,
DELTA_TYPE,
USER_TYPE
FROM
SVC_DS_DMP_PI.PROD_TRANSFER_DATASET_FULL_NEW
WHERE
PART_HOUR = '${hivevar:current_hhmm}'
;


