
USE SVC_DS_DMP_PI;


set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


set hivevar:current_hhmm;

INSERT INTO TABLE SVC_DS_DMP_PI.PROD_TRANSFER_DATASET_DIFF_NEW partition(PART_HOUR,USER_TYPE)

SELECT d.SELECTED_ID, d.SEG_ID, d.DELTA_TYPE, '${hivevar:current_hhmm}', d.USER_TYPE
FROM
(

  SELECT a.SELECTED_ID, b.SEG_ID, a.USER_TYPE, b.DELTA_TYPE
  FROM

  (
    SELECT e.DMP_ID, e.SELECTED_ID, e.USER_TYPE
    FROM
    (

            SELECT DMP_ID, DSP_COOKIE_ID AS SELECTED_ID, 'gaid'                      AS USER_TYPE FROM SVC_DS_DMP_PI.PROD_DMP_ID_MATCHING WHERE ID_TYPE = 'gaid'
            UNION ALL
            SELECT DMP_ID, DSP_COOKIE_ID AS SELECTED_ID, 'idfa'                      AS USER_TYPE FROM SVC_DS_DMP_PI.PROD_DMP_ID_MATCHING WHERE ID_TYPE = 'idfa'

    ) e

    WHERE e.SELECTED_ID IS NOT NULL AND e.SELECTED_ID <> ''

  )
  a

  INNER JOIN

  (
  SELECT DMP_ID, SEG_ID, DELTA_TYPE
  FROM
  SVC_DS_DMP_PI.DMP_DAILY_DATASET_DELTA_TEMP
  )
  b

  ON
  a.DMP_ID = b.DMP_ID

)
d

;
