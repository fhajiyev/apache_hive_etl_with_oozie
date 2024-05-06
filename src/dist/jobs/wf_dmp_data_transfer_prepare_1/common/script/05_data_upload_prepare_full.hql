
USE SVC_DS_DMP;


set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;




set hivevar:dsp_id_type;
set hivevar:current_hhmm;

INSERT INTO TABLE SVC_DS_DMP.PROD_TRANSFER_DATASET_FULL_NEW partition(PART_HOUR,USER_TYPE)

SELECT d.SELECTED_ID, d.SEG_ID, d.DELTA_TYPE, '${hivevar:current_hhmm}', d.USER_TYPE
FROM
(

  SELECT a.SELECTED_ID, b.SEG_ID, a.USER_TYPE, b.DELTA_TYPE
  FROM

  (
    SELECT e.DMP_ID, e.SELECTED_ID, e.USER_TYPE
    FROM
    (

            SELECT DMP_ID, DSP_COOKIE_ID AS SELECTED_ID, '${hivevar:dsp_id_type}' AS USER_TYPE FROM SVC_DS_DMP.PROD_DMP_ID_MATCHING WHERE ID_TYPE = '${hivevar:dsp_id_type}'

    ) e

    WHERE e.SELECTED_ID IS NOT NULL AND e.SELECTED_ID <> ''

  )
  a

  INNER JOIN

  (
  SELECT DMP_ID, SEG_ID, DELTA_TYPE
  FROM
  SVC_DS_DMP.DMP_DAILY_DATASET_FULL_TEMP
  )
  b

  ON
  a.DMP_ID = b.DMP_ID

)
d

;
