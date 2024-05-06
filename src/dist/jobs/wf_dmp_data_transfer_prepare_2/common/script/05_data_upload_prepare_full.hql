
USE SVC_DS_DMP_PI;


set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;



set hivevar:day2before;
set hivevar:dsp_id_type;
set hivevar:current_hhmm;

INSERT INTO TABLE SVC_DS_DMP_PI.PROD_TRANSFER_DATASET_FULL_NEW partition(PART_HOUR,USER_TYPE)

SELECT d.SELECTED_ID, d.SEG_ID, d.DELTA_TYPE, '${hivevar:current_hhmm}', d.USER_TYPE
FROM
(

  SELECT a.SELECTED_ID, b.SEG_ID, a.USER_TYPE, b.DELTA_TYPE
  FROM

  (
    SELECT e.CI, e.SELECTED_ID, e.USER_TYPE
    FROM
    (

            SELECT
            CI,
            CASE
              WHEN '${hivevar:dsp_id_type}' = 'uid001'  THEN OCB_ID
              WHEN '${hivevar:dsp_id_type}' = 'uid002'  THEN SW_ID
              WHEN '${hivevar:dsp_id_type}' = 'uid003'  THEN ELEV_ID
              WHEN '${hivevar:dsp_id_type}' = 'mdn_ocb' THEN MDN_OCB
              WHEN '${hivevar:dsp_id_type}' = 'mdn_sw'  THEN MDN_SW
            END
            AS SELECTED_ID,
            '${hivevar:dsp_id_type}' AS USER_TYPE
            FROM
            DMP_PI.ID_POOL
            WHERE PART_DATE = '${hivevar:day2before}'

    ) e

    WHERE e.SELECTED_ID IS NOT NULL AND e.SELECTED_ID <> ''

  )
  a

  INNER JOIN

  (
  SELECT DMP_ID, SEG_ID, DELTA_TYPE
  FROM
  SVC_DS_DMP_PI.DMP_DAILY_DATASET_FULL_TEMP
  )
  b

  ON
  a.CI = b.DMP_ID

)
d

;
