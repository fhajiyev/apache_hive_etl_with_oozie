
USE SVC_DS_DMP;


set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;





set hivevar:current_hhmm;

INSERT INTO TABLE SVC_DS_DMP.PROD_TRANSFER_DATASET_FULL_NEW partition(PART_HOUR,USER_TYPE)

SELECT DMP_ID, SEG_ID, DELTA_TYPE, '${hivevar:current_hhmm}', 'dmpc'
FROM
SVC_DS_DMP.DMP_DAILY_DATASET_FULL_TEMP
WHERE
(DMP_ID IS NOT NULL AND SUBSTR(DMP_ID,1,6) = '(DMPC)')
;
