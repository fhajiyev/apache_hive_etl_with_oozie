
USE SVC_DS_DMP_PI;


set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

set hivevar:day2before;


INSERT INTO TABLE SVC_DS_DMP_PI.DMP_DAILY_DATASET_FULL_TEMP

 SELECT DMP_ID, SEG_ID, 'plus'
 FROM
 SVC_DS_DMP_PI.PROD_USER_SEGMENT_MAP_TODAY
 LATERAL VIEW EXPLODE(SKP_SEG_ID) SKP_SEGS_LIST AS SEG_ID
;

ALTER TABLE svc_ds_dmp_pi.prod_transfer_dataset_full_new DROP IF EXISTS PARTITION (part_hour>='${hivevar:day2before}00', part_hour<='${hivevar:day2before}24');

