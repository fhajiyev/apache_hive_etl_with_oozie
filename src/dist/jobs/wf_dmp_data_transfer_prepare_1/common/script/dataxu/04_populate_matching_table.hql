

USE SVC_DS_DMP;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hivevar:dataxu_dt_from;
set hivevar:dataxu_dt_to;

INSERT INTO TABLE SVC_DS_DMP.PROD_DMP_ID_MATCHING partition(ID_TYPE)

 SELECT

 a.DMP_ID,
 a.DSP_COOKIE_ID,
 'uid005'

 FROM
 (

 SELECT
 DMP_UID AS DMP_ID,
 SITE_UID AS DSP_COOKIE_ID,
 ROW_NUMBER() OVER(PARTITION BY DMP_UID ORDER BY LOG_TIME DESC ) AS RN

 FROM
 DMP.LOG_SERVER_IDSYNC_PIXEL
 WHERE
 (PART_HOUR BETWEEN '${hivevar:dataxu_dt_from}00' AND '${hivevar:dataxu_dt_to}24')
 AND
 (DMP_UID IS NOT NULL AND DMP_UID <> '' AND SUBSTR(DMP_UID,1,6) = '(DMPC)' AND DMP_UID <> '(DMPC)00000000-0000-0000-0000-000000000000')
 AND
 (SITE_UID IS NOT NULL AND SITE_UID <> '')
 AND
 NID = '5'

 ) a

 WHERE a.RN = 1

;


