

USE SVC_DS_DMP;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hivevar:today_dt;

INSERT INTO TABLE SVC_DS_DMP.PROD_DMP_ID_MATCHING partition(ID_TYPE)



 SELECT

 a.UID AS DMP_ID,
 SUBSTR(a.UID, 7) AS DSP_COOKIE_ID,
 CASE
 WHEN SUBSTR(a.UID, 1, 6)='(GAID)' THEN 'gaid'
 WHEN SUBSTR(a.UID, 1, 6)='(IDFA)' THEN 'idfa'
 END AS ID_TYPE

 FROM
 (

 SELECT DISTINCT UID
 FROM
 SVC_DS_DMP.PROD_SEG_USER_VIEW

 WHERE
 (UID IS NOT NULL AND LENGTH(UID)=42 AND SUBSTR(UID,1,6) IN ('(GAID)','(IDFA)') AND UID <> '(GAID)00000000-0000-0000-0000-000000000000' AND UID <> '(IDFA)00000000-0000-0000-0000-000000000000')
 AND (INSTR(uid, ',') <= 0 AND INSTR(uid, '\;') <= 0 AND INSTR(UID, ' ') <= 0)

 )
 a
;


