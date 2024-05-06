
USE SVC_DS_DMP;


set hivevar:today_dt;
set hivevar:dsp_id;

set mapreduce.job.reduces=128;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


INSERT INTO TABLE SVC_DS_DMP.DMP_COMMON_LOGGING_TABLE_TEMP partition(DSP_ID)

SELECT

'${hivevar:today_dt}',
a.SEG_ID,
a.USER_TYPE,
a.DELTA_TYPE,
COUNT(a.DMP_ID),
'${hivevar:dsp_id}' AS DSP_ID

FROM

(

  SELECT

  DMP_ID,
  SKP_SEG_ID AS SEG_ID,
  DELTA_TYPE,
  USER_TYPE

  FROM

  SVC_DS_DMP.DMP_UPLOAD_DATASET_TEMP

  WHERE DSP_ID='${hivevar:dsp_id}'

)
a

WHERE LENGTH(a.USER_TYPE) <= 6

GROUP BY a.SEG_ID, a.USER_TYPE, a.DELTA_TYPE;

