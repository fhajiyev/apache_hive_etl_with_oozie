USE SVC_DS_DMP;

set hivevar:today_dt;
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

DROP TABLE IF EXISTS SVC_DS_DMP.prod_user_segment_map_yestd;

ALTER TABLE prod_user_segment_map_today RENAME TO prod_user_segment_map_yestd;

CREATE TABLE SVC_DS_DMP.PROD_USER_SEGMENT_MAP_TODAY
(
DMP_ID STRING,
SKP_SEG_ID ARRAY<STRING>
)
CLUSTERED BY (dmp_id)
SORTED BY (dmp_id ASC, skp_seg_id ASC)
INTO 256 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ':'
COLLECTION ITEMS TERMINATED BY ','
LINES TERMINATED BY '\n'
;




-- today

INSERT INTO TABLE SVC_DS_DMP.PROD_USER_SEGMENT_MAP_TODAY

SELECT
   a.uid,
   a.seg_ids

FROM
   SVC_DS_DMP.PROD_SEG_USER_VIEW a

WHERE

   (a.uid <> '' AND INSTR(a.uid, ',') <= 0 AND INSTR(a.uid, '\;') <= 0)
   AND
   a.uid not in ('(DMPC)00000000-0000-0000-0000-000000000000','(GAID)00000000-0000-0000-0000-000000000000','(IDFA)00000000-0000-0000-0000-000000000000')


;


