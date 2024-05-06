USE svc_ds_dmp;

DROP TABLE IF EXISTS svc_ds_dmp.bak_recommend_${oozie_job_name};

CREATE TABLE IF NOT EXISTS svc_recommend_${oozie_job_name} AS SELECT * FROM tmp_recommend_${oozie_job_name} WHERE 1 = 0;

ALTER TABLE svc_recommend_${oozie_job_name} RENAME TO bak_recommend_${oozie_job_name};
ALTER TABLE tmp_recommend_${oozie_job_name} RENAME TO svc_recommend_${oozie_job_name};

