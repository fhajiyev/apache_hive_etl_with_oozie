#!/bin/sh

IMPALA_CLIENT=/app/di/qcshell-pnet/bin/qcshell


oozie_job_name=$1

$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "DROP VIEW IF EXISTS svc_ds_dmp.svc_syrupad_recommend_view_$oozie_job_name";
$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "CREATE VIEW svc_ds_dmp.svc_syrupad_recommend_view_$oozie_job_name AS
SELECT d_uid, m_slot, client_type,
REGEXP_REPLACE(GROUP_CONCAT(level1), ' ', '') as level1,
REGEXP_REPLACE(GROUP_CONCAT(level2), ' ', '') as level2,
REGEXP_REPLACE(GROUP_CONCAT(level3), ' ', '') as level3,
REGEXP_REPLACE(GROUP_CONCAT(level4), ' ', '') as level4,
REGEXP_REPLACE(GROUP_CONCAT(level5), ' ', '') as level5
FROM (
  SELECT d_uid, m_slot, client_type,
                 CASE WHEN MAX(score) >= 0.25 THEN ads ELSE NULL END AS level1,
                                    CASE WHEN MAX(score) >= 0.2 AND MAX(score) < 0.25 THEN ads ELSE NULL END AS level2,
                                                       CASE WHEN MAX(score) >= 0.15 AND MAX(score) < 0.2 THEN ads ELSE NULL END AS level3,
                                                                          CASE WHEN MAX(score) >= 0.1 AND MAX(score) < 0.15 THEN ads ELSE NULL END AS level4,
                                                                                             CASE WHEN MAX(score) < 0.1 THEN ads ELSE NULL END AS level5
                                                                                                      FROM svc_ds_dmp.svc_syrupad_recommend_$oozie_job_name
                                                                                                          GROUP BY d_uid, m_slot, client_type, ads
                                                                                                          ) a
                                                                                                          GROUP BY d_uid, m_slot, client_type";





$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "DROP VIEW IF EXISTS svc_ds_dmp.svc_syrupad_recommend_score_view_$oozie_job_name";
$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "CREATE VIEW svc_ds_dmp.svc_syrupad_recommend_score_view_$oozie_job_name AS
SELECT d_uid, m_slot, client_type,
REGEXP_REPLACE(GROUP_CONCAT(level1), ' ', '') as level1,
REGEXP_REPLACE(GROUP_CONCAT(level2), ' ', '') as level2,
REGEXP_REPLACE(GROUP_CONCAT(level3), ' ', '') as level3,
REGEXP_REPLACE(GROUP_CONCAT(level4), ' ', '') as level4,
REGEXP_REPLACE(GROUP_CONCAT(level5), ' ', '') as level5
FROM (
  SELECT d_uid, m_slot, client_type,
                 CASE WHEN score >= 0.25 THEN CONCAT(ads, ':', CAST(ROUND(score, 20) AS STRING)) ELSE NULL END AS level1,
                                    CASE WHEN score >= 0.2 AND score < 0.25 THEN CONCAT(ads, ':', CAST(ROUND(score, 20) AS STRING)) ELSE NULL END AS level2,
                                                       CASE WHEN score >= 0.15 AND score < 0.2 THEN CONCAT(ads, ':', CAST(ROUND(score, 20) AS STRING)) ELSE NULL END AS level3,
                                                                          CASE WHEN score >= 0.1 AND score < 0.15 THEN CONCAT(ads, ':', CAST(ROUND(score, 20) AS STRING)) ELSE NULL END AS level4,
                                                                                             CASE WHEN score < 0.1 THEN CONCAT(ads, ':', CAST(ROUND(score, 20) AS STRING)) ELSE NULL END AS level5
                                                                                                      FROM svc_ds_dmp.svc_syrupad_recommend_$oozie_job_name
                                                                                                      ) a
                                                                                                      GROUP BY d_uid, m_slot, client_type";




$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "DROP VIEW IF EXISTS svc_ds_dmp.svc_event_log_view_$oozie_job_name";
$IMPALA_CLIENT -b daas-impala -n PS03721 -p Pe958319 -e "CREATE VIEW svc_ds_dmp.svc_event_log_view_$oozie_job_name AS
SELECT d_uid, m_slot, client_type,
REGEXP_REPLACE(GROUP_CONCAT(level1), ' ', '') as level1,
REGEXP_REPLACE(GROUP_CONCAT(level2), ' ', '') as level2,
REGEXP_REPLACE(GROUP_CONCAT(level3), ' ', '') as level3,
REGEXP_REPLACE(GROUP_CONCAT(level4), ' ', '') as level4,
REGEXP_REPLACE(GROUP_CONCAT(level5), ' ', '') as level5
FROM (
  SELECT d_uid, m_slot, client_type,
                 CASE WHEN score >= 3.9139 THEN ads ELSE NULL END AS level1,
                                    CASE WHEN score >= 1.4239 AND score < 3.9139 THEN ads ELSE NULL END AS level2,
                                                       CASE WHEN score >= 0.8009 AND score < 1.4239 THEN ads ELSE NULL END AS level3,
                                                                          CASE WHEN score >= 0.5239 AND score < 0.8009 THEN ads ELSE NULL END AS level4,
                                                                                             CASE WHEN score < 0.5239 THEN ads ELSE NULL END AS level5
                                                                                                      FROM svc_ds_dmp.svc_event_log_$oozie_job_name
                                                                                                      ) a
                                                                                                      GROUP BY d_uid, m_slot, client_type";


