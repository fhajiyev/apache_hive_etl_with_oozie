use user_tadsvc;


set mapreduce.job.queuename=COMMON;


set hivevar:from_dt;
set hivevar:from_ts_out;
set hivevar:to_dt;
set hivevar:to_ts_out;






--while generating logs we should use previous features so move them into other table so that they do not mix up with current features

DROP TABLE IF EXISTS user_fariz_stddev_users_to_features_log;

ALTER TABLE user_fariz_stddev_users_to_features RENAME TO user_fariz_stddev_users_to_features_log;



--for 9466

drop table if exists user_fariz_stddev_users_to_ctr_9466;

drop table if exists user_fariz_stddev_regr_input_9466;

create table user_fariz_stddev_users_to_ctr_9466
(

   d_uid string,
   ratio double

);


create table user_fariz_stddev_regr_input_9466
(

   ratio          double,
   avrg_cps       double,
   avrg_cts       double,
   avrg_mclientid double,
   avrg_mds       double,
   avrg_tgs       double

)
row format delimited fields terminated by '\t'
location '/user/tadsvc/regression_9466';





--IMPORTANT!!!
--the following two queries should be invoked only if we plan to do regression
--otherwise we may skip them to save time



--for 9466

INSERT INTO TABLE user_fariz_stddev_users_to_ctr_9466
SELECT a.d_uid, a.ratio
FROM
(

SELECT d_uid,
       (SUM(CASE WHEN k_event >= '200' AND k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN k_event = '0' THEN 1 ELSE 0 END)) as ratio
    

FROM tad.log_server_event
WHERE
      (
       ( part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}' )
       AND
       ( log_time >= '${hivevar:from_ts_out}' AND log_time <= '${hivevar:to_ts_out}' )
      )
      AND
      ((adt = '0')OR(adt = '1'))
      AND
      ((x_products IS NOT NULL)AND(x_products <> ''))
      AND
      (ads = '9466')

GROUP BY d_uid HAVING SUM(CASE WHEN k_event = '0' THEN 1 ELSE 0 END) <> 0

) a

;



--for 9120

--INSERT INTO TABLE user_fariz_stddev_users_to_ctr_9120 SELECT a.d_uid, a.ratio FROM (SELECT d_uid, (SUM(CASE WHEN k_event >= '200' AND k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN k_event = '0' THEN 1 ELSE 0 END)) as ratio FROM tad.log_server_event WHERE (( part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}' ) AND ( log_time >= '${hivevar:from_ts_out}' AND log_time <= '${hivevar:to_ts_out}' )) AND ((adt = '0')OR(adt = '1')) AND ((x_products IS NOT NULL)AND(x_products <> '')) AND (ads = '9120') GROUP BY d_uid HAVING SUM(CASE WHEN k_event = '0' THEN 1 ELSE 0 END) <> 0) a













INSERT INTO TABLE user_fariz_stddev_regr_input_9466
SELECT b.ratio, a.avrg_cps, a.avrg_cts, a.avrg_mclientid, a.avrg_mds, a.avrg_tgs
FROM
user_fariz_stddev_users_to_ctr_9466 b
JOIN
user_fariz_stddev_users_to_features_log a
ON
b.d_uid = a.d_uid;




--INSERT INTO TABLE user_fariz_stddev_regr_input_9120 SELECT b.ratio, a.avrg_cps, a.avrg_cts, a.avrg_mclientid, a.avrg_mds, a.avrg_tgs FROM user_fariz_stddev_users_to_ctr_9120 b JOIN user_fariz_stddev_users_to_features a ON b.d_uid = a.d_uid







