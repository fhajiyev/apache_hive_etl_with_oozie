use user_tadsvc;


set tez.queue.name=COMMON;
set mapreduce.job.queuename=COMMON;
set hive.execution.engine=tez;


set hivevar:term;
set hivevar:from_dt;
set hivevar:to_dt;
set hivevar:from_ts_out;
set hivevar:to_ts_out;




DROP TABLE IF EXISTS user_fariz_stddev_users_to_ctr_legacy;
DROP TABLE IF EXISTS user_fariz_stddev_users_to_requests;







insert into table user_fariz_stddev_compare_exp_real_11229 select '${hivevar:to_ts_out}', '${hivevar:term}', a.requests, b.impressions, b.clicks, b.real, 0.0, d.legacy, e.audience, 0.0, g1.distinct_impusers, g2.distinct_clkusers from (SELECT SUM(CASE WHEN k_event = '1000' THEN 1 ELSE 0 END) as requests FROM tad.log_server_request WHERE (( part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}' ) AND ( log_time >= '${hivevar:from_ts_out}' AND log_time <= '${hivevar:to_ts_out}' )) AND ((adt = '0')OR(adt = '1')) AND ((x_products IS NOT NULL)AND(x_products <> '')) AND (ads = '11229')) a


join (SELECT (SUM(CASE WHEN k_event >= '200' AND k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN k_event = '0' THEN 1 ELSE 0 END))*100 as real, SUM(CASE WHEN k_event = '0' THEN 1 ELSE 0 END) as impressions, SUM(CASE WHEN k_event >= '200' AND k_event <= '999' THEN 1 ELSE 0 END) as clicks FROM tad.log_server_event WHERE (( part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}' ) AND ( log_time >= '${hivevar:from_ts_out}' AND log_time <= '${hivevar:to_ts_out}' )) AND ((adt = '0')OR(adt = '1')) AND ((x_products IS NOT NULL)AND(x_products <> '')) AND (ads = '11229')) b


--join(select (sum(z.exp)/count(z.exp))*100 as expected from (select ((x.avrg_cps * y.coeff_cps) + (x.avrg_cts * y.coeff_cts) + (x.avrg_mclientid * y.coeff_mclientid) + (x.avrg_mds * y.coeff_mds) + (x.avrg_tgs * y.coeff_tgs) + y.bias) as exp from user_fariz_stddev_users_to_features x join user_fariz_stddev_regr_coeffs_9120 y) z) c1


join (SELECT (SUM(CASE WHEN k_event >= '200' AND k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN k_event = '0' THEN 1 ELSE 0 END))*100 as legacy FROM tad.log_server_event WHERE (( part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}' ) AND ( log_time >= '${hivevar:from_ts_out}' AND log_time <= '${hivevar:to_ts_out}' )) AND ((adt = '0')OR(adt = '1')) AND ((x_products IS NULL)OR((x_products IS NOT NULL)AND(x_products = ''))) AND (ads = '11229')) d


join (SELECT count(distinct d_uid) as audience FROM svc_csw1.svc_syrupad_recommend WHERE ads = '11229') e


--join(select SQRT(SUM(POW(w.exp-w.ratio, 2))) as error from (select ((x.avrg_cps * y.coeff_cps) + (x.avrg_cts * y.coeff_cts) + (x.avrg_mclientid * y.coeff_mclientid) + (x.avrg_mds * y.coeff_mds) + (x.avrg_tgs * y.coeff_tgs) + y.bias) as exp, z.ratio from user_fariz_stddev_users_to_features x join user_fariz_stddev_regr_coeffs_9120 y join user_fariz_stddev_users_to_ctr_9120 z on x.d_uid = z.d_uid) w) f1


join (SELECT count(distinct d_uid)  as distinct_impusers FROM tad.log_server_event WHERE (( part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}' ) AND ( log_time >= '${hivevar:from_ts_out}' AND log_time <= '${hivevar:to_ts_out}' )) AND (k_event = '0') AND ((adt = '0')OR(adt = '1')) AND ((x_products IS NOT NULL)AND(x_products <> '')) AND (ads = '11229')) g1


join (SELECT count(distinct d_uid)  as distinct_clkusers FROM tad.log_server_event WHERE (( part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}' ) AND ( log_time >= '${hivevar:from_ts_out}' AND log_time <= '${hivevar:to_ts_out}' )) AND (( k_event >= '200') AND (k_event <= '999')) AND ((adt = '0')OR(adt = '1')) AND ((x_products IS NOT NULL)AND(x_products <> '')) AND (ads = '11229')) g2





;






