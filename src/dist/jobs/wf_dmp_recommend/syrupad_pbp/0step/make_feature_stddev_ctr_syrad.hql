USE user_tadsvc;


set mapreduce.job.queuename=COMMON;


--for 9466

DROP TABLE IF EXISTS user_fariz_stddev_regr_coeffs_9466;


DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_cps;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_ads;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_adt;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_blt;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_cts;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_tgs;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_mds;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_ccb;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_ctc;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_mclientid;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_mslot;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_duid;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_dosname;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_dosver;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_dmodel;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_msdkver;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_attr_dsales;

DROP TABLE IF EXISTS user_fariz_stddev_users_to_ctr_all;
DROP TABLE IF EXISTS user_fariz_stddev_ctr_activeusers;



--for 9466

CREATE EXTERNAL TABLE user_fariz_stddev_regr_coeffs_9466
(

   coeff_cps double,
   coeff_cts double,
   coeff_mclientid double,
   coeff_mds double,
   coeff_tgs double,
   bias double

)
row format delimited fields terminated by '\t'
location '/user/tadsvc/regression_weights_9466';









CREATE TABLE user_fariz_stddev_ctr_attr_cps
(

   cps string,
   ratio double

);

CREATE TABLE user_fariz_stddev_ctr_attr_cts
(

   cts string,
   ratio double

);

CREATE TABLE user_fariz_stddev_ctr_attr_tgs
(

   tgs string,
   ratio double

);

CREATE TABLE user_fariz_stddev_ctr_attr_mds
(

   mds string,
   ratio double

);

CREATE TABLE user_fariz_stddev_ctr_attr_mclientid
(

   mclientid string,
   ratio double

);




set hivevar:from_dt;
set hivevar:from_ts;
set hivevar:to_dt;
set hivevar:to_ts;




INSERT OVERWRITE TABLE user_fariz_stddev_ctr_attr_cps
SELECT
a.cps,
(
    SUM(CASE WHEN a.k_event >= '200' AND a.k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN a.k_event = '0' THEN 1 ELSE 0 END)
)
AS ratio
FROM tad.log_server_event a
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )
      
      AND a.cps IS NOT NULL AND a.cps <> ''
GROUP BY a.cps;



INSERT OVERWRITE TABLE user_fariz_stddev_ctr_attr_cts
SELECT
a.cts,
(
    SUM(CASE WHEN a.k_event >= '200' AND a.k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN a.k_event = '0' THEN 1 ELSE 0 END)
)
AS ratio
FROM tad.log_server_event a
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )

      AND a.cts IS NOT NULL AND a.cts <> ''
GROUP BY a.cts;



INSERT OVERWRITE TABLE user_fariz_stddev_ctr_attr_tgs
SELECT
a.tgs,
(
    SUM(CASE WHEN a.k_event >= '200' AND a.k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN a.k_event = '0' THEN 1 ELSE 0 END)
)
AS ratio
FROM tad.log_server_event a
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )

      AND a.tgs IS NOT NULL AND a.tgs <> ''
GROUP BY a.tgs;



INSERT OVERWRITE TABLE user_fariz_stddev_ctr_attr_mds
SELECT
a.mds,
(
    SUM(CASE WHEN a.k_event >= '200' AND a.k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN a.k_event = '0' THEN 1 ELSE 0 END)
)
AS ratio
FROM tad.log_server_event a
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )

      AND a.mds IS NOT NULL AND a.mds <> ''
GROUP BY a.mds;



INSERT OVERWRITE TABLE user_fariz_stddev_ctr_attr_mclientid
SELECT
a.m_client_id,
(
    SUM(CASE WHEN a.k_event >= '200' AND a.k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN a.k_event = '0' THEN 1 ELSE 0 END)
)
AS ratio
FROM tad.log_server_event a
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )

      AND a.m_client_id IS NOT NULL AND a.m_client_id <> ''
GROUP BY a.m_client_id;






--for this table we should take users with above average ctr 
--instead of taking some fixed limit value



--INSERT INTO TABLE user_fariz_stddev_users_to_ctr_all
--SELECT a.d_uid, a.ratio
--FROM
--(
--
--SELECT d_uid,
--       (SUM(CASE WHEN k_event >= '200' AND k_event <= '999' THEN 1 ELSE 0 END)/SUM(CASE WHEN k_event = '0' THEN 1 ELSE 0 END)) as ratio
--
--FROM tad.log_event
--WHERE 
--      (
--        ( part_date >= '${hivevar:from_dt}' AND part_date <= '${hivevar:to_dt}' )
--        AND     
--        ( log_time >= '${hivevar:from_ts}' AND log_time <= '${hivevar:to_ts}' )
--      )
--
--GROUP BY d_uid
--
--) a
--
--WHERE a.ratio >= 0 AND a.ratio <= 1
--ORDER BY a.ratio DESC
--LIMIT 1000













--intersection for cps, cts, tgs, mds, mclientid (all 5 features) gives empty set (10 day window)

--intersection for cps, cts, tgs, mds                             gives empty set (10 day window)

--intersection for cts, tgs, mclientid                            gives empty set (10 day window)
--intersection for cps, cts, tgs                                  gives empty set (10 day window) 
--intersection for cps, cts                                       gives empty set (10 day window)

--intersection for cps, tgs                                       gives empty set (10 day window)
--intersection for cts, tgs                                       gives empty set (10 day window)




--only cps                                            gives 308902 records (10 day window)
--only cts                                            gives 922145 records (10 day window)
--only tgs                                            memory limit exceeded
--only mds                                            gives 5      records (10 day window)
--only mclientid                                      gives 15     records (10 day window)


--now we have all necessary tables to find out active users


--INSERT OVERWRITE TABLE user_fariz_stddev_ctr_activeusers
--SELECT
--a.d_uid
--FROM
--tad.log_event a

--JOIN
--(select b.cps from user_fariz_stddev_ctr_attr_cps b where b.ratio in (select max(c.ratio) from user_fariz_stddev_ctr_attr_cps c where c.ratio >= 0 and c.ratio <= 1)) d on a.cps = d.cps

--JOIN
--(select b.cts from user_fariz_stddev_ctr_attr_cts b where b.ratio in (select max(c.ratio) from user_fariz_stddev_ctr_attr_cts c where c.ratio >= 0 and c.ratio <= 1)) e on a.cts = e.cts

--JOIN
--(select b.tgs from user_fariz_stddev_ctr_attr_tgs b where b.ratio in (select max(c.ratio) from user_fariz_stddev_ctr_attr_tgs c where c.ratio >= 0 and c.ratio <= 1)) f on a.tgs = f.tgs

--JOIN
--(select b.mds from user_fariz_stddev_ctr_attr_mds b where b.ratio in (select max(c.ratio) from user_fariz_stddev_ctr_attr_mds c where c.ratio >= 0 and c.ratio <= 1)) g on a.mds = g.mds

--JOIN
--(select b.mclientid from user_fariz_stddev_ctr_attr_mclientid b where b.ratio in (select max(c.ratio) from user_fariz_stddev_ctr_attr_mclientid c where c.ratio >= 0 and c.ratio <= 1)) h on a.m_client_id = h.mclientid

--JOIN
--user_fariz_stddev_users_to_ctr_all i on a.d_uid = i.d_uid


--WHERE
--(
--   ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
--   AND
--   ( a.log_time >= '${hivevar:from_ts_out}' AND a.log_time <= '${hivevar:to_ts_out}' )
--)

--GROUP BY a.d_uid






























