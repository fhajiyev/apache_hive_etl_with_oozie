use user_tadsvc;


set mapreduce.job.queuename=COMMON;


set hivevar:from_dt;
set hivevar:from_ts;
set hivevar:to_dt;
set hivevar:to_ts;






--common 

drop table if exists user_fariz_stddev_users_to_features;

create table user_fariz_stddev_users_to_features
(

   d_uid          string,
   avrg_cps       double,
   avrg_cts       double,
   avrg_mclientid double,
   avrg_mds       double,
   avrg_tgs       double

);




insert into table user_fariz_stddev_users_to_features select d1.d_uid, d1.avrg_cps, d2.avrg_cts, d3.avrg_mclientid, d4.avrg_mds, d5.avrg_tgs

from


(

select c.d_uid, (SUM(c.ratio)/COUNT(c.ratio)) as avrg_cps

from
(

select a.d_uid, b.ratio
from
tad.log_server_event a
join
user_fariz_stddev_ctr_attr_cps b
on
a.cps = b.cps
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )
      AND
      ((a.adt = '0')OR(a.adt = '1'))
      AND
      ((a.x_products IS NOT NULL)AND(a.x_products <> ''))
      AND
      ((ads = '9969')OR(ads = '9971')OR(ads = '10022')OR(ads = '10024'))
      AND
      b.ratio >= 0 AND b.ratio <= 1
      
) c
group by c.d_uid

) d1


join


(

select c.d_uid, SUM(c.ratio)/COUNT(c.ratio) as avrg_cts

from
(

select a.d_uid, b.ratio
from
tad.log_server_event a
join
user_fariz_stddev_ctr_attr_cts b
on
a.cts = b.cts
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )
      AND
      ((a.adt = '0')OR(a.adt = '1'))      
      AND
      ((a.x_products IS NOT NULL)AND(a.x_products <> ''))
      AND
      ((ads = '9969')OR(ads = '9971')OR(ads = '10022')OR(ads = '10024')) 
      AND
      b.ratio >= 0 AND b.ratio <= 1

) c
group by c.d_uid

) d2

on d1.d_uid = d2.d_uid
join


(

select c.d_uid, SUM(c.ratio)/COUNT(c.ratio) as avrg_mclientid

from
(

select a.d_uid, b.ratio
from
tad.log_server_event a
join
user_fariz_stddev_ctr_attr_mclientid b
on
a.m_client_id = b.mclientid
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )
      AND
      ((a.adt = '0')OR(a.adt = '1'))
      AND
      ((a.x_products IS NOT NULL)AND(a.x_products <> ''))
      AND
      ((ads = '9969')OR(ads = '9971')OR(ads = '10022')OR(ads = '10024'))
      AND
      b.ratio >= 0 AND b.ratio <= 1

) c
group by c.d_uid

) d3

on d2.d_uid = d3.d_uid
join


(

select c.d_uid, SUM(c.ratio)/COUNT(c.ratio) as avrg_mds

from
(

select a.d_uid, b.ratio
from
tad.log_server_event a
join
user_fariz_stddev_ctr_attr_mds b
on
a.mds = b.mds
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )
      AND
      ((a.adt = '0')OR(a.adt = '1'))
      AND
      ((a.x_products IS NOT NULL)AND(a.x_products <> ''))
      AND
      ((ads = '9969')OR(ads = '9971')OR(ads = '10022')OR(ads = '10024'))
      AND
      b.ratio >= 0 AND b.ratio <= 1

) c
group by c.d_uid

) d4

on d3.d_uid = d4.d_uid
join


(

select c.d_uid, SUM(c.ratio)/COUNT(c.ratio) as avrg_tgs

from
(

select a.d_uid, b.ratio
from
tad.log_server_event a
join
user_fariz_stddev_ctr_attr_tgs b
on
a.tgs = b.tgs
WHERE (
        ( a.part_date >= '${hivevar:from_dt}' AND a.part_date <= '${hivevar:to_dt}' )
        AND
        ( a.log_time >= '${hivevar:from_ts}' AND a.log_time <= '${hivevar:to_ts}' )
      )
      AND
      ((a.adt = '0')OR(a.adt = '1')) 
      AND
      ((a.x_products IS NOT NULL)AND(a.x_products <> '')) 
      AND
      ((ads = '9969')OR(ads = '9971')OR(ads = '10022')OR(ads = '10024'))
      AND
      b.ratio >= 0 AND b.ratio <= 1

) c
group by c.d_uid

) d5

on d4.d_uid = d5.d_uid;





--





