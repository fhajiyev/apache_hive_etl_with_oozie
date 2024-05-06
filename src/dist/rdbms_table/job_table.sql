drop table dmp_service.dmp_job;

create table dmp_service.dmp_job
(
  dmp_job_id int unsigned not null AUTO_INCREMENT,
  dmp_job_type varchar(100) not null,
  oozie_job_name varchar(255) not null,

  dmp_site_id varchar(200) not null,
  dmp_data_source_id varchar(200) not null,

  dmp_job_description varchar(500) default null,

  create_uid varchar(50) default null,
  update_uid varchar(50) default null,
  create_dt datetime default null,
  update_dt datetime default null,

  primary key (dmp_job_id),
  unique index dmp_job_udx1 (dmp_job_id, dmp_job_type),
  index dmp_job_idx1(oozie_job_name)
);

drop table dmp_service.dmp_job_parameter;

create table dmp_service.dmp_job_parameter
(
  dmp_job_id int,

  param_name varchar(200),
  param_value varchar(4000),
  create_uid varchar(50) default null,
  update_uid varchar(50) default null,
  create_dt datetime default null,
  update_dt datetime default null,

  unique index dmp_job_parameter_udx1 (dmp_job_id, param_name)
);

select max(case when param_name = 'db_name' then param_value else null end) as "db_name",
       max(case when param_name = 'table_name' then param_value else null end) as "table_name"
from dmp_service.dmp_job_parameter
where job_id = 1;

insert into dmp_service.dmp_job (dmp_job_type, oozie_job_name, dmp_site_id, dmp_data_source_id) values ('import', '1daily_job_for_import_syrupad_dmp', 'syrupad', 'service_activity_log');

insert into dmp_service.dmp_job_parameter (dmp_job_id, param_name, param_value) values (1, 'db_name', 'dmp');
insert into dmp_service.dmp_job_parameter (dmp_job_id, param_name, param_value) values (1, 'table_name', 'data_store');

insert into dmp_service.dmp_job (dmp_job_type, oozie_job_name, dmp_site_id, dmp_data_source_id) values ('import', '1hourly_job_for_syrupad_retargeting', 'syrupad', 'service_activity_log');
insert into dmp_service.dmp_job_parameter (dmp_job_id, param_name, param_value) values (2, '--similarityClassname', 'SIMILARITY_LOGLIKELIHOOD');
insert into dmp_service.dmp_job_parameter (dmp_job_id, param_name, param_value) values (2, '--numRecommendations', '100');
insert into dmp_service.dmp_job_parameter (dmp_job_id, param_name, param_value) values (2, 'mapreduce.job.reduces', '128');

select * from dmp_job_parameter;

update dmp_job_parameter
set param_value = '1'
where dmp_job_id = 2 and param_name = 'window_size';

commit;

insert into dmp_service.dmp_job_parameter (dmp_job_id, param_name, param_value) values (2, 'window_size', '7');
insert into dmp_service.dmp_job_parameter (dmp_job_id, param_name, param_value) values (2, 'preference', 'ROUND
 (
       SUM(
           CASE WHEN a.action = ''view'' THEN 1 WHEN a.action = ''basket'' THEN 5 WHEN a.action IN (''order'', ''orderSKP'') THEN 10 ELSE 0 END
           *
           1/(UNIX_TIMESTAMP() - UNIX_TIMESTAMP(a.log_time, ''yyyy/MM/dd HH:mm:ss'') + 1)
       )
 , 6)');

select *
from dmp_job_parameter
where dmp_job_id = 2;

SELECT
  MAX(CASE WHEN param_name = 'mapreduce.job.reduces' THEN param_value ELSE null END) AS "mapreduce.job.reduces",
  MAX(CASE WHEN param_name = '--similarityClassname' THEN param_value ELSE null END) AS "--similarityClassname",
  MAX(CASE WHEN param_name = '--numRecommendations' THEN param_value ELSE null END) AS "--numRecommendations"
FROM dmp_service.dmp_job_parameter
WHERE dmp_job_id = 2;

SELECT  MAX(CASE WHEN param_name = 'mapreduce.job.reduces' THEN param_value ELSE null END) AS "mapreduce.job.reduces",
        MAX(CASE WHEN param_name = '--similarityClassname' THEN param_value ELSE null END) AS "--similarityClassname",
        MAX(CASE WHEN param_name = '--numRecommendations' THEN param_value ELSE null END) AS "--numRecommendations",
        DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -1*MAX(CASE WHEN param_name = 'window_size' THEN CAST(param_value AS DECIMAL) ELSE 1 END)  DAY), '%Y%m%d%H') AS "from_dt",
        MAX(CASE WHEN param_name = 'preference' THEN param_value ELSE null END) AS "preference"
FROM  dmp_service.dmp_job_parameter
WHERE  dmp_job_id = 2;

SELECT cast(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL -2 DAY), '%Y%m%d%H') as int) AS now_time;


select * from dmp_parameter;

delete from dmp_parameter where param_name in ('mapreduce.job.reduces', '128');
