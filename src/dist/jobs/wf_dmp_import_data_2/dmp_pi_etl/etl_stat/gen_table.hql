create table if not exists dmp_pi.etl_stat
(
  etl_name  string,
  dt        string,
  stat      string,
  elaps     string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
location 'hdfs://skpds/user/dmp_pi/dmp_pi_etl_stat'
;

