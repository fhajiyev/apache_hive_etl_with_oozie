create table if not exists dmp_pi.prod_uid_life (
    uid    string,
    begin  string,
    latest string,
    abgroup string,
    aged_days string,
    active_days    string
)
partitioned by (part_hour string);


