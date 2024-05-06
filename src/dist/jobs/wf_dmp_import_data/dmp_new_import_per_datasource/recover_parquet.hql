
set hivevar:daycurr;
set hivevar:daybefore;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set hive.exec.max.dynamic.partitions=1000000;

insert overwrite table dmp.prod_data_source_store_parquet PARTITION (part_hour,data_source_id) select
uid,
  col001 ,  col002 ,  col003 ,  col004 ,  col005 ,  col006 ,  col007 ,  col008 ,  col009 ,  col010 ,
  col011 ,  col012 ,  col013 ,  col014 ,  col015 ,  col016 ,  col017 ,  col018 ,  col019 ,  col020 ,
  col021 ,  col022 ,  col023 ,  col024 ,  col025 ,  col026 ,  col027 ,  col028 ,  col029 ,  col030 ,
  col031 ,  col032 ,  col033 ,  col034 ,  col035 ,  col036 ,  col037 ,  col038 ,  col039 ,  col040 ,
  col041 ,  col042 ,  col043 ,  col044 ,  col045 ,  col046 ,  col047 ,  col048 ,  col049 ,  col050 ,
  col051 ,  col052 ,  col053 ,  col054 ,  col055 ,  col056 ,  col057 ,  col058 ,  col059 ,  col060 ,
  col061 ,  col062 ,  col063 ,  col064 ,  col065 ,  col066 ,  col067 ,  col068 ,  col069 ,  col070 ,
  col071 ,  col072 ,  col073 ,  col074 ,  col075 ,  col076 ,  col077 ,  col078 ,  col079 ,  col080 ,
  col081 ,  col082 ,  col083 ,  col084 ,  col085 ,  col086 ,  col087 ,  col088 ,  col089 ,  col090 ,
  col091 ,  col092 ,  col093 ,  col094 ,  col095 ,  col096 ,  col097 ,  col098 ,  col099 ,  col100 ,
  '${hivevar:daycurr}00',
  '${hivevar:daycurr}00',
  '${hivevar:daycurr}00',
  data_source_id
from dmp.prod_data_source_store where part_hour = '${hivevar:daybefore}00' and data_source_id NOT IN ('63','64','65','67','68','71','72','77','82','89','97','117','118','113','114','115','116','123','124','93','110','111','122');

