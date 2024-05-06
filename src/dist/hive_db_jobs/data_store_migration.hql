alter table data_store rename to data_store_source;
alter table data_store_without_bucket rename to data_store;

set hive.exec.max.dynamic.partitions=2000000;
set hive.exec.dynamic.partition.mode=true;

insert into dmp.data_store partition (dmp_site_id, dmp_data_source_id, dmp_part_date)
select
dmp_uid   ,
  site_uid   ,

  col001    ,
  col002    ,
  col003    ,
  col004    ,
  col005    ,
  col006    ,
  col007    ,
  col008    ,
  col009    ,
  col010    ,

  col011    ,
  col012    ,
  col013    ,
  col014    ,
  col015    ,
  col016    ,
  col017    ,
  col018    ,
  col019    ,
  col020    ,

  col021    ,
  col022    ,
  col023    ,
  col024    ,
  col025    ,
  col026    ,
  col027    ,
  col028    ,
  col029    ,
  col030    ,

  col031    ,
  col032    ,
  col033    ,
  col034    ,
  col035    ,
  col036    ,
  col037    ,
  col038    ,
  col039    ,
  col040    ,

  col041    ,
  col042    ,
  col043    ,
  col044    ,
  col045    ,
  col046    ,
  col047    ,
  col048    ,
  col049    ,
  col050    ,

  col051    ,
  col052    ,
  col053    ,
  col054    ,
  col055    ,
  col056    ,
  col057    ,
  col058    ,
  col059    ,
  col060    ,

  col061    ,
  col062    ,
  col063    ,
  col064    ,
  col065    ,
  col066    ,
  col067    ,
  col068    ,
  col069    ,
  col070    ,

  col071    ,
  col072    ,
  col073    ,
  col074    ,
  col075    ,
  col076    ,
  col077    ,
  col078    ,
  col079    ,
  col080    ,

  col081    ,
  col082    ,
  col083    ,
  col084    ,
  col085    ,
  col086    ,
  col087    ,
  col088    ,
  col089    ,
  col090    ,

  col091    ,
  col092    ,
  col093    ,
  col094    ,
  col095    ,
  col096    ,
  col097    ,
  col098    ,
  col099    ,
  col100    ,
dmp_site_id , dmp_data_source_id , dmp_part_date
from data_store_source;


drop table dmp.data_store_without_bucket;

CREATE TABLE IF NOT EXISTS dmp.data_store_without_bucket
(
  dmp_uid   string,
  site_uid   string,

  col001    string,
  col002    string,
  col003    string,
  col004    string,
  col005    string,
  col006    string,
  col007    string,
  col008    string,
  col009    string,
  col010    string,

  col011    string,
  col012    string,
  col013    string,
  col014    string,
  col015    string,
  col016    string,
  col017    string,
  col018    string,
  col019    string,
  col020    string,

  col021    string,
  col022    string,
  col023    string,
  col024    string,
  col025    string,
  col026    string,
  col027    string,
  col028    string,
  col029    string,
  col030    string,

  col031    string,
  col032    string,
  col033    string,
  col034    string,
  col035    string,
  col036    string,
  col037    string,
  col038    string,
  col039    string,
  col040    string,

  col041    string,
  col042    string,
  col043    string,
  col044    string,
  col045    string,
  col046    string,
  col047    string,
  col048    string,
  col049    string,
  col050    string,

  col051    string,
  col052    string,
  col053    string,
  col054    string,
  col055    string,
  col056    string,
  col057    string,
  col058    string,
  col059    string,
  col060    string,

  col061    string,
  col062    string,
  col063    string,
  col064    string,
  col065    string,
  col066    string,
  col067    string,
  col068    string,
  col069    string,
  col070    string,

  col071    string,
  col072    string,
  col073    string,
  col074    string,
  col075    string,
  col076    string,
  col077    string,
  col078    string,
  col079    string,
  col080    string,

  col081    string,
  col082    string,
  col083    string,
  col084    string,
  col085    string,
  col086    string,
  col087    string,
  col088    string,
  col089    string,
  col090    string,

  col091    string,
  col092    string,
  col093    string,
  col094    string,
  col095    string,
  col096    string,
  col097    string,
  col098    string,
  col099    string,
  col100    string
)
PARTITIONED BY (dmp_site_id string, dmp_data_source_id string, dmp_part_date string);