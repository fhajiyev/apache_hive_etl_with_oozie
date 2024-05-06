#!/bin/bash
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_etl/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
#    printf "etl_solution\t$CTIME\t$1\t$SECONDS" > proc_$CTIME.log
#    $HDD fs -put proc_$CTIME.log hdfs://skpds/user/dmp_pi/dmp_pi_etl_stat/
}

die() {
    etl_stat 'FAIL';
    echo >&2 -e "\nERROR: $@\n"; exit 1;
}
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }
print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }
print_sstr() { printf "\e[93m$@\e[0m\n"; }


LAST_JOB_DAY=`expr 2 + $1`
LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`


DROP_DAYS=`expr 92 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;
set hive.mapjoin.optimized.hashtable=false;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='13')




select 

c.uid,
c.dt,
c.entertime,
c.exittime,
c.weekday,
c.tagname,
c.dwelltime,


REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
 (REGEXP_REPLACE
  (REGEXP_REPLACE
   (REGEXP_REPLACE
    (REGEXP_REPLACE
     (REGEXP_REPLACE
      (REGEXP_REPLACE(c.sido,  '인천광역시','인천'),
                            '울산광역시','울산'),
                         '세종특별자치시','세종'),
                      '서울특별시','서울'),
                   '부산광역시','부산'),
                '대전광역시','대전'),
 '대구광역시','대구'),
 '광주광역시','광주'),
 '전라남도','전남'),
 '전라북도','전북'),
 '제주특별자치도','제주'),
 '충청남도','충남'),
 '충청북도','충북'),

 '강원도','강원'),
 '경기도','경기'),
 '경상남도','경남'),
 '경상북도','경북'),



REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE	      
(REGEXP_REPLACE	      
(REGEXP_REPLACE	      
(REGEXP_REPLACE	      
(REGEXP_REPLACE
(REGEXP_REPLACE
 (REGEXP_REPLACE
  (REGEXP_REPLACE
   (REGEXP_REPLACE
    (REGEXP_REPLACE
     (REGEXP_REPLACE
      (REGEXP_REPLACE(c.sigungu, '인천광역시','인천'),
                            '울산광역시','울산'),
                         '세종특별자치시','세종'),
                      '서울특별시','서울'),
                   '부산광역시','부산'),
                '대전광역시','대전'),
 '대구광역시','대구'),
 '광주광역시','광주'),
 '전라남도','전남'),
 '전라북도','전북'),
 '제주특별자치도','제주'),
 '충청남도','충남'),
 '충청북도','충북'),
 
 '강원도','강원'),
 '경기도','경기'),
 '경상남도','경남'),
 '경상북도','경북'),
 
REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
(REGEXP_REPLACE
 (REGEXP_REPLACE
  (REGEXP_REPLACE
   (REGEXP_REPLACE
    (REGEXP_REPLACE
     (REGEXP_REPLACE
      (REGEXP_REPLACE(c.dong, '인천광역시','인천'),
                            '울산광역시','울산'),
                         '세종특별자치시','세종'),
                      '서울특별시','서울'),
                   '부산광역시','부산'),
                '대전광역시','대전'),
 '대구광역시','대구'),
 '광주광역시','광주'),
 '전라남도','전남'),
 '전라북도','전북'),
 '제주특별자치도','제주'),
 '충청남도','충남'),
 '충청북도','충북'),

 '강원도','강원'),
 '경기도','경기'),
 '경상남도','경남'),
 '경상북도','경북'),



'',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','','',
'','','','','','','','','',''


from

(


select distinct 
a.ci as uid, 
a.part_dt as dt,
substr(a.entertime, 9,2) as entertime,
substr(a.exittime,  9,2) as exittime,
case from_unixtime(unix_timestamp(a.part_dt, 'yyyyMMdd'), 'u')
      when 1 then 'mon'
      when 2 then 'tue'
      when 3 then 'wed'
      when 4 then 'thu'
      when 5 then 'fri'
      when 6 then 'sat'
      else 'sun'
end as weekday,
b.tag_name as tagname,
cast( ( UNIX_TIMESTAMP(a.exittime, 'yyyyMMddHHmmss')-UNIX_TIMESTAMP(a.entertime, 'yyyyMMddHHmmss') ) / 60 as int ) as dwelltime,
case when size(b.addr_1)>0 then b.addr_1[0] else '' end as sido,
case when size(b.addr_1)>0 and size(b.addr_2)>0                      and b.addr_1[0] <> '' and b.addr_2[0] <> ''                       then concat(b.addr_1[0], ' ', b.addr_2[0]) else '' end as sigungu,
case when size(b.addr_1)>0 and size(b.addr_2)>0 and size(b.addr_3)>0 and b.addr_1[0] <> '' and b.addr_2[0] <> '' and b.addr_3[0] <> '' then concat_ws(' ', b.addr_1[0], b.addr_2[0], b.addr_3[0]) else '' end as dong

from
svc_cim.prod_daily_visit a 
join 
svc_cim.meta_tag_address_info b 
on a.tag_seq = b.tag_seq

where a.parent_seq is not null and a.part_dt='${TARGET_JOB_DATE}'

) c
;
" > cim_${LAST_JOB_DATE_NOSLA}.hql


run /app/di/script/run_hivetl.sh -f cim_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='13');"

etl_stat 'DONE'

rm -f cim_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log

