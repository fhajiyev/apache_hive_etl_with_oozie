#!/bin/sh
if test "$#" -ne 1; then
    echo "Illegal number of parameters"
    exit
fi


HDD=/app/yarn_dic/bin/hadoop
etl_stat() {
    CTIME=`date +%Y%m%d%H%M%S`
}

die() {
    etl_stat 'FAIL';
    echo >&2 -e "\nERROR: $@\n"; exit 1;
}
run() { "$@"; code=$?; [ $code -ne 0 ] && die "command [$*] failed with error code $code"; }
print_bline() { printf "\e[96m============================================================================\e[0m\n"; }
print_bstr() { printf "\e[92m$@\e[0m\n"; }
print_sstr() { printf "\e[93m$@\e[0m\n"; }

HD="/app/yarn_etl/bin/hadoop jar /app/yarn_etl/share/hadoop/tools/lib/hadoop-streaming-2.6.0-cdh5.5.1.jar"



LAST_JOB_DAY=`expr 2 + $1`
LAST_JOB_DAY_270_AGO=`expr 271 + $1`
LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
LAST_JOB_DATE_HYP=`date +%Y-%m-%d -d "$LAST_JOB_DAY day ago"`
LAST_JOB_DATE_HYP_270_AGO=`date +%Y-%m-%d -d "$LAST_JOB_DAY_270_AGO day ago"`
TARGET_JOB_DATE=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
TODAY_JOB_DATE=`date +%Y%m%d -d "2 day ago"`

PR_DATA_JOB_DATE=`date +%Y/%m/%d -d "2 day ago"`
#PR_DATA_JOB_DATE="2017/11/30"


PR_DATA_JOB_DATE2=`date +%Y/%m/%d -d "1 day ago"`

DROP_DAYS=`expr 367 + $1`
DROP_DATE=`date +%Y%m%d -d "$DROP_DAYS day ago"`

DMP_PI_HDFS_HOME='hdfs://skpds/user/dmp_pi/dmp_pi_store'
DMP_PI_LOCAL_HOME=/app/dmp_pi/dmp_pi_etl

WORKDIR=$DMP_PI_HDFS_HOME/$TARGET_JOB_DATE

ID_POOL=hdfs://skpds/user/dmp_pi/dmp_pi_id_pool/part_date=$TODAY_JOB_DATE

etl_stat 'DOING'
run cp $DMP_PI_LOCAL_HOME/11st_disp_ct_dic.dat $DMP_PI_LOCAL_HOME/11st_new_order/
#####run cp $DMP_PI_LOCAL_HOME/11st_brand_dic.dat $DMP_PI_LOCAL_HOME/11st_order/
print_bline
print_bstr "ETL : 11st 구매/장바구니 정보 취합"

# mapper 1 : print '%s\t01\t%s\t%s\t%s\t%s' % (prd_no, member_no, cate, brand_cd, dt)
# mapper 2 : print '%s\t02\t%s\t%s\t\t%s' % (prd_no, member_no, cate, dt)
# mapper 3 : print '%s\t%s' % (prd_no, brand_cd)

# hdfs://skpds/data_bis/11st/raw/evs_pd_prd_hits/2017/07/24 뷰 테이블

# tb_evs_ods_f_sd_ord_detl_rslt
# tb_evs_ods_f_tr_bckt
# tb_evs_ods_m_pd_prd


echo "
set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='17')

select
   AA.ci                          as uid
 , BB.create_dt                   as dateval
 , BB.create_time                 as time
 , CASE from_unixtime(unix_timestamp(BB.create_dt, 'yyyyMMdd'), 'u')
     WHEN 1 THEN 'mon'
     WHEN 2 THEN 'tue'
     WHEN 3 THEN 'wed'
     WHEN 4 THEN 'thu'
     WHEN 5 THEN 'fri'
     WHEN 6 THEN 'sat'
     ELSE 'sun'
   END as weekday
 , BB.action
 , CASE WHEN CC.disp_ctgr_ctgr1_no IS NULL THEN '' ELSE CC.disp_ctgr_ctgr1_no END
 , CASE WHEN CC.disp_ctgr_ctgr2_no IS NULL THEN '' ELSE CC.disp_ctgr_ctgr2_no END
 , CASE WHEN CC.disp_ctgr_no       IS NULL THEN '' ELSE CC.disp_ctgr_no       END
 , ''
 , CASE WHEN CC.disp_ctgr_ctgr1_nm IS NULL THEN '' ELSE CC.disp_ctgr_ctgr1_nm END
 , CASE WHEN CC.disp_ctgr_ctgr2_nm IS NULL THEN '' ELSE CC.disp_ctgr_ctgr2_nm END
 , CASE WHEN CC.disp_ctgr_ctrg3_nm IS NULL THEN '' ELSE CC.disp_ctgr_ctrg3_nm END
 , ''
 , TRIM
 (
 CONCAT
 (
     CASE WHEN CC.disp_ctgr_ctgr1_nm IN (' ','') OR CC.disp_ctgr_ctgr1_nm IS NULL THEN ''
     ELSE CC.disp_ctgr_ctgr1_nm
     END,
     CASE WHEN CC.disp_ctgr_ctgr2_nm IN (' ','') OR CC.disp_ctgr_ctgr2_nm IS NULL THEN ''
     ELSE CONCAT('\||',CC.disp_ctgr_ctgr2_nm)
     END,
     CASE WHEN CC.disp_ctgr_ctrg3_nm IN (' ','') OR CC.disp_ctgr_ctrg3_nm IS NULL THEN ''
     ELSE CONCAT('\||',CC.disp_ctgr_ctrg3_nm)
     END
 )
 ) AS cat_nm
 , ''
 , ''
 , BB.prd_no
 , BB.prd_nm


 , IF(DD.sel_prc  is null, '', DD.sel_prc )
 , IF(HH.opt_name is null, '', HH.opt_name)

 , '',   
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
    select
    ci,
    elev_id
    from
    dmp_pi.id_pool
    where part_date = '${TODAY_JOB_DATE}'

 ) AA
 join
 (
 select  regexp_replace(substr(A.create_dt,1,10),'-','') as create_dt
       , substr(A.create_dt,12,2) as create_time
       , A.create_no
       , A.bckt_seq 
       , A.prd_no
       , B.prd_nm
       , 'keep' as action
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN A.disp_ctgr_no ELSE B.disp_ctgr_no_de  END as cat_id
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr1_no_de END as cat1_id
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr2_no_de END as cat2_id
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr3_no_de END as cat3_id
       , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr4_no_de END as cat4_id

 from
 11st.tb_evs_ods_f_tr_bckt A
 join
 11st.tb_evs_ods_m_pd_prd B
 on A.prd_no = B.prd_no
 
 where A.part_date = '${TODAY_JOB_DATE}'
   and B.part_date = '${TODAY_JOB_DATE}'
   and regexp_replace(substr(A.create_dt,1,10),'-','') = '${LAST_JOB_DATE_NOSLA}'   
   and A.create_no not in ('-1', '', '\n', '0')
 ) BB on AA.elev_id = BB.create_no

 
 left join
 (
    select
    bckt_seq,
    slct_prd_opt_nm as opt_name
    from
    11st.tb_evs_ods_f_tr_slct_prd_opt
    where
    part_date = '${TODAY_JOB_DATE}'
 )
 HH
 on
 HH.bckt_seq = BB.bckt_seq

 left join
 (
    select
    disp_ctgr_no,
    disp_ctgr_ctgr1_no,
    disp_ctgr_ctgr2_no,
    disp_ctgr_ctrg3_no,
    disp_ctgr_ctgr1_nm,
    disp_ctgr_ctgr2_nm,
    disp_ctgr_ctrg3_nm
    from
    11st.tb_evs_ods_m_dd_disp_ctgr
    where
    part_date = '${LAST_JOB_DATE_NOSLA}'
 )
 CC
 on
 BB.cat_id = CC.disp_ctgr_no

 left join
 (
    select
    prd_no,
    sel_prc

    from
    11st.tb_evs_ods_m_pd_prd_prc
    where
    part_date = '${LAST_JOB_DATE_NOSLA}'
    and
    substr(create_dt,1,10) >= '${LAST_JOB_DATE_HYP_270_AGO}' and substr(create_dt,1,10) <= '${LAST_JOB_DATE_HYP}'

 ) DD on BB.prd_no = DD.prd_no

;" > 11st_basket_${LAST_JOB_DATE_NOSLA}.hql


/app/di/qcshell/bin/qcshell -b eda-hive2 -f 11st_basket_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='17');"

etl_stat 'DONE'

rm -f 11st_basket_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log

