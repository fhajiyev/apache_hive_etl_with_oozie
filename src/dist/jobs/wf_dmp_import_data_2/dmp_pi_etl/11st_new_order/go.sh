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


TDY=`date +%Y%m%d`

#if [ "$TDY" == "20180125" ]; then
#LAST_JOB_DAY=`expr 2 + $1`
#elif [ "$TDY" == "20180126" ]; then
#LAST_JOB_DAY=`expr 3 + $1`
#elif [ "$TDY" == "20180127" ]; then
#LAST_JOB_DAY=`expr 4 + $1`
#fi

LAST_JOB_DAY=`expr 2 + $1`
LAST_JOB_DAY_15_AGO=`expr 16 + $1`
LAST_JOB_DAY_730_AGO=`expr 730 + $1`

LAST_JOB_DATE=`date +%Y/%m/%d -d "$LAST_JOB_DAY day ago"`
LAST_JOB_DATE_NOSLA=`date +%Y%m%d -d "$LAST_JOB_DAY day ago"`
LAST_JOB_DATE_NOSLA_730_AGO=`date +%Y%m%d -d "$LAST_JOB_DAY_730_AGO day ago"`
LAST_JOB_DATE_HYP=`date +%Y-%m-%d -d "$LAST_JOB_DAY day ago"`
LAST_JOB_DATE_HYP_15_AGO=`date +%Y-%m-%d -d "$LAST_JOB_DAY_15_AGO day ago"`
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

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${LAST_JOB_DATE_NOSLA}00', data_source_id='16')


select

  t.ci,
  t.pay_dt,
  t.pay_time,
  t.weekday,
  t.action,
  t.disp_ctgr_ctgr1_no,
  t.disp_ctgr_ctgr2_no,
  t.disp_ctgr_no,
  '',
  t.disp_ctgr_ctgr1_nm,
  t.disp_ctgr_ctgr2_nm,
  t.disp_ctgr_ctrg3_nm,
  '',
  t.cat_nm,
  '',
  '',
  t.prd_no,
  t.prd_nm,
  t.amount,
  t.opt_name,
  '',
  t.stl_mns_clf_cd,
  t.dtls_com_nm,

  t.sido,

  CASE
     WHEN t.sido <> '' AND t.sigungu <> '' THEN CONCAT(t.sido, ' ', t.sigungu)
     ELSE ''
  END,

  CASE
     WHEN t.sido <> '' AND t.sigungu <> '' AND t.dong <> '' THEN CONCAT(t.sido, ' ', t.sigungu, ' ', t.dong)
     ELSE ''
  END,

  t.bldng,


  '','','','',
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
  AA.ci
, BB.pay_dt
, BB.pay_time
, CASE from_unixtime(unix_timestamp(BB.pay_dt, 'yyyyMMdd'), 'u')
    WHEN 1 THEN 'mon'
    WHEN 2 THEN 'tue'
    WHEN 3 THEN 'wed'
    WHEN 4 THEN 'thu'
    WHEN 5 THEN 'fri'
    WHEN 6 THEN 'sat'
    ELSE 'sun'
  END as weekday
, BB.action 
, if(CC.disp_ctgr_ctgr1_no is null, '', CC.disp_ctgr_ctgr1_no) as disp_ctgr_ctgr1_no
, if(CC.disp_ctgr_ctgr2_no is null, '', CC.disp_ctgr_ctgr2_no) as disp_ctgr_ctgr2_no
, if(CC.disp_ctgr_no       is null, '', CC.disp_ctgr_no      ) as disp_ctgr_no      
, ''
, if(CC.disp_ctgr_ctgr1_nm is null, '', CC.disp_ctgr_ctgr1_nm) as disp_ctgr_ctgr1_nm
, if(CC.disp_ctgr_ctgr2_nm is null, '', CC.disp_ctgr_ctgr2_nm) as disp_ctgr_ctgr2_nm
, if(CC.disp_ctgr_ctrg3_nm is null, '', CC.disp_ctgr_ctrg3_nm) as disp_ctgr_ctrg3_nm
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
, BB.prd_no
, BB.prd_nm


, if(BB.amount         is null, '', BB.amount        ) as amount
, if(HH.opt_name       is null, '', HH.opt_name      ) as opt_name
, if(LL.stl_mns_clf_cd is null, '', LL.stl_mns_clf_cd) as stl_mns_clf_cd
, if(KK.dtls_com_nm    is null, '', KK.dtls_com_nm   ) as dtls_com_nm

, CASE
      WHEN GG.sido is null THEN ''

      WHEN GG.sido like '서울%'        THEN '서울'

      WHEN GG.sido like '인천%'        THEN '인천'
      WHEN GG.sido like '울산%'        THEN '울산'
      WHEN GG.sido like '부산%'        THEN '부산'
      WHEN GG.sido like '대전%'        THEN '대전'
      WHEN GG.sido like '대구%'        THEN '대구'
      WHEN GG.sido like '광주%'        THEN '광주'

      WHEN GG.sido like '세종%'        THEN '세종'
      WHEN GG.sido like '제주%'        THEN '제주'

      WHEN GG.sido like '강원%'        THEN '강원'
      WHEN GG.sido like '경기%'        THEN '경기'

      WHEN GG.sido = '전라남도'        THEN '전남'
      WHEN GG.sido = '전라북도'        THEN '전북'
      WHEN GG.sido = '충청남도'        THEN '충남'
      WHEN GG.sido = '충청북도'        THEN '충북'
      WHEN GG.sido = '경상남도'        THEN '경남'
      WHEN GG.sido = '경상북도'        THEN '경북'
  END as sido

, if (GG.sigungu                   is null,  '',   GG.sigungu) as sigungu
, if (GG.dong                      is null,  '',   GG.dong)    as dong
, if (GG.bldng                     is null,  '',   GG.bldng)   as bldng

 
 
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
select  A.pay_dt
      , substr(A.pay_dttm,12,2) as pay_time  
      , A.pur_mbr_no
      , A.ord_no       
      , A.prd_no
      , B.prd_nm
      , A.ord_amt as amount
      , 'buy' as action
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN A.disp_ctgr_no ELSE B.disp_ctgr_no_de  END as cat_id
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr1_no_de END as cat1_id
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr2_no_de END as cat2_id
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr3_no_de END as cat3_id
      , CASE WHEN B.disp_ctgr_no_de IN ('0', '') THEN ''             ELSE B.disp_ctgr4_no_de END as cat4_id

from 
11st.tb_evs_ods_f_sd_ord_detl_rslt A
join 
11st.tb_evs_ods_m_pd_prd B 
on A.prd_no = B.prd_no
 
where A.part_date = '${TODAY_JOB_DATE}'
  and B.part_date = '${TODAY_JOB_DATE}'
  and A.pay_dt = '${LAST_JOB_DATE_NOSLA}'    
  and A.pay_trns_amt - A.pay_seller_dscnt_amt_basic > 0
  and A.pur_mbr_no not in ('-1', '', '\n', '0')
) BB on AA.elev_id = BB.pur_mbr_no



left join
(
  select 
  ord_no,
  slct_prd_opt_nm as opt_name
  from
  11st.tb_evs_ods_f_tr_slct_prd_opt
  where
  part_date = '${TODAY_JOB_DATE}'
) 
HH
on 
HH.ord_no = BB.ord_no 



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

  select distinct
  ord_no,
  dtls_com_nm

  from

  (

  select
  ord_no,
  crden_cd

  from
  11st.TB_EVS_ODS_F_TR_CRDT_STL
  where
  part_date = '${LAST_JOB_DATE_NOSLA}'

  ) A
  
  inner join

  (

  select
  dtls_cd,
  dtls_com_nm

  from
  11st.tb_evs_ods_m_sy_co_detail
  where
  part_date = '${LAST_JOB_DATE_NOSLA}'
  and grp_cd='TR223'

  ) B

  on
  A.crden_cd = B.dtls_cd


) KK
on
KK.ord_no = BB.ord_no


left join

( 
  select distinct
  ord_no,
  stl_mns_clf_cd

  from
  11st.TB_EVS_ODS_F_TR_ORD_STL_RFND
  where 
  part_date = '${LAST_JOB_DATE_NOSLA}'

) LL
on
LL.ord_no = BB.ord_no


left join

(
select distinct 
DD.buy_mem_no_de,
EE.sido_nm          as sido,
EE.sigungu_nm       as sigungu,
EE.ueupmyon_nm      as dong,
EE.build_nm as bldng

from


(

            select
            buy_mem_no_de,
            max(delvplace_seq) as delvplace_seq_max
            from
            11st.tb_evs_ods_f_tr_ord_prd
            where
            part_date = '${TODAY_JOB_DATE}' and (SUBSTR(ord_no,1,8) between '${LAST_JOB_DATE_NOSLA_730_AGO}' and '${LAST_JOB_DATE_NOSLA}') and buy_mem_no_de not in ('','-1','0')
            group by buy_mem_no_de

)
DD
 
join
(


      select
      delvplace_seq,
      sido_nm,
      sigungu_nm,
      ueupmyon_nm,
      sigungu_build_nm as build_nm

      from

      (
         select
         delvplace_seq,
         rcvr_mail_no,
         rcvr_build_mng_no
         from
         11st.tb_evs_ods_f_tr_ord_clm_delvplace
         where
         part_date = '${TODAY_JOB_DATE}' and (SUBSTR(create_dt,1,10) between '${LAST_JOB_DATE_HYP_15_AGO}' and '${LAST_JOB_DATE_HYP}') and rcvr_build_mng_no <> ''
      ) tb11

      join
      (

         select
         CASE
              WHEN part_date = '${TODAY_JOB_DATE}' THEN area_no
              WHEN part_date = '20171029' THEN mail_no
         END as zcode,
         sido_nm,
         sigungu_nm,
         ueupmyon_nm,
         build_mng_no,
         sigungu_build_nm
         from
         11st.tb_evs_ods_m_sy_road_addr
         where ((part_date = '${TODAY_JOB_DATE}' and length(area_no)=5)or(part_date = '20171029' and length(mail_no)=6))

      ) tb12

      on
      (tb11.rcvr_mail_no = tb12.zcode and tb11.rcvr_build_mng_no = tb12.build_mng_no)



      union all      



      select
      delvplace_seq,
      sido_nm,
      sigungu_nm,
      ueupmyon_nm,
      '' as build_nm

      from

      (
         select
         delvplace_seq,
         rcvr_mail_no
         from
         11st.tb_evs_ods_f_tr_ord_clm_delvplace
         where
         part_date = '${TODAY_JOB_DATE}' and (SUBSTR(create_dt,1,10) between '${LAST_JOB_DATE_HYP_15_AGO}' and '${LAST_JOB_DATE_HYP}') and rcvr_build_mng_no = ''
      ) tb21

      join
      (

         select
         CASE
              WHEN part_date = '${TODAY_JOB_DATE}' THEN area_no
              WHEN part_date = '20171029' THEN mail_no
         END as zcode,
         sido_nm,
         sigungu_nm,
         ueupmyon_nm
         from
         11st.tb_evs_ods_m_sy_road_addr
         where ((part_date = '${TODAY_JOB_DATE}' and length(area_no)=5)or(part_date = '20171029' and length(mail_no)=6))

      ) tb22
      on
      tb21.rcvr_mail_no = tb22.zcode



)
EE
on
DD.delvplace_seq_max = EE.delvplace_seq
 

)GG
on
BB.pur_mbr_no = GG.buy_mem_no_de

) t
;" > 11st_purchase_${LAST_JOB_DATE_NOSLA}.hql


#run /app/di/script/run_hivetl.sh -f 11st_purchase_${LAST_JOB_DATE_NOSLA}.hql

/app/di/qcshell/bin/qcshell -b eda-hive2 -f 11st_purchase_${LAST_JOB_DATE_NOSLA}.hql

run /app/di/script/run_hivetl.sh -e "alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${DROP_DATE}00', data_source_id='16');"

etl_stat 'DONE'

rm -f 11st_purchase_${LAST_JOB_DATE_NOSLA}.hql

rm -f *.log

