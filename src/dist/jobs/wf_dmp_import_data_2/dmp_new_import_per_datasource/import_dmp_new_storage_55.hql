

set hivevar:day2before;
set hivevar:day367before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='55')

SELECT

A.ci,

if(B.created_date is null, '', REGEXP_REPLACE(SUBSTR(B.created_date, 1, 10), '-', '')),
if(B.approval_date is null, '', B.approval_date),
if(B.approval_time is null, '', B.approval_time),
if(B.approval_number is null, '', B.approval_number),
if(B.card_no is null, '', B.card_no),
if(B.partner_card_code is null, '', B.partner_card_code),
if(B.id is null, '', B.id),
if(B.orig_approval_date is null, '', B.orig_approval_date),
if(B.orig_approval_time is null, '', B.orig_approval_time),
if(B.orig_approval_number is null, '', B.orig_approval_number),
if(B.tr_type is null, '', B.tr_type),
if(B.installments is null, '', B.installments),
if(B.currency_code is null, '', B.currency_code),
if(B.service_charge is null, '', B.service_charge),
if(B.vat is null, '', B.vat),
if(B.pay_amount is null, '', B.pay_amount),
if(B.supply_amount is null, '', B.supply_amount),
if(B.abroad_use is null, '', B.abroad_use),
if(B.merchant_no is null, '', B.merchant_no),
if(B.merchant_name is null, '', B.merchant_name),
if(B.merchant_type_code is null, '', B.merchant_type_code),
if(B.merchant_biz_no is null, '', B.merchant_biz_no),
if(B.merchant_industry_code is null, '', B.merchant_industry_code),

if(C.industry_name is null, '', C.industry_name),
if(C.status is null, '', C.status),
if(C.address1 is null, '', C.address1),
if(C.address2 is null, '', C.address2),
if(C.zipcode is null, '', C.zipcode),
if(C.mdn is null, '', C.mdn),

if(D.check_credit is null, '', D.check_credit),
if(D.card_name is null, '', D.card_name),
if(D.card_description is null, '', D.card_description),
if(D.start_date is null, '', D.start_date),
if(D.end_date is null, '', D.end_date),
if(D.card_status is null, '', D.card_status),
if(D.card_code is null, '', D.card_code),

  '','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '','','','','','','','','','',
  '${hivevar:day2before}00',
  '${hivevar:day2before}00'


FROM
svc_chac.batch_member_chac A

LEFT JOIN
svc_chac.clo_approval_chac B
ON
A.card_no = B.card_no

LEFT JOIN
svc_chac.clo_merchant_chac C
ON
B.merchant_no = C.merchant_no

LEFT JOIN
svc_chac.card_meta_chac D
ON
B.partner_card_code = D.partner_card_code
;

alter table dmp_pi.prod_data_source_store drop partition (part_hour < '${hivevar:day367before}00', data_source_id='55');
alter table dmp_pi.prod_data_source_store_parquet drop partition (part_hour < '${hivevar:day367before}00', data_source_id='55');


