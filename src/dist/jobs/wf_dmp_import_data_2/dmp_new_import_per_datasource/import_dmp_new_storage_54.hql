

set hivevar:day2before;
set hivevar:day5before;

set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.prod_data_source_store partition (part_hour='${hivevar:day2before}00', data_source_id='54')

SELECT

A.ci,
REGEXP_REPLACE(SUBSTR(A.created_date, 1, 10), '-', ''),
A.standard_date,
A.partner_member_id,
A.gender,
A.member_birthday_type,
A.member_birthday,
A.member_status,
A.card_issue_date,
A.card_no,
A.partner_card_code,
A.card_type_code,
A.bank_code,

if(B.check_credit is null, '', B.check_credit),
if(B.card_name is null, '', B.card_name),
if(B.card_description is null, '', B.card_description),
if(B.start_date is null, '', B.start_date),
if(B.end_date is null, '', B.end_date),
if(B.card_status is null, '', B.card_status),
if(B.card_code is null, '', B.card_code),

if(C.terms_date_time is null, '', SUBSTR(C.terms_date_time, 1, 10)),
if(C.terms_agree is null, '', C.terms_agree),
if(C.terms_code is null, '', C.terms_code),

  '','','','','','','','',
  '','','','','','','','','','',
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
svc_chac.card_meta_chac B
ON
A.partner_card_code = B.partner_card_code

LEFT JOIN
svc_chac.clo_terms_chac C
ON
A.ci = C.ci
;

