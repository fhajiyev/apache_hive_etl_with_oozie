

set hivevar:day2before;
set hivevar:day5before;


set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

insert overwrite table dmp_pi.id_pool partition (part_date='${hivevar:day2before}')

select distinct

t.uid,
t.ocb_id,
t.sw_id,
t.elev_id,
t.mdnval_ocb,
t.mdnval_sw

from

(


      SELECT
      ci                                  as uid,
      rep_ocb_mbr_id                      as ocb_id,
      ''                                  as sw_id,
      ''                                  as elev_id,
      ''                                  as mdnval_ocb,
      ''                                  as mdnval_sw
      FROM
      svc_custpfdb.PDB_CI_MBR
      WHERE
      ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'
      AND
      rep_ocb_mbr_id IS NOT NULL AND rep_ocb_mbr_id <> ''


      UNION ALL

      SELECT
      ci                                  as uid,
      ''                                  as ocb_id,
      rep_syr_mbr_id                      as sw_id,
      ''                                  as elev_id,
      ''                                  as mdnval_ocb,
      ''                                  as mdnval_sw
      FROM
      svc_custpfdb.PDB_CI_MBR
      WHERE
      ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'
      AND
      rep_syr_mbr_id IS NOT NULL AND rep_syr_mbr_id <> ''


      UNION ALL

      SELECT
      ci                                  as uid,
      ''                                  as ocb_id,
      ''                                  as sw_id,
      rep_evs_mbr_id                      as elev_id,
      ''                                  as mdnval_ocb,
      ''                                  as mdnval_sw
      FROM
      svc_custpfdb.PDB_CI_MBR
      WHERE
      ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'
      AND
      rep_evs_mbr_id IS NOT NULL AND rep_evs_mbr_id <> ''


      UNION ALL

      SELECT
      b1.ci                               as uid,
      ''                                  as ocb_id,
      ''                                  as sw_id,
      ''                                  as elev_id,
      a1.clphn_no                         as mdnval_ocb,
      ''                                  as mdnval_sw
      FROM

      (
        SELECT
        mbr_id,
        clphn_no
        FROM
        ocb.mart_mbr_mst
        WHERE
        mbr_id IS NOT NULL AND mbr_id <> ''
        AND mbr_sts_cd = 'A'
      ) a1

      inner join

               (

                SELECT
                ci,
                rep_ocb_mbr_id
                FROM
                svc_custpfdb.PDB_CI_MBR
                WHERE
                ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'
                AND
                rep_ocb_mbr_id IS NOT NULL AND rep_ocb_mbr_id <> ''

               ) b1

               ON a1.mbr_id = b1.rep_ocb_mbr_id



      UNION ALL

      SELECT
      b2.ci                               as uid,
      ''                                  as ocb_id,
      ''                                  as sw_id,
      ''                                  as elev_id,
      ''                                  as mdnval_ocb,
      a2.mdn                              as mdnval_sw
      FROM

      (
        SELECT
        member_id,
        mdn
        FROM
        smartwallet.mt3_member
        WHERE
        member_id IS NOT NULL AND member_id <> ''
        AND wallet_accept = 1 and wallet_accept1 = 1 and wallet_accept2 = 1 and vm_state_cd = '9' and length(last_auth_dt) = 14
      ) a2

      inner join

               (

                SELECT
                ci,
                rep_syr_mbr_id
                FROM
                svc_custpfdb.PDB_CI_MBR
                WHERE
                ci IS NOT NULL AND ci <> '' AND ci <> '\n' AND ci not like '%\u0001%'
                AND
                rep_syr_mbr_id IS NOT NULL AND rep_syr_mbr_id <> ''

               ) b2

               ON a2.member_id = b2.rep_syr_mbr_id


) t
;

alter table dmp_pi.id_pool drop partition (part_date < '${hivevar:day5before}');






