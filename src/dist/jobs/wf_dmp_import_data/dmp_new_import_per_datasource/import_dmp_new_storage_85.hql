USE dmp;


set hivevar:daybefore;
set hivevar:day2before;
set hivevar:day90before;
set hivevar:day91before;



set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.smallfiles.avgsize=67108864;
set hive.merge.size.per.task=134217728;
set hive.merge.tezfiles=true;

set hive.execution.engine=tez;
set tez.queue.name=COMMON;
set hive.exec.parallel=true;

-- BizSpring

  INSERT OVERWRITE TABLE dmp.prod_data_source_store PARTITION (part_hour='${hivevar:day2before}00', data_source_id='85')
  SELECT
  b.DMP_ID as uid,
  a.REQUEST_DATE,
  a.REQUEST_TIME,
  CASE
        WHEN a.WEEKDAY = '1' THEN 'Mon'
        WHEN a.WEEKDAY = '2' THEN 'Tue'
        WHEN a.WEEKDAY = '3' THEN 'Wed'
        WHEN a.WEEKDAY = '4' THEN 'Thu'
        WHEN a.WEEKDAY = '5' THEN 'Fri'
        WHEN a.WEEKDAY = '6' THEN 'Sat'
        WHEN a.WEEKDAY = '7' THEN 'Sun'
  END,

  a.COMP_NM,
  a.ACTION,
  a.KEYWORD,
  a.PROD_NM,
  a.PROD_CD,
  a.AMOUNT,
  a.DEVICE_NM,
  a.OS_NM,
  a.BROWSER_NM,
  a.FIRST_YN,
  a.CAMPAIGN_CD_YN,
  a.CAMPAIGN_CD,
  a.VIRAL_CAMPAIGN_CD_YN,
  a.VIRAL_CAMPAIGN_CD,


 CASE
      WHEN a.HOSTVAL = 'zigzag.kr'                      THEN '소셜미디어'
      WHEN a.HOSTVAL = 'l.instagram.com'               THEN '소셜미디어'
      WHEN a.HOSTVAL = 'm.facebook.com'                THEN '소셜미디어'
      WHEN a.HOSTVAL = 'www.facebook.com'              THEN '소셜미디어'
      WHEN a.HOSTVAL = 't.co'                            THEN '소셜미디어'
      WHEN a.HOSTVAL = 'instagram.com'                 THEN '소셜미디어'
      WHEN a.HOSTVAL = 'l.facebook.com'                THEN '소셜미디어'
      WHEN a.HOSTVAL = 'lm.facebook.com'               THEN '소셜미디어'
      WHEN a.HOSTVAL = 'ammonia.daum.net'              THEN '광고'
      WHEN a.HOSTVAL = 'zum.com'                         THEN '광고'
      WHEN a.HOSTVAL = 'dreamsearch.or.kr'             THEN '광고'
      WHEN a.HOSTVAL = 'm.nxad.search.naver.com'      THEN '광고'
      WHEN a.HOSTVAL = 'realdsp.realclick.co.kr'      THEN '광고'
      WHEN a.HOSTVAL = 'm.trend.shopping.naver.com'  THEN '광고'
      WHEN a.HOSTVAL = 'cr2.shopping.naver.com'       THEN '광고'
      WHEN a.HOSTVAL = 'cl.ncclick.co.kr'              THEN '광고'
      WHEN a.HOSTVAL = 'msearch.shopping.naver.com'  THEN '광고'
      WHEN a.HOSTVAL = 'www.dreamsearch.or.kr'        THEN '광고'
      WHEN a.HOSTVAL = 'display.ad.daum.net'          THEN '광고'
      WHEN a.HOSTVAL = 'antg.widerplanet.com'         THEN '광고'
      WHEN a.HOSTVAL = 'cas.as.criteo.com'             THEN '광고'
      WHEN a.HOSTVAL = 'm.ad.search.naver.com'        THEN '광고'
      WHEN a.HOSTVAL = 'googleads.g.doubleclick.net' THEN '광고'
      WHEN a.HOSTVAL = 'ads.as.criteo.com'             THEN '광고'
      WHEN a.HOSTVAL = 'ad.search.naver.com'          THEN '광고'
      WHEN a.HOSTVAL = 'castbox.shopping.naver.com'  THEN '광고'
      WHEN a.HOSTVAL = 'm.pla.naver.com'               THEN '광고'
      WHEN a.HOSTVAL = 'pla.naver.com'                 THEN '광고'
      WHEN a.HOSTVAL = 'shopping.daum.net'            THEN '광고'
      WHEN a.HOSTVAL = 'm.search.naver.com'           THEN '검색'
      WHEN a.HOSTVAL = 'www.google.co.kr'             THEN '검색'
      WHEN a.HOSTVAL = 'search.naver.com'             THEN '검색'
      WHEN a.HOSTVAL = 'search.daum.net'              THEN '검색'
      WHEN a.HOSTVAL = 'm.search.daum.net'            THEN '검색'
      WHEN a.HOSTVAL = 'search.zum.com'               THEN '검색'
      WHEN a.HOSTVAL = 'www.bing.com'                  THEN '검색'
      WHEN a.HOSTVAL = 'www.google.com'               THEN '검색'
      ELSE ''
 END,

 CASE
       WHEN a.HOSTVAL = 'zigzag.kr'                      THEN '지그재그'
       WHEN a.HOSTVAL = 'l.instagram.com'               THEN '인스타그램'
       WHEN a.HOSTVAL = 'm.facebook.com'                THEN '페이스북'
       WHEN a.HOSTVAL = 'www.facebook.com'              THEN '페이스북'
       WHEN a.HOSTVAL = 't.co'                            THEN '트위터'
       WHEN a.HOSTVAL = 'instagram.com'                 THEN '인스타그램'
       WHEN a.HOSTVAL = 'l.facebook.com'                THEN '페이스북'
       WHEN a.HOSTVAL = 'lm.facebook.com'               THEN '페이스북'
       WHEN a.HOSTVAL = 'ammonia.daum.net'              THEN '다음 쇼핑박스'
       WHEN a.HOSTVAL = 'zum.com'                         THEN '줌닷컴 광고'
       WHEN a.HOSTVAL = 'dreamsearch.or.kr'             THEN '모비온 광고'
       WHEN a.HOSTVAL = 'm.nxad.search.naver.com'      THEN '네이버 검색광고'
       WHEN a.HOSTVAL = 'realdsp.realclick.co.kr'      THEN '리얼클릭 광고'
       WHEN a.HOSTVAL = 'm.trend.shopping.naver.com'  THEN '네이버 트렌드픽'
       WHEN a.HOSTVAL = 'cr2.shopping.naver.com'       THEN '네이버쇼핑 광고'
       WHEN a.HOSTVAL = 'cl.ncclick.co.kr'              THEN '네오클릭 광고'
       WHEN a.HOSTVAL = 'msearch.shopping.naver.com'  THEN '네이버쇼핑 광고'
       WHEN a.HOSTVAL = 'www.dreamsearch.or.kr'        THEN '모비온 광고'
       WHEN a.HOSTVAL = 'display.ad.daum.net'          THEN '다음 디스플레이광고'
       WHEN a.HOSTVAL = 'antg.widerplanet.com'         THEN '와이더플래닛 광고'
       WHEN a.HOSTVAL = 'cas.as.criteo.com'             THEN '크리테오 광고'
       WHEN a.HOSTVAL = 'm.ad.search.naver.com'        THEN '네이버 검색광고'
       WHEN a.HOSTVAL = 'googleads.g.doubleclick.net' THEN '구글 더블클릭 광고'
       WHEN a.HOSTVAL = 'ads.as.criteo.com'             THEN '크리테오 광고'
       WHEN a.HOSTVAL = 'ad.search.naver.com'          THEN '네이버 검색광고'
       WHEN a.HOSTVAL = 'castbox.shopping.naver.com'  THEN '네이버 쇼핑박스'
       WHEN a.HOSTVAL = 'm.pla.naver.com'               THEN '네이버쇼핑 광고'
       WHEN a.HOSTVAL = 'pla.naver.com'                 THEN '네이버쇼핑 광고'
       WHEN a.HOSTVAL = 'shopping.daum.net'            THEN '다음쇼핑 광고'
       WHEN a.HOSTVAL = 'm.search.naver.com'           THEN '네이버'
       WHEN a.HOSTVAL = 'www.google.co.kr'             THEN '구글'
       WHEN a.HOSTVAL = 'search.naver.com'             THEN '네이버'
       WHEN a.HOSTVAL = 'search.daum.net'              THEN '다음'
       WHEN a.HOSTVAL = 'm.search.daum.net'            THEN '다음'
       WHEN a.HOSTVAL = 'search.zum.com'               THEN '줌닷컴'
       WHEN a.HOSTVAL = 'www.bing.com'                  THEN '빙'
       WHEN a.HOSTVAL = 'www.google.com'               THEN '구글'
       ELSE ''
 END,

 CASE
       WHEN a.HOSTVAL = 'm.search.naver.com'           THEN a.QUERYVAL
       WHEN a.HOSTVAL = 'search.naver.com'             THEN a.QUERYVAL
       WHEN a.HOSTVAL = 'search.daum.net'              THEN a.QVAL
       WHEN a.HOSTVAL = 'm.search.daum.net'            THEN a.QVAL
       WHEN a.HOSTVAL = 'search.zum.com'               THEN a.QUERYVAL
       ELSE ''
 END,

  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  '', '', '', '', '', '', '', '', '', '',
  part_hour,
  log_time

  FROM
  (
  SELECT

        UID,

        SUBSTR(LOG_TIME,1,8) AS REQUEST_DATE,
        SUBSTR(LOG_TIME,9,2) AS REQUEST_TIME,
        from_unixtime(unix_timestamp(SUBSTR(LOG_TIME,1,8),'yyyyMMdd'),'u') AS WEEKDAY,

        if(COL004 IS NULL, '', COL004) AS COMP_NM,

        if(COL011 IS NULL OR COL011 = '', 'visit', COL011) AS ACTION,

        if(COL012 IS NOT NULL AND COL011 IS NOT NULL AND COL011 = 'search', COL012, '') AS KEYWORD,

        if(COL013 IS NOT NULL AND COL011 IS NOT NULL AND COL011 IN ('pdv','sci','odr'), COL013, '') AS PROD_NM,

        if(COL014 IS NOT NULL AND COL011 IS NOT NULL AND COL011 IN ('pdv','sci','odr'), COL014, '') AS PROD_CD,

        if(COL015 IS NOT NULL AND COL011 IS NOT NULL AND COL011 = 'odr', COL015, '') AS AMOUNT,

        if(COL001 IS NULL, '', COL001) AS DEVICE_NM,
        if(COL002 IS NULL, '', COL002) AS OS_NM,
        if(COL003 IS NULL, '', COL003) AS BROWSER_NM,

        if(COL005 IS NULL, '', COL005) AS FIRST_YN,
        if(COL006 IS NULL, '', COL006) AS CAMPAIGN_CD_YN,
        if(COL007 IS NULL, '', COL007) AS CAMPAIGN_CD,
        if(COL008 IS NULL, '', COL008) AS VIRAL_CAMPAIGN_CD_YN,
        if(COL009 IS NULL, '', COL009) AS VIRAL_CAMPAIGN_CD,

        if(COL010 IS NULL, '', PARSE_URL(COL010, 'HOST')) AS HOSTVAL,

        if(COL010 IS NULL, '', PARSE_URL(COL010, 'QUERY', 'q')) AS QVAL,

        if(COL010 IS NULL, '', PARSE_URL(COL010, 'QUERY', 'query')) AS QUERYVAL,

        log_time,
        part_hour



  FROM dmp.log_server_bulk_collect_sftp
  WHERE
  part_hour between '${hivevar:day2before}00' and '${hivevar:day2before}24'
  AND UID IS NOT NULL AND UID <> ''
  AND LSID IS NOT NULL AND LSID = '85'


  )
  a

  INNER JOIN
  (

    SELECT
    DMP_ID,
    DSP_COOKIE_ID
    FROM
    SVC_DS_DMP.PROD_DMP_ID_MATCHING
    WHERE
    ID_TYPE IN ('uid010', 'gaid', 'idfa')

  ) b
  ON
  a.UID = b.DSP_COOKIE_ID

;




