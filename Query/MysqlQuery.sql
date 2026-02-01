use tsdb;
select * from raw_data;

select equip_id ,count(*) from raw_data group by equip_id order by count(*);


/*
ALTER TABLE raw_data
  MODIFY EQUIP_ID VARCHAR(64),
  MODIFY SALE_CD  VARCHAR(64);
*/
/*===============================================
	정규화도 별 의미가 없음...
    
    그렇다면..
    
    설비별 데이터를 컬럼 별로 조회하고 
    mySql 기반으로 최대한 인덱싱 + 쿼리 튜닝으로 최적화 해보기 
		+
    influxdb + grafana로 설비별 power 값들 모니터링 경험해보기가 이 프로젝트의 마무리가 될듯..   
===============================================*/

/*===============================================
	설비 1대의 시간구간 조회
===============================================*/
SELECT mfg_dt, real_power
FROM raw_data
WHERE 
   mfg_dt >= '2017-03-01'
  AND mfg_dt <  '2025-04-01'
ORDER BY mfg_dt;


/*===============================================
	여러 설비를 한꺼번에 집계
===============================================*/
SELECT
  equip_id,
  DATE_FORMAT(mfg_dt, '%Y-%m-%d %H:00:00') AS hour_bucket,
  AVG(real_power) AS avg_power
FROM raw_data
WHERE mfg_dt >= '2021-03-01'
  AND mfg_dt <  '2021-04-01'
GROUP BY equip_id, hour_bucket
ORDER BY equip_id, hour_bucket;



