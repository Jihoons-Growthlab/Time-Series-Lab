use tsdb;


/*
============================================================
	설비별 평균 부하값
============================================================
*/
-- 설비별 평균 부하값
explain SELECT EQUIP_ID as '인덱싱 적용 전' , SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data 
GROUP BY EQUIP_ID;

-- 
explain SELECT EQUIP_ID as '인덱싱 적용 후', SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data_applyindex 
GROUP BY EQUIP_ID;