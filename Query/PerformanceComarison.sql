use tsdb;
-- ALTER TABLE  raw_data dROP PRIMARY KEY; 

/*===============================================
	index 적용 전 raw data 조회
============================================*/

select * from raw_data;

-- 설비별 평균 부하값
SELECT EQUIP_ID, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data 
GROUP BY EQUIP_ID;

-- 일자별 설비별 공정별 평균 부하값.
SELECT MFG_DT, EQUIP_ID, MOLD_POS, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data 
GROUP BY MFG_DT, EQUIP_ID, MOLD_POS;


-- 년별, 설비별, 공정별 평균 부하값
SELECT YEAR(MFG_DT) AS MFG_Year, EQUIP_ID, MOLD_POS, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data 
GROUP BY YEAR(MFG_DT), EQUIP_ID, MOLD_POS;

/*===============================================
	index 적용 전 raw data 조회 end
============================================*/


