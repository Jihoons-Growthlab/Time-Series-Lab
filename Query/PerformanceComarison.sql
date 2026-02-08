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
SELECT DATE(MFG_DT) AS MFG_DAY, EQUIP_ID, MOLD_POS, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data 
GROUP BY DATE(MFG_DT), EQUIP_ID, MOLD_POS;


-- 년별, 설비별, 공정별 평균 부하값
SELECT YEAR(MFG_DT) AS MFG_YEAR, EQUIP_ID, MOLD_POS, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data 
GROUP BY YEAR(MFG_DT), EQUIP_ID, MOLD_POS;

/*===============================================
	index 적용 전 raw data 조회 end
============================================*/
/*===============================================
	index 적용 후 raw data 조회
			CREATE TABLE raw_data_applyindex AS
			SELECT *
			FROM raw_data;
============================================*/

select * from raw_data_applyindex;

-- 설비별 평균 부하값
SELECT EQUIP_ID, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data_applyindex 
GROUP BY EQUIP_ID;

-- 일자별 설비별 공정별 평균 부하값.
SELECT mfg_year , mfg_month, mfg_day, EQUIP_ID, MOLD_POS, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data_applyindex 
GROUP BY  EQUIP_ID, MOLD_POS, mfg_year, mfg_month, mfg_day;


-- 년별, 설비별, 공정별 평균 부하값
SELECT mfg_year AS MFG_YEAR, mfg_month, EQUIP_ID, MOLD_POS, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data_applyindex 
GROUP BY EQUIP_ID, MOLD_POS , mfg_year, mfg_month;

-- 년별, 설비별, 공정별 평균 부하값
SELECT mfg_year AS MFG_YEAR, EQUIP_ID, MOLD_POS, SUM(REAL_POWER)/COUNT(*) AS AVG_POWER_PERDAY
FROM raw_data_applyindex 
GROUP BY EQUIP_ID, MOLD_POS , mfg_year;


/*===========================================
	index 적용 후 raw data 조회 end
============================================*/


