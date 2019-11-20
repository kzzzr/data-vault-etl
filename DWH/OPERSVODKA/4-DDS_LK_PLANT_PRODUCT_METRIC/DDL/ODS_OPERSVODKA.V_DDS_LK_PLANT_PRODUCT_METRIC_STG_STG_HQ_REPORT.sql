CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_STG_STG_HQ_REPORT_DDS_LK_PLANT_PRODUCT_METRIC AS
SELECT DISTINCT
	MD5(MD5(TRIM(src.PLANT_PRODUCT_ID::VARCHAR)) || MD5(TRIM(src.DT::VARCHAR))) AS HK_DDS_LK_PLANT_PRODUCT_METRIC,
	MD5(TRIM(src.PLANT_PRODUCT_ID::VARCHAR)) AS HK_DDS_HUB_PLANT_PRODUCT,
	MD5(TRIM(src.DT::VARCHAR)) AS HK_DDS_HUB_DATE,
	fl.FILE_ID ,
	fl.LOAD_TS
FROM STG_OPERSVODKA.STG_STG_HQ_REPORT src
	INNER JOIN STG_OPERSVODKA.V_ETL_FILE_LOAD fl
			ON fl.SRC_SYSTEM = src.SRC_SYSTEM
				AND fl.FILE_NAME = src.SCENARIO
				AND fl.BUSINESS_DT = src.BUSINESS_DT
	LEFT JOIN ODS_OPERSVODKA.DDS_LK_PLANT_PRODUCT_METRIC trg
		ON MD5(MD5(TRIM(src.PLANT_PRODUCT_ID::VARCHAR)) || MD5(TRIM(src.DT::VARCHAR))) = trg.HK_DDS_LK_PLANT_PRODUCT_METRIC
WHERE trg.HK_DDS_LK_PLANT_PRODUCT_METRIC IS NULL
;
