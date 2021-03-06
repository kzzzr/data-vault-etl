CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_STG_REFERENCE_DDS_HUB_PLANT_PRODUCT AS
SELECT DISTINCT
	MD5(TRIM(src.ID_PLANT_PRODUCT::VARCHAR)) AS HK_DDS_HUB_PLANT_PRODUCT ,
	fl.FILE_ID ,
	fl.LOAD_TS ,
	src.ID_PLANT_PRODUCT
FROM STG_OPERSVODKA.STG_REFERENCE src
	INNER JOIN STG_OPERSVODKA.V_ETL_FILE_LOAD fl
			ON fl.SRC_SYSTEM = src.SRC_SYSTEM
				AND fl.FILE_NAME = src.SCENARIO
				AND fl.BUSINESS_DT = src.BUSINESS_DT
	LEFT JOIN ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT trg
		ON MD5(TRIM(src.ID_PLANT_PRODUCT::VARCHAR)) = trg.HK_DDS_HUB_PLANT_PRODUCT
WHERE trg.HK_DDS_HUB_PLANT_PRODUCT IS NULL
;
