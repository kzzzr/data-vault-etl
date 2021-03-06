CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_STG_REFERENCE_DDS_HST_PLANT_PRODUCT_REFERENCE AS
SELECT DISTINCT
	MD5(TRIM(src.ID_PLANT_PRODUCT::VARCHAR)) AS HK_DDS_HUB_PLANT_PRODUCT ,
	fl.FILE_ID ,
	fl.LOAD_TS ,
	MD5(isnull(src.PLANT::VARCHAR, 'NULL') || isnull(src.PLANT_ENCRYPTED::VARCHAR, 'NULL') || isnull(src.PRODUCT::VARCHAR, 'NULL') || isnull(src.PLANT_PRODUCT::VARCHAR, 'NULL') || isnull(src.PLANT_PRODUCT_ENCRYPTED::VARCHAR, 'NULL') || isnull(src.FACTORY::VARCHAR, 'NULL')) AS HASHDIFF ,
	src.PLANT,
	src.PLANT_ENCRYPTED,
	src.PRODUCT,
	src.PLANT_PRODUCT,
	src.PLANT_PRODUCT_ENCRYPTED,
	src.FACTORY
FROM STG_OPERSVODKA.STG_REFERENCE src
	INNER JOIN STG_OPERSVODKA.V_ETL_FILE_LOAD fl
			ON fl.SRC_SYSTEM = src.SRC_SYSTEM
				AND fl.FILE_NAME = src.SCENARIO
				AND fl.BUSINESS_DT = src.BUSINESS_DT
	LEFT JOIN ODS_OPERSVODKA.V_DDS_HST_PLANT_PRODUCT_REFERENCE trg
		ON MD5(TRIM(src.ID_PLANT_PRODUCT::VARCHAR)) = trg.HK_DDS_HUB_PLANT_PRODUCT
			AND MD5(isnull(src.PLANT::VARCHAR, 'NULL') || isnull(src.PLANT_ENCRYPTED::VARCHAR, 'NULL') || isnull(src.PRODUCT::VARCHAR, 'NULL') || isnull(src.PLANT_PRODUCT::VARCHAR, 'NULL') || isnull(src.PLANT_PRODUCT_ENCRYPTED::VARCHAR, 'NULL') || isnull(src.FACTORY::VARCHAR, 'NULL')) = trg.HASHDIFF
WHERE trg.HK_DDS_HUB_PLANT_PRODUCT IS NULL
;
