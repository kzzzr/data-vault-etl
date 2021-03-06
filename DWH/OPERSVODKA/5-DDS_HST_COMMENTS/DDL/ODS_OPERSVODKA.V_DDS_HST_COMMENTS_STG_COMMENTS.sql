CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_STG_COMMENTS_DDS_HST_COMMENTS AS
SELECT DISTINCT
	MD5(TRIM(src.ID::VARCHAR) || TRIM(src.DT::VARCHAR)) AS HK_DDS_HUB_COMMENTS ,
	fl.FILE_ID ,
	fl.LOAD_TS ,
	MD5(isnull(src.PLANT::VARCHAR, 'NULL') || isnull(src.SUBPLANT::VARCHAR, 'NULL') || isnull(src.COMMENT::VARCHAR, 'NULL') || isnull(src.TEMPERATURE::VARCHAR, 'NULL')) AS HASHDIFF ,
	src.PLANT,
	src.SUBPLANT,
	src.COMMENT,
	src.TEMPERATURE
FROM STG_OPERSVODKA.STG_COMMENTS src
	INNER JOIN STG_OPERSVODKA.V_ETL_FILE_LOAD fl
			ON fl.SRC_SYSTEM = src.SRC_SYSTEM
				AND fl.FILE_NAME = src.SCENARIO
				AND fl.BUSINESS_DT = src.BUSINESS_DT
	LEFT JOIN ODS_OPERSVODKA.V_DDS_HST_COMMENTS trg
		ON MD5(TRIM(src.ID::VARCHAR) || TRIM(src.DT::VARCHAR)) = trg.HK_DDS_HUB_COMMENTS
			AND MD5(isnull(src.PLANT::VARCHAR, 'NULL') || isnull(src.SUBPLANT::VARCHAR, 'NULL') || isnull(src.COMMENT::VARCHAR, 'NULL') || isnull(src.TEMPERATURE::VARCHAR, 'NULL')) = trg.HASHDIFF
WHERE trg.HK_DDS_HUB_COMMENTS IS NULL
;
