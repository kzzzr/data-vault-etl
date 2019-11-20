CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_ODS_COMMENTS_STG_COMMENTS AS
SELECT
	fl.FILE_ID ,
	fl.LOAD_TS ,
	src.SRC_SYSTEM,
	src.SCENARIO,
	src.BUSINESS_DT,
	src.ID,
	src.DT,
	src.PLANT,
	src.SUBPLANT,
	src.COMMENT,
	src.TEMPERATURE
FROM STG_OPERSVODKA.STG_COMMENTS src
	INNER JOIN STG_OPERSVODKA.V_ETL_FILE_LOAD fl
			ON fl.SRC_SYSTEM = src.SRC_SYSTEM
				AND fl.FILE_NAME = src.SCENARIO
				AND fl.BUSINESS_DT = src.BUSINESS_DT
	LEFT JOIN ODS_OPERSVODKA.ODS_COMMENTS trg
		ON fl.FILE_ID = trg.FILE_ID
WHERE trg.FILE_ID IS NULL
;
