CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT AS
SELECT
	fl.FILE_ID ,
	fl.LOAD_TS ,
	src.SRC_SYSTEM,
	src.SCENARIO,
	src.BUSINESS_DT,
	src.DT,
	src.PLANT_PRODUCT_ID,
	src.BP,
	src.PLAN,
	src.PPR,
	src.FORECAST,
	src.FACT
FROM STG_OPERSVODKA.STG_STG_HQ_REPORT src
	INNER JOIN STG_OPERSVODKA.V_ETL_FILE_LOAD fl
			ON fl.SRC_SYSTEM = src.SRC_SYSTEM
				AND fl.FILE_NAME = src.SCENARIO
				AND fl.BUSINESS_DT = src.BUSINESS_DT
	LEFT JOIN ODS_OPERSVODKA.ODS_STG_HQ_REPORT trg
		ON fl.FILE_ID = trg.FILE_ID
WHERE trg.FILE_ID IS NULL
;
