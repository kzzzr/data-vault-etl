INSERT INTO STG_OPERSVODKA.DQ_LOG (
	LOAD_TS,
	OBJECT,
    DQ_NAME,
	METRIC,
    STATUS,
	DESCRIPTION
)
SELECT DISTINCT
	GETDATE()
	, FILE_REG.STAGE_TABLE AS OBJECT
	, 'CHECK IF FILE ALREADY EXISTS' AS DQ_NAME
	, FILE_REG.FILE_COUNT AS METRIC
	, CASE FILE_REG.FILE_COUNT WHEN 0 THEN 'OK' ELSE 'WARN' END AS STATUS
	, 'CHECK IF FILE FOR BUSINESS DATE FROM GIVEN DATA SOURCE ALREADY BEEN REGISTERED. OK=0 (NEW FILE)' AS DESCRIPTION
FROM (SELECT
			'STG_STG_HQ_REPORT'	AS STAGE_TABLE,
			COUNT(DISTINCT 1) AS FILE_COUNT
		FROM STG_OPERSVODKA.STG_STG_HQ_REPORT src
			INNER JOIN STG_OPERSVODKA.ETL_FILE_LOAD fl
				ON fl.SRC_SYSTEM = src.SRC_SYSTEM
					AND fl.FILE_NAME = src.SCENARIO
					AND fl.BUSINESS_DT = src.BUSINESS_DT) AS FILE_REG

;
