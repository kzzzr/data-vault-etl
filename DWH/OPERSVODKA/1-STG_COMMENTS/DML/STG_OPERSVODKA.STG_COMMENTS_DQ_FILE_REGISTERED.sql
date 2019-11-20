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
	, 'CHECK FILE REGISTERED' AS DQ_NAME
	, FILE_REG.FILE_COUNT AS METRIC
	, CASE FILE_REG.FILE_COUNT WHEN 1 THEN 'OK' ELSE 'WARN' END AS STATUS
	, 'CHECK IF FILE HAS BEEN REGISTERED. OK=1 (FILE FOUND)' AS DESCRIPTION
FROM (SELECT
			'STG_COMMENTS'	AS STAGE_TABLE,
			COUNT(DISTINCT 1) AS FILE_COUNT
		FROM STG_OPERSVODKA.STG_COMMENTS src
			INNER JOIN STG_OPERSVODKA.ETL_FILE_LOAD fl
				ON fl.SRC_SYSTEM = src.SRC_SYSTEM
					AND fl.FILE_NAME = src.SCENARIO
					AND fl.BUSINESS_DT = src.BUSINESS_DT) AS FILE_REG
;