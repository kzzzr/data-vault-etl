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
	, 'STG_COMMENTS' AS OBJECT
	, 'COMPARE SOURCE AND STAGE' AS DQ_NAME
	, SOURCE_COUNT.ROWCOUNT - STAGE_COUNT.ROWCOUNT AS METRIC
	, CASE (SOURCE_COUNT.ROWCOUNT - STAGE_COUNT.ROWCOUNT) WHEN 0 THEN 'OK' ELSE 'WARN' END AS STATUS
	, 'SOURCE.ROWCOUNT - STAGE.ROWCOUNT. OK=0. COMPARE NUMBER OF ROWS IN SOURCE AND NUMBER OF ROWS LOADED TO STAGE' AS DESCRIPTION
FROM STG_OPERSVODKA.ETL_LOAD_LOG
	CROSS JOIN (SELECT count(1) AS ROWCOUNT FROM STG_OPERSVODKA.STG_COMMENTS) AS STAGE_COUNT
	CROSS JOIN (SELECT ROWCOUNT FROM STG_OPERSVODKA.ETL_LOAD_LOG
				WHERE OBJECT = 'STG_COMMENTS'
				AND STEP = 'LOAD FILE'
				LIMIT 1 OVER (PARTITION BY OBJECT ORDER BY LOAD_TS DESC)) AS SOURCE_COUNT
;
