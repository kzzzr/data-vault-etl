INSERT INTO STG_OPERSVODKA.ETL_FILE_LOAD (
	SRC_SYSTEM ,
	FILE_NAME ,
	BUSINESS_DT ,
	LOAD_TS
)
SELECT DISTINCT
	src.SRC_SYSTEM AS SRC_SYSTEM ,
	src.SCENARIO AS FILE_NAME ,
	src.BUSINESS_DT AS BUSINESS_DT ,
	GETDATE()
FROM STG_OPERSVODKA.STG_COMMENTS src
;
