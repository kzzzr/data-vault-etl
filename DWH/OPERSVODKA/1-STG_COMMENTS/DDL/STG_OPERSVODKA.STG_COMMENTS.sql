DROP TABLE IF EXISTS STG_OPERSVODKA.STG_COMMENTS ;

CREATE TABLE IF NOT EXISTS STG_OPERSVODKA.STG_COMMENTS (
	SRC_SYSTEM VARCHAR(64) NOT NULL DEFAULT 'EXCEL FILE',
	SCENARIO VARCHAR(64) NOT NULL DEFAULT 'COMMENTS ON SHARED FOLDER W:',
	BUSINESS_DT DATE NOT NULL DEFAULT GETDATE(),
	ID VARCHAR(12) NOT NULL,
	DT DATE NOT NULL,
	PLANT VARCHAR(12),
	SUBPLANT VARCHAR(12),
	COMMENT VARCHAR(12000),
	TEMPERATURE NUMERIC(8,2)
)
ORDER BY ID
SEGMENTED BY HASH(MD5(ID::VARCHAR)) ALL NODES
;
