CREATE TABLE IF NOT EXISTS ODS_OPERSVODKA.ODS_COMMENTS (
	FILE_ID INTEGER NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
	SRC_SYSTEM VARCHAR(64) NOT NULL DEFAULT 'EXCEL FILE',
	SCENARIO VARCHAR(64) NOT NULL DEFAULT 'COMMENTS ON SHARED FOLDER W:',
	BUSINESS_DT DATE NOT NULL DEFAULT GETDATE(),
	ID VARCHAR(12) NOT NULL,
	DT DATE NOT NULL,
	PLANT VARCHAR(12),
	SUBPLANT VARCHAR(12),
	COMMENT VARCHAR(12000),
	TEMPERATURE NUMERIC(8,2),
	PRIMARY KEY (FILE_ID, ID) DISABLED
)
ORDER BY
	FILE_ID,
	ID
SEGMENTED BY HASH(MD5(ID::VARCHAR)) ALL NODES
PARTITION BY DT GROUP BY CALENDAR_HIERARCHY_DAY(DT, 1, 2)
;