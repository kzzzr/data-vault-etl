CREATE TABLE IF NOT EXISTS STG_OPERSVODKA.DQ_LOG (
	LOAD_TS TIMESTAMP,
	OBJECT VARCHAR(256),
    DQ_NAME VARCHAR(256),
	METRIC FLOAT,
    STATUS VARCHAR(64),
	DESCRIPTION VARCHAR(2048)
)
ORDER BY LOAD_TS, OBJECT
UNSEGMENTED ALL NODES
;
