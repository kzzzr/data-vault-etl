CREATE TABLE IF NOT EXISTS ODS_OPERSVODKA.DM_DQ_REFERENCE(
NUM				INTEGER,
CHECK_NAME		VARCHAR(64),
CHECK_TYPE		VARCHAR(64), 
CHECK_DESC		VARCHAR(256),
CHECK_TABLE		VARCHAR(256), 
CHECK_ROW		VARCHAR(64), 
CRITERION_OK	VARCHAR(256),
CRITERION_WARN	VARCHAR(256),
CRITERION_ERROR	VARCHAR(256), 
LOAD_TS 		TIMESTAMP, 
PRIMARY KEY		(NUM))
ORDER BY NUM UNSEGMENTED ALL NODES;