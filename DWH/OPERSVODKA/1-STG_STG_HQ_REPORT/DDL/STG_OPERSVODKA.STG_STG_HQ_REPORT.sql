DROP TABLE IF EXISTS STG_OPERSVODKA.STG_STG_HQ_REPORT ;

CREATE TABLE IF NOT EXISTS STG_OPERSVODKA.STG_STG_HQ_REPORT (
	SRC_SYSTEM VARCHAR(64) NOT NULL DEFAULT 'SAP MII',
	SCENARIO VARCHAR(64) NOT NULL,
	BUSINESS_DT DATE NOT NULL DEFAULT GETDATE(),
	DT DATE NOT NULL,
	PLANT_PRODUCT_ID VARCHAR(64) NOT NULL,
	BP NUMERIC(18,5),
	PLAN NUMERIC(18,5),
	PPR NUMERIC(18,5),
	FORECAST NUMERIC(18,5),
	FACT NUMERIC(18,5)
)
ORDER BY PLANT_PRODUCT_ID,
	DT
SEGMENTED BY HASH(MD5(PLANT_PRODUCT_ID::VARCHAR)) ALL NODES
;
