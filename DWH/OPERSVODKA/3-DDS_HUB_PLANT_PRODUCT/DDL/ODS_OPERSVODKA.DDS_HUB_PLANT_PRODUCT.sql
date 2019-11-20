CREATE TABLE IF NOT EXISTS ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT (
	HK_DDS_HUB_PLANT_PRODUCT VARCHAR(32) NOT NULL,
	FILE_ID INTEGER NOT NULL,
	LOAD_TS TIMESTAMP NOT NULL,
	ID_PLANT_PRODUCT VARCHAR(64) NOT NULL,
	PRIMARY KEY (HK_DDS_HUB_PLANT_PRODUCT) ENABLED
)
ORDER BY HK_DDS_HUB_PLANT_PRODUCT
SEGMENTED BY HASH(HK_DDS_HUB_PLANT_PRODUCT) ALL NODES
;
