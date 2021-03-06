CREATE TABLE IF NOT EXISTS ODS_OPERSVODKA.DDS_HST_DUMMY_CALENDAR (
	HK_DDS_HUB_DUMMY_CALENDAR VARCHAR(32) NOT NULL CONSTRAINT HK_DDS_HUB_DUMMY_CALENDAR REFERENCES ODS_OPERSVODKA.DDS_HUB_DUMMY_CALENDAR (HK_DDS_HUB_DUMMY_CALENDAR) ,
	FILE_ID INTEGER NOT NULL ,
	LOAD_TS TIMESTAMP NOT NULL ,
	HASHDIFF VARCHAR(32) NOT NULL ,
	DATE_TODAY DATE,
	DATE_DAY_BEFORE DATE,
	DATE_MONTH_BEFORE DATE,
	DATE_YEAR_BEFORE DATE ,
	PRIMARY KEY (HK_DDS_HUB_DUMMY_CALENDAR, HASHDIFF) ENABLED
)
ORDER BY
	HK_DDS_HUB_DUMMY_CALENDAR ,
	LOAD_TS
SEGMENTED BY HASH(HK_DDS_HUB_DUMMY_CALENDAR) ALL NODES
;
