INSERT INTO ODS_OPERSVODKA.DDS_HUB_DUMMY_CALENDAR (
	HK_DDS_HUB_DUMMY_CALENDAR ,
	FILE_ID ,
	LOAD_TS ,
	ID
)
SELECT
	src.HK_DDS_HUB_DUMMY_CALENDAR ,
	src.FILE_ID ,
	src.LOAD_TS ,
	src.ID
FROM ODS_OPERSVODKA.V_STG_DUMMY_CALENDAR_DDS_HUB_DUMMY_CALENDAR src
;