INSERT INTO ODS_OPERSVODKA.DDS_HST_DUMMY_CALENDAR (
	HK_DDS_HUB_DUMMY_CALENDAR ,
	FILE_ID ,
	LOAD_TS ,
	HASHDIFF ,
	DATE_TODAY,
	DATE_DAY_BEFORE,
	DATE_MONTH_BEFORE,
	DATE_YEAR_BEFORE
)
SELECT
	HK_DDS_HUB_DUMMY_CALENDAR ,
	FILE_ID ,
	LOAD_TS ,
	HASHDIFF ,
	DATE_TODAY,
	DATE_DAY_BEFORE,
	DATE_MONTH_BEFORE,
	DATE_YEAR_BEFORE
FROM ODS_OPERSVODKA.V_STG_DUMMY_CALENDAR_DDS_HST_DUMMY_CALENDAR src
;
