INSERT INTO ODS_OPERSVODKA.DDS_LK_PLANT_PRODUCT_METRIC (
	HK_DDS_LK_PLANT_PRODUCT_METRIC,
	HK_DDS_HUB_PLANT_PRODUCT,
	HK_DDS_HUB_DATE,
	FILE_ID,
	LOAD_TS
)
SELECT
	HK_DDS_LK_PLANT_PRODUCT_METRIC ,
	HK_DDS_HUB_PLANT_PRODUCT,
	HK_DDS_HUB_DATE,
	FILE_ID,
	LOAD_TS
FROM ODS_OPERSVODKA.V_STG_STG_HQ_REPORT_DDS_LK_PLANT_PRODUCT_METRIC src
;