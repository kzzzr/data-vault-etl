CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_UVS AS
SELECT
	shflu.FILE_ID
	, shflu.LOAD_TS
	, shflu.DT
	, rf.ID_PLANT_PRODUCT
	, rf.FACTORY
	, rf.PLANT
	, rf.PRODUCT
	, shflu.BP + isnull(bgs.BP, 0) + isnull(pbt.BP, 0) AS BP
	, shflu.PPR + isnull(bgs.PPR, 0) + isnull(pbt.PPR, 0) AS PPR
	, shflu.PLAN + isnull(bgs.PLAN, 0) + isnull(pbt.PLAN, 0) AS PLAN
	, shflu.FORECAST + isnull(bgs.FORECAST, 0) + isnull(pbt.FORECAST, 0) AS FORECAST
	, shflu.FACT + isnull(bgs.FACT, 0) + isnull(pbt.FACT, 0) AS FACT
FROM ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE AS rf
	LEFT JOIN ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE AS shflu ON shflu.plant = rf.plant AND shflu.product = 'ШФЛУ'
	LEFT JOIN ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE AS bgs ON bgs.plant = rf.plant AND bgs.product = 'БГС' AND bgs.DT = shflu.DT
	LEFT JOIN ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE AS pbt ON pbt.plant = rf.plant AND pbt.product = 'ПБТ' AND pbt.DT = shflu.DT
WHERE rf.product = 'УВС' 
	AND rf.ID_PLANT_PRODUCT <> '[MENGE_6]'
;