CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_PNG AS
SELECT	
	png.FILE_ID
	, png.LOAD_TS
	, png.DT
	, rf.ID_PLANT_PRODUCT
	, rf.FACTORY
	, rf.PLANT
	, rf.PRODUCT
	, (sz.BP / NULLIF(png.BP, 0) * 1000)::NUMERIC(18, 5) AS BP
	, (sz.PPR / NULLIF(png.PPR, 0) * 1000)::NUMERIC(18, 5) AS PPR
	, (sz.PLAN / NULLIF(png.PLAN, 0) * 1000)::NUMERIC(18, 5) AS PLAN
	, (sz.FORECAST / NULLIF(png.FORECAST, 0) * 1000)::NUMERIC(18, 5) AS FORECAST
	, (sz.FACT / NULLIF(png.FACT, 0) * 1000)::NUMERIC(18, 5) AS FACT
FROM ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE AS rf
	LEFT JOIN ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE AS png ON png.plant = rf.plant AND png.product = 'ПНГ'
	LEFT JOIN ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE AS sz ON sz.plant = rf.plant AND sz.product = 'С3+ в ПНГ' AND sz.DT = png.DT
WHERE rf.product = 'С3+/ПНГ'
;