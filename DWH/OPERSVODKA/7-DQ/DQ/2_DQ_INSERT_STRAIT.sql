-- TRUNCATE TABLE ODS_OPERSVODKA.DM_DQ_STRAIT;
MERGE INTO ODS_OPERSVODKA.DM_DQ_STRAIT AS trg
	USING ODS_OPERSVODKA.DM_DQ_TEMP AS src ON
		src.CHECK_NUMBER = trg.CHECK_NUMBER
		AND isnull(src.DT, getdate()) = isnull(trg.DT, getdate())
		AND isnull(src.PLANT_PRODUCT_ID,'none') = isnull(trg.PLANT_PRODUCT_ID, 'none')
		AND isnull(src.TYPE, 'none') = isnull(trg.TYPE, 'none')
		AND isnull(src.VALUE::NUMERIC(18, 5), 1) = isnull(trg.VALUE, 1)
WHEN NOT MATCHED THEN INSERT (FILE_ID,CHECK_NUMBER,DT,PLANT_PRODUCT_ID,FACTORY,PLANT,PRODUCT,TYPE,VALUE,B_TYPE,B_VALUE,DELTA,COMMENT,ERR_TYPE)
VALUES (src.FILE_ID, src.CHECK_NUMBER,src.DT,src.PLANT_PRODUCT_ID,src.FACTORY,src.PLANT,src.PRODUCT,src.TYPE,src.VALUE,src.B_TYPE,src.B_VALUE,src.DELTA,src.COMMENT,src.ERR_TYPE);

--INSERT INTO ODS_OPERSVODKA.DM_DQ_STRAIT (FILE_ID, CHECK_NUMBER,DT,PLANT_PRODUCT_ID,FACTORY,PLANT,PRODUCT,TYPE,VALUE,B_TYPE,B_VALUE,DELTA,COMMENT,ERR_TYPE)
--SELECT FILE_ID, CHECK_NUMBER,DT,PLANT_PRODUCT_ID,FACTORY,PLANT,PRODUCT,"TYPE",VALUE,B_TYPE,B_VALUE,DELTA,COMMENT,ERR_TYPE FROM ODS_OPERSVODKA.DM_DQ_TEMP