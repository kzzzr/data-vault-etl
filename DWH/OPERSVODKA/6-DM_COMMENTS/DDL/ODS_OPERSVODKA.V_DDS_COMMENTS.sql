CREATE OR REPLACE VIEW ODS_OPERSVODKA.V_DDS_COMMENTS AS
SELECT 
	hub_comm.FILE_ID,
	hub_comm.LOAD_TS,
	hub_comm.ID,
	hub_comm.DT,
	hst_comm.PLANT,
	hst_comm.SUBPLANT,
	hst_comm.COMMENT,
	hst_comm.TEMPERATURE
FROM ODS_OPERSVODKA.DDS_HUB_COMMENTS hub_comm
	INNER JOIN ODS_OPERSVODKA.V_DDS_HST_COMMENTS hst_comm ON hst_comm.HK_DDS_HUB_COMMENTS = hub_comm.HK_DDS_HUB_COMMENTS
;