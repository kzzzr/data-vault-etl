TRUNCATE TABLE ODS_OPERSVODKA.DM_DQ_REFERENCE;
INSERT into ODS_OPERSVODKA.DM_DQ_REFERENCE (NUM, CHECK_NAME, CHECK_TYPE, CHECK_DESC, CHECK_TABLE, CHECK_ROW, CRITERION_OK, CRITERION_WARN, CRITERION_ERROR, LOAD_TS)
SELECT 1, 'BP BELOW 0 CHECK', 'CUSTOM', 'Проверка на значения БП меньше 0', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'BP', 'BP >= 0', '', 'BP < 0', CURRENT_TIMESTAMP UNION ALL
SELECT 2, 'PLAN BELOW 0 CHECK', 'CUSTOM', 'Проверка на значения ПЛАН меньше 0', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PLAN', 'PLAN >= 0', '', 'PLAN < 0', CURRENT_TIMESTAMP UNION ALL
SELECT 3, 'PPR BELOW 0 CHECK', 'CUSTOM', 'Проверка на значения ППР меньше 0', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PPR', 'PPR >= 0', '', 'PPR < 0', CURRENT_TIMESTAMP UNION ALL
SELECT 4, 'FACT BELOW 0 CHECK', 'CUSTOM', 'Проверка на значения ФАКТ меньше 0', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'FACT', 'FACT >= 0', '', 'FACT < 0', CURRENT_TIMESTAMP UNION ALL
SELECT 5, 'BP DEVIATIONS CHECK', 'CUSTOM', 'Проверка отклонений БП от средних значений в большую сторону', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'BP', 'С3+/ПНГ: показатель меньше максимального установленного
С3+/СОГ: показатель меньше 200% от среднего
Остальные: Меньше 150% от среднего', 'С3+/ПНГ: показатель больше максимального установленного
С3+/СОГ: показатель больше 200% от среднего
Остальные: больше 175% от среднего', 'ПНГ, ШФЛУ, БГС, ПБТ, УВС, СОГ: больше 300% от среднего', CURRENT_TIMESTAMP UNION ALL
SELECT 6, 'PLAN DEVIATIONS CHECK', 'CUSTOM', 'Проверка отклонений ПЛАН от средних значений в большую сторону', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PLAN', 'С3+/ПНГ: показатель меньше максимального установленного
С3+/СОГ: показатель меньше 200% от среднего
Остальные: Меньше 150% от среднего', 'С3+/ПНГ: показатель больше максимального установленного
С3+/СОГ: показатель больше 200% от среднего
Остальные: больше 150% от среднего', 'ПНГ, ШФЛУ, БГС, ПБТ, УВС, СОГ: больше 300% от среднего', CURRENT_TIMESTAMP UNION ALL
SELECT 7, 'PPR DEVIATIONS CHECK', 'CUSTOM', 'Проверка отклонений ППР от средних значений в большую сторону', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PPR', 'С3+/ПНГ: показатель меньше максимального установленного
С3+/СОГ: показатель меньше 200% от среднего
Остальные: Меньше 150% от среднего', 'С3+/ПНГ: показатель больше максимального установленного
С3+/СОГ: показатель больше 200% от среднего
Остальные: больше 150% от среднего', 'ПНГ, ШФЛУ, БГС, ПБТ, УВС, СОГ: больше 300% от среднего', CURRENT_TIMESTAMP UNION ALL
SELECT 8, 'FACT DEVIATIONS CHECK', 'CUSTOM', 'Проверка отклонений ФАКТ от средних значений в большую сторону', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'FACT', 'С3+/ПНГ: показатель меньше максимального установленного
С3+/СОГ: показатель меньше 200% от среднего
Остальные: Меньше 175% от среднего', 'С3+/ПНГ: показатель больше максимального установленного
С3+/СОГ: показатель больше 200% от среднего
Остальные: больше 175% от среднего', 'ПНГ, ШФЛУ, БГС, ПБТ, УВС, СОГ: больше 300% от среднего', CURRENT_TIMESTAMP UNION ALL
SELECT 9, 'BP IS NOT ZERO', 'CUSTOM', 'Проверка на нулевые или заниженные показатели БП для ПНГ/УВС/СОГ (кроме случаев ухода на ремонт/простой)', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'BP', '', 'Один из показателей < 10% от среднего, остальные > 10% от среднего', '', CURRENT_TIMESTAMP UNION ALL
SELECT 10, 'PLAN IS NOT ZERO', 'CUSTOM', 'Проверка на нулевые или заниженные показатели ПЛАН для ПНГ/УВС/СОГ (кроме случаев ухода на ремонт/простой)', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PLAN', '', 'Один из показателей < 10% от среднего, остальные > 10% от среднего', '', CURRENT_TIMESTAMP UNION ALL
SELECT 11, 'PPR IS NOT ZERO', 'CUSTOM', 'Проверка на нулевые или заниженные показатели ППР для ПНГ/УВС/СОГ (кроме случаев ухода на ремонт/простой)', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PPR', '', 'Один из показателей < 10% от среднего, остальные > 10% от среднего', '', CURRENT_TIMESTAMP UNION ALL
SELECT 12, 'FACT IS NOT ZERO', 'CUSTOM', 'Проверка на нулевые или заниженные показатели ФАКТ для ПНГ/УВС/СОГ (кроме случаев ухода на ремонт/простой)', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'FACT', '', 'Один из показателей < 10% от среднего, остальные > 10% от среднего', '', CURRENT_TIMESTAMP UNION ALL
SELECT 13, 'BP PNG and SOG', 'CUSTOM', 'БП ПНГ на входе  больше чем СОГ на выходе', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'BP', 'PNG BP >= SOG BP', 'PNG BP < SOG BP', '', CURRENT_TIMESTAMP UNION ALL
SELECT 14, 'PLAN PNG and SOG', 'CUSTOM', 'ПЛАН ПНГ на входе  больше чем СОГ на выходе', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PLAN', 'PNG PLAN >= SOG PLAN', 'PNG PLAN < SOG PLAN', '', CURRENT_TIMESTAMP UNION ALL
SELECT 15, 'PPR PNG and SOG', 'CUSTOM', 'ППР ПНГ на входе  больше чем СОГ на выходе', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PPR', 'PNG PPR >= SOG PPR', 'PNG PPR < SOG PPR', '', CURRENT_TIMESTAMP UNION ALL
SELECT 16, 'FACT PNG and SOG', 'CUSTOM', 'ФАКТ ПНГ на входе  больше чем СОГ на выходе', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'FACT', 'PNG FACT >= SOG FACT', 'PNG FACT < SOG FACT', '', CURRENT_TIMESTAMP UNION ALL
SELECT 17, 'BP C3 and UVS', 'CUSTOM', 'БП масса С3+ в ПНГ на входе  больше чем УВС на выходе', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'BP', 'С3+ PNG BP >= UVS BP', 'С3+ PNG BP < UVS BP', '', CURRENT_TIMESTAMP UNION ALL
SELECT 18, 'PLAN C3 and UVS', 'CUSTOM', 'ПЛАН масса С3+ в ПНГ на входе  больше чем УВС на выходе', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PLAN', 'С3+ PNG PLAN >= UVS PLAN', 'С3+ PNG PLAN < UVS PLAN', '', CURRENT_TIMESTAMP UNION ALL
SELECT 19, 'PPR C3 and UVS', 'CUSTOM', 'ППР масса С3+ в ПНГ на входе  больше чем УВС на выходе', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PPR', 'С3+ PNG PPR >= UVS PPR', 'С3+ PNG PPR < UVS PPR', '', CURRENT_TIMESTAMP UNION ALL
SELECT 20, 'FACT C3 and UVS', 'CUSTOM', 'ФАКТ масса С3+ в ПНГ на входе  больше чем УВС на выходе', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'FACT', 'С3+ PNG FACT >= UVS FACT', 'С3+ PNG FACT < UVS FACT', '', CURRENT_TIMESTAMP UNION ALL
SELECT 21, 'BP IS NOT NULL', 'AUTOMATIC', 'БП не равен NULL', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'BP', 'BP is not NULL', '', 'BP is NULL', CURRENT_TIMESTAMP UNION ALL
SELECT 22, 'PLAN IS NOT NULL', 'AUTOMATIC', 'ПЛАН не равен NULL', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PLAN', 'PLAN is not NULL', '', 'PLAN is NULL', CURRENT_TIMESTAMP UNION ALL
SELECT 23, 'PPR IS NOT NULL', 'AUTOMATIC', 'ППР не равен NULL', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PPR', 'PPR is not NULL', '', 'PPR is NULL', CURRENT_TIMESTAMP UNION ALL
SELECT 24, 'FACT IS NOT NULL', 'AUTOMATIC', 'ФАКТ не равен NULL', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'FACT', 'FACT is not NULL', '', 'FACT is NULL', CURRENT_TIMESTAMP UNION ALL
SELECT 25, 'DUPLICATE TEST MAIN', 'AUTOMATIC', 'Проверка на отсутствие дублей в основной витрине', 'ODS_OPERSVODKA.DM_OPER_MAIN', '', 'Row number = 0', '', 'Row number > 0', CURRENT_TIMESTAMP UNION ALL
SELECT 26, 'DUPLICATE TEST STRAIT', 'AUTOMATIC', 'Проверка на отсутствие дублей в узкой витрине', 'ODS_OPERSVODKA.DM_OPER_STRAIT', '', 'Row number = 0', '', 'Row number > 0', CURRENT_TIMESTAMP UNION ALL
SELECT 27, 'ROWS COUNT CHECK', 'AUTOMATIC', 'Проверка на "добавление" синтетических дат ("добивание месяца до 31 дня")', 'ODS_OPERSVODKA.DM_OPER_MAIN', '', 'COUNT(*)%31 = 0', '', 'COUNT(*)%31 != 0', CURRENT_TIMESTAMP UNION ALL
SELECT 28, 'MB YB FORECAST NULL', 'AUTOMATIC', 'Прогнозы прошлых периодов NULL', 'ODS_OPERSVODKA.DM_OPER_MAIN', '', '(not FORECAST_YB is NULL 
or not FORECAST_C_YB is NULL
or not FORECAST_MB is NULL 
or not FORECAST_C_MB is NULL)', '', 'not (not FORECAST_YB is NULL 
or not FORECAST_C_YB is NULL
or not FORECAST_MB is NULL 
or not FORECAST_C_MB is NULL)', CURRENT_TIMESTAMP UNION ALL
SELECT 29, 'SYNTHETICK CHECK 1', 'AUTOMATIC', 'Для синтетических дат, показатели текущего периода и периода год назад равны NULL', 'ODS_OPERSVODKA.DM_OPER_MAIN', '', 'Row number = 0', '', 'Row number > 0', CURRENT_TIMESTAMP UNION ALL
SELECT 30, 'SYNTHETICK CHECK 2', 'AUTOMATIC', 'Для месяцев следующих за месяцами где меньше 31 дня, соотвествующие значения NULL', 'ODS_OPERSVODKA.DM_OPER_MAIN', '', 'Row number = 0', '', 'Row number > 0', CURRENT_TIMESTAMP UNION ALL
SELECT 31, 'STRAIT MAIN COMPRASION', 'AUTOMATIC', 'Проверка на совпадение значений в основной и узкой витрине', 'ODS_OPERSVODKA.DM_OPER_MAIN, ODS_OPERSVODKA.DM_OPER_STRAIT', '', 'Row number = 1', '', 'Row number > 1', CURRENT_TIMESTAMP UNION ALL
SELECT 32, 'STRAIT ODS COMPRASION', 'AUTOMATIC', 'Проверка на совпадение значений в узкой витрине и слое ODS', 'ODS_OPERSVODKA.DM_OPER_STRAIT, ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK', '', 'Row number = 2', '', 'Row number > 2', CURRENT_TIMESTAMP UNION ALL
SELECT 33, 'ODS STG COMPRASION', 'AUTOMATIC', 'Проверка на совпадение значений в слое STG и слое ODS', 'ODS_OPERSVODKA.STG_ZPP_PROD_REPORT_TRANSP, ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK', '', 'Row number = 3', '', 'Row number > 3', CURRENT_TIMESTAMP UNION ALL
SELECT 34, 'PNG PPR PLAN MONTH COMPARE', 'CUSTOM', 'Сравнение месячных сумм ППР и ПЛАН по продуктам (ПНГ, ШФЛУ, БГС,  ПБТ, СОГ)', 'ODS_OPERSVODKA.DM_OPER_MAIN', 'PLAN, PPR', 'Сумма ППР и ПЛАН по месяцу равны', 'Сумма ППР и ПЛАН по месяцу не равны', '', CURRENT_TIMESTAMP UNION ALL
SELECT 35, 'UVS FORMULA MONTH COMPARE', 'CUSTOM', 'Сравнение месячных сумм УВС(SAP) и УВС по формуле( ШФЛУ+ПБТ+БГС+БГС(неконд)+ПТ)', 'ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK', '', 'Сумма УВС(SAP) и УВС(формула) по месяцу равны', 'Сумма УВС(SAP) и УВС(формула) по месяцу не равны', '', CURRENT_TIMESTAMP UNION ALL
SELECT 36, 'NVGPK PNG FORMULA COMPARE', 'CUSTOM', 'НВГПК. Сравнение показателей ПНГ (SAP) и ПНГ по формуле((НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг))', 'ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK', '', 'Сумма ПНГ(SAP) и ПНГ(формула) на НВГПК равны', 'Сумма ПНГ(SAP) и ПНГ(формула) на НВГПК не равны', '', CURRENT_TIMESTAMP UNION ALL
SELECT 37, 'BGPK PNG FORMULA COMPARE', 'CUSTOM', 'БГПК. Сравнение показателей ПНГ (SAP) и ПНГ по формуле(БГПЗ:ПНГ+БГПЗ:НГ+БКС:ПНГ-БКС:НГ(отг)+ВКС:ПНГ-ВКС:НГ(отг))', 'ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK', '', 'Сумма ПНГ(SAP) и ПНГ(формула) на БГПК равны', 'Сумма ПНГ(SAP) и ПНГ(формула) на БГПК не равны', '', CURRENT_TIMESTAMP UNION ALL
SELECT 38, 'MGPZ PNG FORMULA COMPARE', 'CUSTOM', 'МГПЗ. Сравнение показателей ПНГ (SAP) и ПНГ по формуле((МГПЗ:ПНГ+ХКС:ПНГ)', 'ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK', '', 'Сумма ПНГ(SAP) и ПНГ(формула) на МГПЗ равны', 'Сумма ПНГ(SAP) и ПНГ(формула) на МГПЗ не равны', '', CURRENT_TIMESTAMP UNION ALL
SELECT 39, 'VGPZ PNG FORMULA COMPARE', 'CUSTOM', 'ВГПЗ. Сравнение показателей ПНГ (SAP) и ПНГ по формуле((ВГПЗ:ПНГ+ВГПЗ:НГ+ВяКС:ПНГ-ВяКС:НГ(отг))', 'ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK', '', 'Сумма ПНГ(SAP) и ПНГ(формула) на ВГПЗ равны', 'Сумма ПНГ(SAP) и ПНГ(формула) на ВГПЗ не равны', '', CURRENT_TIMESTAMP
