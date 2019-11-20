--Очищаем табблицу бенчмарков для обновления
TRUNCATE TABLE ODS_OPERSVODKA.DDS_DQ_BENCHMARK;


--вставляем бенчмарки (считаются на основании основной витрины)
INSERT INTO ODS_OPERSVODKA.DDS_DQ_BENCHMARK ( PLANT_PRODUCT_ID, NAME, BTYPE, VALUE, PERIOD_FROM, PERIOD_TO, LOAD_TS)
WITH ROW_CTE AS (
SELECT ROW_NUMBER() over (PARTITION by PLANT_PRODUCT_ID ORDER by BP DESC) as BP_numer, ROW_NUMBER() over (PARTITION by PLANT_PRODUCT_ID ORDER by PLAN DESC) as PLAN_numer,ROW_NUMBER() over (PARTITION by PLANT_PRODUCT_ID ORDER by PPR DESC) as PPR_numer,  ROW_NUMBER() over (PARTITION by PLANT_PRODUCT_ID ORDER by FACT DESC) as FACT_numer, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT, BP, PLAN, PPR, FORECAST, FACT, BP_C, PLAN_C, PPR_C, 
FACT_C, DATE_DAY_BEFORE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
WHERE (1 = 1) and DT BETWEEN '2018-11-01' and GETDATE()-1 and DATE_DAY_BEFORE is not NULL and PLANT_PRODUCT_ID not in ('[MENGE_03_1-_01-M]', '[MENGE_03_png]') )
SELECT *, CURRENT_TIMESTAMP as LOAD_TS FROM (
SELECT PLANT_PRODUCT_ID, 'BP' as NAME, 'avg' as BTYPE, CAST(avg(BP) as NUMERIC(15,3)) as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
WHERE (1 = 1) and BP_numer < (select max(BP_numer) from ROW_CTE)*0.9 and BP_numer > (select max(BP_numer) from ROW_CTE)*0.1 and PRODUCT not in ('С3+/ПНГ', 'С3+/СОГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
SELECT PLANT_PRODUCT_ID, 'PLAN' as NAME, 'avg' as BTYPE, CAST(avg(PLAN) as NUMERIC(15,3)) as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
WHERE (1 = 1) and PLAN_numer < (select max(PLAN_numer) from ROW_CTE)*0.9 and PLAN_numer > (select max(PLAN_numer) from ROW_CTE)*0.1  and PRODUCT not in ('С3+/ПНГ', 'С3+/СОГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
(SELECT PLANT_PRODUCT_ID, 'PPR' as NAME, 'avg' as BTYPE, CAST(avg(PPR) as NUMERIC(15,3)) as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
WHERE (1 = 1) and PPR_numer < (select max(PPR_numer) from ROW_CTE)*0.9 and PPR_numer > (select max(PPR_numer) from ROW_CTE)*0.1  and PRODUCT not in ('С3+/ПНГ', 'С3+/СОГ')
GROUP BY PLANT_PRODUCT_ID)
UNION ALL
(SELECT PLANT_PRODUCT_ID, 'FACT' as NAME, 'avg' as BTYPE, CAST(avg(FACT) as NUMERIC(15,3)) as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and FACT_numer < (select max(FACT_numer) from ROW_CTE)*0.9 and FACT_numer > (select max(FACT_numer) from ROW_CTE)*0.1  and PRODUCT not in ('С3+/ПНГ', 'С3+/СОГ')
GROUP BY PLANT_PRODUCT_ID)
UNION ALL
SELECT CONCAT(CONCAT('[MENGE_', SUBSTRING(PLANT_PRODUCT_ID, 8, 2)),'_png]') , 'BP' as NAME, 'max' as BTYPE, 400 as VALUE,
CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and BP_numer < (select max(BP_numer) from ROW_CTE)*0.9 and BP_numer > (select max(BP_numer) from ROW_CTE)*0.1
and PRODUCT in ('С3+ в ПНГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
SELECT CONCAT(CONCAT('[MENGE_', SUBSTRING(PLANT_PRODUCT_ID, 8, 2)),'_sog]') , 'BP' as NAME, 'avg' as BTYPE, sum(BP)/sum(BP_C)*1000 as VALUE,
CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and BP_numer < (select max(BP_numer) from ROW_CTE)*0.9 and BP_numer > (select max(BP_numer) from ROW_CTE)*0.1
and PRODUCT in ('С3+ в СОГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
SELECT CONCAT(CONCAT('[MENGE_', SUBSTRING(PLANT_PRODUCT_ID, 8, 2)),'_png]') , 'PLAN' as NAME, 'max' as BTYPE, 400 as VALUE,
CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and PLAN_numer < (select max(PLAN_numer) from ROW_CTE)*0.9 and PLAN_numer > (select max(PLAN_numer) from ROW_CTE)*0.1
and PRODUCT in ('С3+ в ПНГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
SELECT CONCAT(CONCAT('[MENGE_', SUBSTRING(PLANT_PRODUCT_ID, 8, 2)),'_sog]') , 'PLAN' as NAME, 'avg' as BTYPE, sum(PLAN)/sum(PLAN_C)*1000 as VALUE,
CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and PLAN_numer < (select max(PLAN_numer) from ROW_CTE)*0.9 and PLAN_numer > (select max(PLAN_numer) from ROW_CTE)*0.1
and PRODUCT in ('С3+ в СОГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
SELECT CONCAT(CONCAT('[MENGE_', SUBSTRING(PLANT_PRODUCT_ID, 8, 2)),'_png]') , 'PPR' as NAME, 'max' as BTYPE, 400 as VALUE,
CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and PPR_numer < (select max(PPR_numer) from ROW_CTE)*0.9 and PPR_numer > (select max(PPR_numer) from ROW_CTE)*0.1
and PRODUCT in ('С3+ в ПНГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
SELECT CONCAT(CONCAT('[MENGE_', SUBSTRING(PLANT_PRODUCT_ID, 8, 2)),'_sog]') , 'PPR' as NAME, 'avg' as BTYPE, sum(PPR)/sum(PPR_C)*1000 as VALUE,
CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and PPR_numer < (select max(PPR_numer) from ROW_CTE)*0.9 and PPR_numer > (select max(PPR_numer) from ROW_CTE)*0.1
and PRODUCT in ('С3+ в СОГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
SELECT CONCAT(CONCAT('[MENGE_', SUBSTRING(PLANT_PRODUCT_ID, 8, 2)),'_png]') , 'FACT' as NAME, 'max' as BTYPE, 400 as VALUE,
CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and fact_numer < (select max(fact_numer) from ROW_CTE)*0.9 and fact_numer > (select max(fact_numer) from ROW_CTE)*0.1
and PRODUCT in ('С3+ в ПНГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL
SELECT CONCAT(CONCAT('[MENGE_', SUBSTRING(PLANT_PRODUCT_ID, 8, 2)),'_sog]') , 'FACT' as NAME, 'avg' as BTYPE, sum(FACT)/sum(FACT_C)*1000 as VALUE,
CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO
FROM ROW_CTE
where (1 = 1) and fact_numer < (select max(fact_numer) from ROW_CTE)*0.9 and fact_numer > (select max(fact_numer) from ROW_CTE)*0.1
and PRODUCT in ('С3+ в СОГ')
GROUP BY PLANT_PRODUCT_ID
UNION ALL ---------------- далее временные UNUON'ы грубых значений средних для С3+ в ПНГ и С3+/ПНГ, пока не починим данные из SAP, т.к. слишком много некорректных значений
SELECT '[MENGE_03_1-_01-M]' as PLANT_PRODUCT_ID, 'BP' as NAME, 'max' as BTYPE, 4000.0 as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO 
UNION ALL
SELECT '[MENGE_03_1-_01-M]' as PLANT_PRODUCT_ID, 'PLAN' as NAME, 'max' as BTYPE, 4000.0 as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO 
UNION ALL
SELECT '[MENGE_03_1-_01-M]' as PLANT_PRODUCT_ID, 'PPR' as NAME, 'max' as BTYPE, 4000.0 as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO 
UNION ALL
SELECT '[MENGE_03_1-_01-M]' as PLANT_PRODUCT_ID, 'FACT' as NAME, 'max' as BTYPE, 4000.0 as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO 
UNION ALL
SELECT '[MENGE_03_png]' as PLANT_PRODUCT_ID, 'BP' as NAME, 'max' as BTYPE, 400.0 as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO 
UNION ALL
SELECT '[MENGE_03_png]' as PLANT_PRODUCT_ID, 'PLAN' as NAME, 'max' as BTYPE, 400.0 as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO 
UNION ALL
SELECT '[MENGE_03_png]' as PLANT_PRODUCT_ID, 'PPR' as NAME, 'max' as BTYPE, 400.0 as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO 
UNION ALL
SELECT '[MENGE_03_png]' as PLANT_PRODUCT_ID, 'FACT' as NAME, 'max' as BTYPE, 400.0 as VALUE, CAST('2018-11-01' as DATE) as  PERIOD_FROM, cast(GETDATE()-1 as DATE) as PERIOD_TO 
)allias