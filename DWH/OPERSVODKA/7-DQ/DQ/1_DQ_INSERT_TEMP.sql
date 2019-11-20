-- очистить таблицу TEMP для новых данных
TRUNCATE TABLE ODS_OPERSVODKA.DM_DQ_TEMP;

-- вставить результаты проверки: UNION ALL всех скриптов проверок + добавление актуального FILE_ID ко всем строкам)
INSERT INTO ODS_OPERSVODKA.DM_DQ_TEMP (FILE_ID,CHECK_NUMBER,DT,PLANT_PRODUCT_ID,FACTORY,PLANT,PRODUCT,TYPE,VALUE,B_TYPE,B_VALUE,DELTA,COMMENT,ERR_TYPE)
SELECT DISTINCT (select max(FILE_ID) from ODS_OPERSVODKA.ETL_FILE_LOAD where FILE_NAME = 'STG_ZPP_PROD_REPORT_TRANSP') as FILE_ID, main.* FROM 
(
SELECT 1 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'BP' as TYPE, BP as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(PLANT, CONCAT(' ', CONCAT(PRODUCT, CONCAT(CONCAT(' BP = ',TO_CHAR(BP)), ' ERROR: Отрицательное значение')))) as COMMENT, 'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
where (1 = 1)
and BP < 0
and PRODUCT not in ('С3+ в ПНГ', 'С3+ в СОГ')
UNION ALL
SELECT 2 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'PLAN' as TYPE, PLAN as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(PLANT, CONCAT(' ', CONCAT(PRODUCT, CONCAT(CONCAT(' PLAN = ',TO_CHAR(PLAN)), ' ERROR: Отрицательное значение')))) as COMMENT, 'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
where (1 = 1)
and PLAN < 0
and PRODUCT not in ('С3+ в ПНГ', 'С3+ в СОГ')
UNION ALL
SELECT 3 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'PPR' as TYPE, PPR as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(PLANT, CONCAT(' ', CONCAT(PRODUCT, CONCAT(CONCAT(' PPR = ',TO_CHAR(PPR)), ' ERROR: Отрицательное значение')))) as COMMENT, 'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
where (1 = 1)
and PPR < 0
and PRODUCT not in ('С3+ в ПНГ', 'С3+ в СОГ')
UNION ALL
SELECT 4 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'FACT' as TYPE, FACT as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(PLANT, CONCAT(' ', CONCAT(PRODUCT, CONCAT(CONCAT(' FACT = ',TO_CHAR(FACT)), ' ERROR: Отрицательное значение')))) as COMMENT, 'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
where (1 = 1)
and FACT < 0
and PRODUCT not in ('С3+ в ПНГ', 'С3+ в СОГ')
UNION ALL
SELECT 5 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'BP' as TYPE, main.BP as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, 
CAST((main.BP/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' БП = ',TO_CHAR(cast(main.BP as NUMERIC(18,2)))), ' avg = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.BP/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
CASE 
 WHEN main.BP < (reff.VALUE * 3) THEN ' WARNING, отклонение от среднего БП' 
 WHEN main.BP >= (reff.VALUE * 3) THEN ' ERROR отклонение от среднего БП'
END)))) as COMMENT,
CASE 
 WHEN main.BP < (reff.VALUE * 3) THEN 'WARNING' 
 WHEN main.BP >= (reff.VALUE * 3) THEN 'ERROR'
END as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'BP' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.BP - (reff.VALUE * 1.5) >= 0
and main.BP != 0
and main.PRODUCT not in ('С3+ в ПНГ', 'С3+ в СОГ', 'С3+/СОГ', 'С3+/ПНГ')
and main.PLANT not in ('ALL')
UNION ALL
SELECT 5 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'BP' as TYPE, main.BP as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, CAST((main.BP/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' БП = ',TO_CHAR(cast(main.BP as NUMERIC(18,2)))), ' avg = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.BP/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
' WARNING, отклонение от среднего БП')))) as COMMENT,
 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'BP' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.BP - (reff.VALUE * 2) >= 0
and main.PRODUCT in ('С3+/СОГ')
and main.BP != 0
and main.PLANT not in ('ALL')
UNION ALL
SELECT 5 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'BP' as TYPE, main.BP as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, CAST((main.BP/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' БП = ',TO_CHAR(cast(main.BP as NUMERIC(18,2)))), ' MAX = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.BP/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
' WARNING, отклонение от максимального допустимого БП')))) as COMMENT,
 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'BP' and reff.BTYPE = 'max' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.BP > reff.VALUE
and main.PRODUCT in ('С3+/ПНГ')
and main.BP != 0
and main.PLANT not in ('ALL')
UNION ALL
SELECT 6 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'PLAN' as TYPE, main.PLAN as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, 
CAST((main.PLAN/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ПЛАН = ',TO_CHAR(cast(main.PLAN as NUMERIC(18,2)))), ' avg = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.PLAN/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
CASE 
 WHEN main.PLAN < (reff.VALUE * 3) THEN ' WARNING, отклонение от среднего ПЛАНА' 
 WHEN main.PLAN >= (reff.VALUE * 3) THEN ' ERROR отклонение от среднего ПЛАНА'
END)))) as COMMENT,
CASE 
 WHEN main.PLAN < (reff.VALUE * 3) THEN 'WARNING' 
 WHEN main.PLAN >= (reff.VALUE * 3) THEN 'ERROR'
END as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'PLAN' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.PLAN - (reff.VALUE * 1.5) >= 0
and main.PLAN != 0
and main.PRODUCT not in ('С3+ в ПНГ', 'С3+ в СОГ', 'С3+/СОГ', 'С3+/ПНГ')
and main.PLANT not in ('ALL')
UNION ALL
SELECT 6 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'PLAN' as TYPE, main.PLAN as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, CAST((main.PLAN/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ПЛАН = ',TO_CHAR(cast(main.PLAN as NUMERIC(18,2)))), ' avg = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.PLAN/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
' WARNING, отклонение от среднего ПЛАНА')))) as COMMENT,
 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'PLAN' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.PLAN - (reff.VALUE * 2) >= 0
and main.PRODUCT in ('С3+/СОГ')
and main.PLAN != 0
and main.PLANT not in ('ALL')
UNION ALL
SELECT 6 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'PLAN' as TYPE, main.PLAN as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, CAST((main.PLAN/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ПЛАН = ',TO_CHAR(cast(main.PLAN as NUMERIC(18,2)))), ' MAX = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.PLAN/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
' WARNING, отклонение от максимального допустимого ПЛАНА')))) as COMMENT,
 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'PLAN' and reff.BTYPE = 'max' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.PLAN > reff.VALUE
and main.PRODUCT in ('С3+/ПНГ')
and main.PLAN != 0
and main.PLANT not in ('ALL')
UNION ALL
SELECT 7 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'PPR' as TYPE, main.PPR as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, 
CAST((main.PPR/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ППР = ',TO_CHAR(cast(main.PPR as NUMERIC(18,2)))), ' avg = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.PPR/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
CASE 
 WHEN main.PPR < (reff.VALUE * 3) THEN ' WARNING, отклонение от среднего ППР' 
 WHEN main.PPR >= (reff.VALUE * 3) THEN ' ERROR отклонение от среднего ППР'
END)))) as COMMENT,
CASE 
 WHEN main.PPR < (reff.VALUE * 3) THEN 'WARNING' 
 WHEN main.PPR >= (reff.VALUE * 3) THEN 'ERROR'
END as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'PPR' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.PPR - (reff.VALUE * 1.5) >= 0
and main.PPR != 0
and main.PRODUCT not in ('С3+ в ПНГ', 'С3+ в СОГ', 'С3+/СОГ', 'С3+/ПНГ')
and main.PLANT not in ('ALL')
UNION ALL
SELECT 7 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'PPR' as TYPE, main.PPR as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, CAST((main.PPR/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ППР = ',TO_CHAR(cast(main.PPR as NUMERIC(18,2)))), ' avg = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.PPR/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
' WARNING, отклонение от среднего ППР')))) as COMMENT,
 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'PPR' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.PPR - (reff.VALUE * 2) >= 0
and main.PRODUCT in ('С3+/СОГ')
and main.PPR != 0
and main.PLANT not in ('ALL')
UNION ALL
SELECT 7 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'PPR' as TYPE, main.PPR as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, CAST((main.PPR/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ППР = ',TO_CHAR(cast(main.PPR as NUMERIC(18,2)))), ' MAX = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.PPR/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
' WARNING, отклонение от максимального допустимого ППР')))) as COMMENT,
 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'PPR' and reff.BTYPE = 'max' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.PPR > reff.VALUE
and main.PRODUCT in ('С3+/ПНГ')
and main.PPR != 0
and main.PLANT not in ('ALL')
UNION ALL
SELECT 8 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'FACT' as TYPE, main.FACT as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, 
CAST((main.FACT/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ФАКТ = ',TO_CHAR(cast(main.FACT as NUMERIC(18,2)))), ' avg = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.FACT/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
CASE 
 WHEN main.FACT < (reff.VALUE * 3) THEN ' WARNING, отклонение от среднего ФАКТА' 
 WHEN main.FACT >= (reff.VALUE * 3) THEN ' ERROR отклонение от среднего ФАКТА'
END)))) as COMMENT,
CASE 
 WHEN main.FACT < (reff.VALUE * 3) THEN 'WARNING' 
 WHEN main.FACT >= (reff.VALUE * 3) THEN 'ERROR'
END as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'FACT' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.FACT - (reff.VALUE * 1.5) >= 0
and main.FACT != 0
and main.PRODUCT not in ('С3+ в ПНГ', 'С3+ в СОГ', 'С3+/СОГ', 'С3+/ПНГ')
and main.PLANT not in ('ALL')
UNION ALL
SELECT 8 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'FACT' as TYPE, main.FACT as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, CAST((main.FACT/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ФАКТ = ',TO_CHAR(cast(main.FACT as NUMERIC(18,2)))), ' avg = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.FACT/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
' WARNING, отклонение от среднего ФАКТА')))) as COMMENT,
 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'FACT' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.FACT - (reff.VALUE * 2) >= 0
and main.PRODUCT in ('С3+/СОГ')
and main.FACT != 0
and main.PLANT not in ('ALL')
UNION ALL
SELECT 8 as CHECK_NUMBER, main.DT, main.PLANT_PRODUCT_ID, main.FACTORY, main.PLANT, main.PRODUCT,
'FACT' as TYPE, main.FACT as VALUE, reff.BTYPE as B_TYPE, cast(reff.VALUE as NUMERIC(18,2)) as B_VALUE, CAST((main.FACT/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2)) as DELTA,
CONCAT(main.PLANT, CONCAT(' ', CONCAT(main.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(' ФАКТ = ',TO_CHAR(cast(main.FACT as NUMERIC(18,2)))), ' MAX = '), 
cast(reff.VALUE as NUMERIC(18,2))), ' Delta = '), CAST((main.FACT/(case when reff.VALUE = 0 then null else reff.value end)*100 - 100) as NUMERIC(18,2))), '%'),
' WARNING, отклонение от максимального допустимого ФАКТА')))) as COMMENT,
 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as main
JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as reff ON main.PLANT_PRODUCT_ID = reff.PLANT_PRODUCT_ID and reff.NAME = 'FACT' and reff.BTYPE = 'max' 
where (1 = 1)
and main.DATE_DAY_BEFORE is not NULL
and main.FACT > reff.VALUE
and main.PRODUCT in ('С3+/ПНГ')
and main.FACT != 0
and main.PLANT not in ('ALL')
UNION ALL
SELECT 9 as CHECK_NUMBER, a.DT, a.PLANT_PRODUCT_ID, a.FACTORY, a.PLANT, NULL as PRODUCT, 'BP' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(a.PLANT, ' Заниженные показатели БП') as COMMENT,
'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as a
LEFT JOIN ODS_OPERSVODKA.DM_OPER_MAIN as b ON a.DT = b.DT and SUBSTRING(a.PLANT_PRODUCT_ID, 8, 2) = SUBSTRING(b.PLANT_PRODUCT_ID, 8, 2) and SUBSTRING(b.PLANT_PRODUCT_ID, 11, 4) = 'uvs]'
LEFT JOIN ODS_OPERSVODKA.DM_OPER_MAIN as c ON a.DT = c.DT and SUBSTRING(a.PLANT_PRODUCT_ID, 8, 2) = SUBSTRING(c.PLANT_PRODUCT_ID, 8, 2) and SUBSTRING(c.PLANT_PRODUCT_ID, 11, 2) = '4]'
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as x ON a.PLANT_PRODUCT_ID = x.PLANT_PRODUCT_ID and x.NAME = 'BP' 
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as y ON b.PLANT_PRODUCT_ID = y.PLANT_PRODUCT_ID and y.NAME = 'BP' 
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as z ON c.PLANT_PRODUCT_ID = z.PLANT_PRODUCT_ID and z.NAME = 'BP' 
WHERE SUBSTRING(a.PLANT_PRODUCT_ID, 11, 2) = '1]' and a.DATE_DAY_BEFORE is not NULL and a.DT < getdate()
and (
(a.BP < x.VALUE*0.1 and (b.BP > y.VALUE*0.1 or c.BP >  z.VALUE*0.1)) or
(b.BP < y.VALUE*0.1 and (a.BP > x.VALUE*0.1 or c.BP > z.VALUE*0.1)) or
(c.BP < z.VALUE*0.1 and (b.BP >  y.VALUE*0.1 or a.BP > x.VALUE*0.1))
)
UNION ALL
SELECT 10 as CHECK_NUMBER, a.DT, a.PLANT_PRODUCT_ID, a.FACTORY, a.PLANT, NULL as PRODUCT, 'PLAN' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(a.PLANT, ' Заниженные показатели ПЛАН') as COMMENT,
'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as a
LEFT JOIN ODS_OPERSVODKA.DM_OPER_MAIN as b ON a.DT = b.DT and SUBSTRING(a.PLANT_PRODUCT_ID, 8, 2) = SUBSTRING(b.PLANT_PRODUCT_ID, 8, 2) and SUBSTRING(b.PLANT_PRODUCT_ID, 11, 4) = 'uvs]'
LEFT JOIN ODS_OPERSVODKA.DM_OPER_MAIN as c ON a.DT = c.DT and SUBSTRING(a.PLANT_PRODUCT_ID, 8, 2) = SUBSTRING(c.PLANT_PRODUCT_ID, 8, 2) and SUBSTRING(c.PLANT_PRODUCT_ID, 11, 2) = '4]'
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as x ON a.PLANT_PRODUCT_ID = x.PLANT_PRODUCT_ID and x.NAME = 'PLAN' 
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as y ON b.PLANT_PRODUCT_ID = y.PLANT_PRODUCT_ID and y.NAME = 'PLAN' 
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as z ON c.PLANT_PRODUCT_ID = z.PLANT_PRODUCT_ID and z.NAME = 'PLAN' 
WHERE SUBSTRING(a.PLANT_PRODUCT_ID, 11, 2) = '1]' and a.DATE_DAY_BEFORE is not NULL and a.DT < getdate()
and (
(a.PLAN < x.VALUE*0.1 and (b.PLAN > y.VALUE*0.1 or c.PLAN >  z.VALUE*0.1)) or
(b.PLAN < y.VALUE*0.1 and (a.PLAN > x.VALUE*0.1 or c.PLAN > z.VALUE*0.1)) or
(c.PLAN < z.VALUE*0.1 and (b.PLAN >  y.VALUE*0.1 or a.PLAN > x.VALUE*0.1))
)
UNION ALL
SELECT 11 as CHECK_NUMBER, a.DT, a.PLANT_PRODUCT_ID, a.FACTORY, a.PLANT, NULL as PRODUCT, 'PPR' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(a.PLANT, ' Заниженные показатели ППР') as COMMENT,
'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as a
LEFT JOIN ODS_OPERSVODKA.DM_OPER_MAIN as b ON a.DT = b.DT and SUBSTRING(a.PLANT_PRODUCT_ID, 8, 2) = SUBSTRING(b.PLANT_PRODUCT_ID, 8, 2) and SUBSTRING(b.PLANT_PRODUCT_ID, 11, 4) = 'uvs]'
LEFT JOIN ODS_OPERSVODKA.DM_OPER_MAIN as c ON a.DT = c.DT and SUBSTRING(a.PLANT_PRODUCT_ID, 8, 2) = SUBSTRING(c.PLANT_PRODUCT_ID, 8, 2) and SUBSTRING(c.PLANT_PRODUCT_ID, 11, 2) = '4]'
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as x ON a.PLANT_PRODUCT_ID = x.PLANT_PRODUCT_ID and x.NAME = 'PPR' 
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as y ON b.PLANT_PRODUCT_ID = y.PLANT_PRODUCT_ID and y.NAME = 'PPR' 
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as z ON c.PLANT_PRODUCT_ID = z.PLANT_PRODUCT_ID and z.NAME = 'PPR' 
WHERE SUBSTRING(a.PLANT_PRODUCT_ID, 11, 2) = '1]' and a.DATE_DAY_BEFORE is not NULL and a.DT < getdate()
and (
(a.PPR < x.VALUE*0.1 and (b.PPR > y.VALUE*0.1 or c.PPR >  z.VALUE*0.1)) or
(b.PPR < y.VALUE*0.1 and (a.PPR > x.VALUE*0.1 or c.PPR > z.VALUE*0.1)) or
(c.PPR < z.VALUE*0.1 and (b.PPR >  y.VALUE*0.1 or a.PPR > x.VALUE*0.1))
)
UNION ALL
SELECT 12 as CHECK_NUMBER, a.DT, a.PLANT_PRODUCT_ID, a.FACTORY, a.PLANT, NULL as PRODUCT, 'FACT' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(a.PLANT, ' Заниженные показатели ФАКТ') as COMMENT,
'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN as a
LEFT JOIN ODS_OPERSVODKA.DM_OPER_MAIN as b ON a.DT = b.DT and SUBSTRING(a.PLANT_PRODUCT_ID, 8, 2) = SUBSTRING(b.PLANT_PRODUCT_ID, 8, 2) and SUBSTRING(b.PLANT_PRODUCT_ID, 11, 4) = 'uvs]'
LEFT JOIN ODS_OPERSVODKA.DM_OPER_MAIN as c ON a.DT = c.DT and SUBSTRING(a.PLANT_PRODUCT_ID, 8, 2) = SUBSTRING(c.PLANT_PRODUCT_ID, 8, 2) and SUBSTRING(c.PLANT_PRODUCT_ID, 11, 2) = '4]'
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as x ON a.PLANT_PRODUCT_ID = x.PLANT_PRODUCT_ID and x.NAME = 'FACT' 
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as y ON b.PLANT_PRODUCT_ID = y.PLANT_PRODUCT_ID and y.NAME = 'FACT' 
LEFT JOIN ODS_OPERSVODKA.DDS_DQ_BENCHMARK as z ON c.PLANT_PRODUCT_ID = z.PLANT_PRODUCT_ID and z.NAME = 'FACT' 
WHERE SUBSTRING(a.PLANT_PRODUCT_ID, 11, 2) = '1]' and a.DATE_DAY_BEFORE is not NULL and a.DT < getdate()
and (
(a.FACT < x.VALUE*0.1 and (b.FACT > y.VALUE*0.1 or c.FACT >  z.VALUE*0.1)) or
(b.FACT < y.VALUE*0.1 and (a.FACT > x.VALUE*0.1 or c.FACT > z.VALUE*0.1)) or
(c.FACT < z.VALUE*0.1 and (b.FACT >  y.VALUE*0.1 or a.FACT > x.VALUE*0.1))
)
UNION ALL
SELECT 13 as CHECK_NUMBER, a.DT, a.PLANT_PRODUCT_ID, a.FACTORY, a.PLANT, a.PRODUCT,  'BP' as TYPE, a.BP as VALUE, 'СОГ' as B_TYPE, b.BP as B_VALUE, NULL as DELTA,
CONCAT(a.PLANT, CONCAT(' ', CONCAT(a.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(' БП = ',TO_CHAR(a.BP)), ' СОГ БП = '), b.BP), ' WARNING: СОГ > ПНГ')))) as COMMENT, 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_STRAIT as a LEFT JOIN ODS_OPERSVODKA.DM_OPER_STRAIT as b
ON a.DT = b.DT 
and a.PLANT = b.PLANT
and a.PRODUCT = 'ПНГ' and b.PRODUCT = 'СОГ'
where (1 = 1)
and a.PRODUCT = 'ПНГ' 
and a.BP < b.BP
UNION ALL
SELECT 14 as CHECK_NUMBER, a.DT, a.PLANT_PRODUCT_ID, a.FACTORY, a.PLANT, a.PRODUCT,  'PLAN' as TYPE, a.PLAN as VALUE, 'СОГ' as B_TYPE, b.PLAN as B_VALUE, NULL as DELTA,
CONCAT(a.PLANT, CONCAT(' ', CONCAT(a.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(' ПЛАН = ',TO_CHAR(a.PLAN)), ' СОГ ПЛАН = '), b.PLAN), ' WARNING: СОГ > ПНГ')))) as COMMENT, 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_STRAIT as a LEFT JOIN ODS_OPERSVODKA.DM_OPER_STRAIT as b
ON a.DT = b.DT 
and a.PLANT = b.PLANT
and a.PRODUCT = 'ПНГ' and b.PRODUCT = 'СОГ'
where (1 = 1)
and a.PRODUCT = 'ПНГ' 
and a.PLAN < b.PLAN
UNION ALL
SELECT 15 as CHECK_NUMBER, a.DT, a.PLANT_PRODUCT_ID, a.FACTORY, a.PLANT, a.PRODUCT,  'PPR' as TYPE, a.PPR as VALUE, 'СОГ' as B_TYPE, b.PPR as B_VALUE, NULL as DELTA,
CONCAT(a.PLANT, CONCAT(' ', CONCAT(a.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(' ППР = ',TO_CHAR(a.PPR)), ' СОГ ППР = '), b.PPR), ' WARNING: СОГ > ПНГ')))) as COMMENT, 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_STRAIT as a LEFT JOIN ODS_OPERSVODKA.DM_OPER_STRAIT as b
ON a.DT = b.DT 
and a.PLANT = b.PLANT
and a.PRODUCT = 'ПНГ' and b.PRODUCT = 'СОГ'
where (1 = 1)
and a.PRODUCT = 'ПНГ' 
and a.PPR < b.PPR
UNION ALL
SELECT 16 as CHECK_NUMBER, a.DT, a.PLANT_PRODUCT_ID, a.FACTORY, a.PLANT, a.PRODUCT,  'FACT' as TYPE, a.FACT as VALUE, 'СОГ' as B_TYPE, b.FACT as B_VALUE, NULL as DELTA,
CONCAT(a.PLANT, CONCAT(' ', CONCAT(a.PRODUCT, CONCAT(CONCAT(CONCAT(CONCAT(' ППР = ',TO_CHAR(a.FACT)), ' СОГ ППР = '), b.FACT), ' WARNING: СОГ > ПНГ')))) as COMMENT, 'WARNING' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_STRAIT as a LEFT JOIN ODS_OPERSVODKA.DM_OPER_STRAIT as b
ON a.DT = b.DT 
and a.PLANT = b.PLANT
and a.PRODUCT = 'ПНГ' and b.PRODUCT = 'СОГ'
where (1 = 1)
and a.PRODUCT = 'ПНГ' 
and a.FACT < b.FACT
------------
--- были удалены проверки 17-20 после согласования с Алексеем Смирновым 18 сентября. Переписка "RE: СТГ Оперсводка Дашборд по качеству данных (DQ)"
------------
UNION ALL
SELECT 21 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'BP' as TYPE, BP as VALUE, NULL, NULL, NULL, 
CONCAT(CONCAT(CONCAT(PLANT,' '), PRODUCT), 'Отсутствует БП') as COMMENT,
'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
where (1 = 1)
and BP is NULL
and PRODUCT not in ('С3+/СОГ', 'С3+/ПНГ')
and DATE_DAY_BEFORE is not NULL
UNION ALL
SELECT 22 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'PLAN' as TYPE, PLAN as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(CONCAT(CONCAT(PLANT,' '), PRODUCT), 'Отсутствует ПЛАН') as COMMENT,
'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
where (1 = 1)
and PLAN is NULL
and PRODUCT not in ('С3+/СОГ', 'С3+/ПНГ')
and DATE_DAY_BEFORE is not NULL
UNION ALL
SELECT 23 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'PPR' as TYPE, PPR as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(CONCAT(CONCAT(PLANT,' '), PRODUCT), 'Отсутствует ППР') as COMMENT,
'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
where (1 = 1)
and PPR is NULL
and PRODUCT not in ('С3+/СОГ', 'С3+/ПНГ')
and DATE_DAY_BEFORE is not NULL
UNION ALL
SELECT 24 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'FACT' as TYPE, FACT as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(CONCAT(CONCAT(PLANT,' '), PRODUCT), 'Отсутствует ФАКТ') as COMMENT,
'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
where (1 = 1)
and ((FACT is NULL and DT < getdate()-1) or (FORECAST is NULL and DT > getdate()-1))
and PRODUCT not in ('С3+/СОГ', 'С3+/ПНГ')
and DATE_DAY_BEFORE is not NULL
UNION ALL
SELECT 25 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'none' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(CONCAT(CONCAT(PLANT,' '), PRODUCT), 'Дубликат записи') as COMMENT,
'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
WHERE (1 = 1)
and DATE_DAY_BEFORE is not NULL
GROUP BY DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT
HAVING count(*) > 1
UNION ALL
SELECT 26 as CHECK_NUMBER, DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT,  'none' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
CONCAT(CONCAT(CONCAT(PLANT,' '), PRODUCT), 'Дубликат записи') as COMMENT,
'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_STRAIT
WHERE (1 = 1)
GROUP BY DT, PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT
HAVING count(*) > 1
UNION ALL
SELECT 27 as CHECK_NUMBER, cast(getdate() as date) as DT, NULL as PLANT_PRODUCT_ID, NULL as FACTORY,  NULL as PLANT,  NULL as PRODUCT,  'none' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
'Некорректное число дней в источнике' as COMMENT,
'ERROR' as ERR_TYPE
FROM
(SELECT count(*)%31 as COUNT
FROM ODS_OPERSVODKA.DM_OPER_MAIN
) as a
WHERE COUNT != 0
UNION ALL
SELECT 28 as CHECK_NUMBER, cast(getdate() as date) as DT, NULL as PLANT_PRODUCT_ID, NULL as FACTORY,  NULL as PLANT,  NULL as PRODUCT,  'none' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
'Некорректное прогнозы прошлых периодов' as COMMENT, 'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
WHERE (1 = 1)
and 
(not FORECAST_YB is NULL 
or not FORECAST_C_YB is NULL
or not FORECAST_MB is NULL 
or not FORECAST_C_MB is NULL)
UNION ALL
SELECT 29 as CHECK_NUMBER, cast(getdate() as date) as DT, NULL as PLANT_PRODUCT_ID, NULL as FACTORY,  NULL as PLANT,  NULL as PRODUCT,  'none' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
'Ненулевые показатели для синтетических дат' as COMMENT, 'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
WHERE (1 = 1)
and DATE_DAY_BEFORE is NULL
and not
(
BP is NULL and PLAN is NULL and PPR is NULL and FORECAST is NULL and FACT is NULL 
and FACT_DB is NULL and BP_C is NULL and PLAN_C is NULL and PPR_C is NULL 
and FORECAST_C is NULL and FACT_C is NULL and FACT_C_DB is NULL 
and BP_YB is NULL and PLAN_YB is NULL and PPR_YB is NULL and FORECAST_YB is NULL 
and FACT_YB is NULL and BP_C_YB is NULL and PLAN_C_YB is NULL and PPR_C_YB is NULL and FORECAST_C_YB is NULL and FACT_C_YB is NULL)
UNION ALL
SELECT 30 as CHECK_NUMBER, cast(getdate() as date) as DT, NULL as PLANT_PRODUCT_ID, NULL as FACTORY,  NULL as PLANT,  NULL as PRODUCT,  'none' as TYPE, NULL as VALUE, NULL as B_TYPE, NULL as B_VALUE, NULL as DELTA, 
'Ненулевые показатели для синтетических дат для месяцев в которых 31 день' as COMMENT, 'ERROR' as ERR_TYPE
FROM ODS_OPERSVODKA.DM_OPER_MAIN
WHERE (1 = 1)
and DATE_MONTH_BEFORE is NULL
and not
(
BP_MB is NULL and PLAN_MB is NULL and PPR_MB is NULL and FORECAST_MB is NULL 
and FACT_MB is NULL and BP_C_MB is NULL and PLAN_C_MB is NULL and PPR_C_MB is NULL and FORECAST_C_MB is NULL and FACT_C_MB is NULL)
UNION ALL
 select
 	31 as CHECK_NUMBER, 
	DT,
	PLANT_PRODUCT_ID,
	FACTORY,
	PLANT,
	PRODUCT,
	'none' as TYPE,
	null as VALUE,
	null as B_TYPE,
	null as B_VALUE,
	null as DELTA,
	CONCAT(PLANT_PRODUCT_ID, ' Несовпадение данных в DM_OPER_STRAIT и DM_OPER_MAIN') as COMMENT,
	'ERROR' as ERR_TYPE
from
	(
	select
		count (LOAD_TS),
		DT,
		PLANT_PRODUCT_ID,
		FACTORY,
		PLANT,
		PRODUCT,
		BP,
		PLAN,
		PPR,
		FORECAST,
		FACT,
		BP_C,
		PLAN_C,
		PPR_C,
		FORECAST_C,
		FACT_C,
		FILE_ID
	FROM
		(
		SELECT
			DT,
			PLANT_PRODUCT_ID,
			FACTORY,
			PLANT,
			PRODUCT,
			BP,
			PLAN,
			PPR,
			FORECAST,
			FACT,
			BP_C,
			PLAN_C,
			PPR_C,
			FORECAST_C,
			FACT_C,
			FILE_ID,
			LOAD_TS
		FROM
			ODS_OPERSVODKA.DM_OPER_STRAIT
	UNION ALL
		SELECT
			DT,
			PLANT_PRODUCT_ID,
			FACTORY,
			PLANT,
			PRODUCT,
			BP,
			PLAN,
			PPR,
			FORECAST,
			FACT,
			BP_C,
			PLAN_C,
			PPR_C,
			FORECAST_C,
			FACT_C,
			FILE_ID,
			LOAD_TS
		FROM
			ODS_OPERSVODKA.DM_OPER_MAIN p
		where
			DATE_DAY_BEFORE is not null) result
	group by
		DT,
		PLANT_PRODUCT_ID,
		FACTORY,
		PLANT,
		PRODUCT,
		BP,
		PLAN,
		PPR,
		FORECAST,
		FACT,
		BP_C,
		PLAN_C,
		PPR_C,
		FORECAST_C,
		FACT_C,
		FILE_ID
	having
		count (LOAD_TS) = 1) result		
UNION ALL
 select
 	32 as CHECK_NUMBER, 
	DT,
	PLANT_PRODUCT_ID,
	FACTORY,
	PLANT,
	PRODUCT,
	'none' as TYPE,
	null as VALUE,
	null as B_TYPE,
	null as B_VALUE,
	null as DELTA,
	CONCAT(PLANT_PRODUCT_ID, ' Несовпадение данных в DM_OPER_STRAIT и ODS') as COMMENT,
	'ERROR' as ERR_TYPE
from
	(
	select
		count (LOAD_TS),
		DT,
		PLANT_PRODUCT_ID,
		FACTORY,
		PLANT,
		PRODUCT,
		ROUND(BP, 5) BP,
		PLAN,
		PPR,
		FORECAST,
		FACT,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else ROUND(BP_C, 5)
		end BP_C,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else PLAN_C
		end PLAN_C,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else PPR_C
		end PPR_C,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else FORECAST_C
		end FORECAST_C,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else FACT_C
		end FACT_C
	FROM
		(
		SELECT
			*
		FROM
			ODS_OPERSVODKA.DM_OPER_STRAIT
		where
			(PLANT_PRODUCT_ID not like '%png%'
			and PLANT_PRODUCT_ID not like '%uvs%'
			and PLANT_PRODUCT_ID not like '%sog%')
			--and PLANT_PRODUCT_ID = '[MENGE_05_sog]' and dt = '2019-06-04'
	UNION ALL
		SELECT
			p.DT,
			p.PLANT_PRODUCT_ID,
			FACTORY,
			PLANT,
			PRODUCT,
			sum(p2.BP)/ DATE_PART('DAY',
			last_day(p.dt)) BP,
			p.PLAN,
			p.PPR,
			p.FORECAST,
			p.FACT,
			p1.BP / DATE_PART('DAY',
			last_day(p1.dt)) BP_C,
			p3.PLAN PLAN_C,
			p3.PPR PPR_C,
			p3.FORECAST FORECAST_C,
			p3.FACT FACT_C,
			MAX(p.FILE_ID),
			now() LOAD_TS
		FROM
			ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK p
		inner join ODS_OPERSVODKA.DDS_REFERENCE r on
			p.PLANT_PRODUCT_ID = r.PLANT_PRODUCT_ID
		INNER JOIN (
				SELECT MAX(FILE_ID) FILE_ID,
				DT ,
				PLANT_PRODUCT_ID
			FROM
				ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			GROUP BY
				DT ,
				PLANT_PRODUCT_ID) r1 ON
			R1.DT = P.DT
			AND R1.PLANT_PRODUCT_ID = P.PLANT_PRODUCT_ID
			AND R1.FILE_ID = P.FILE_ID
		left join (
				select sum (bp) bp,
				trunc (p1.dt,
				'MONTH') dt,
				PLANT_PRODUCT_ID ,
				FILE_ID
			from
				ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK p1
			group by
				trunc (p1.dt,
				'MONTH'),
				PLANT_PRODUCT_ID ,
				FILE_ID) p1 on
			p1.PLANT_PRODUCT_ID = substr(p.PLANT_PRODUCT_ID,
			0,
			12)|| ']'
			and trunc (p.dt,
			'MONTH') = p1.dt
			AND P.FILE_ID = P1.FILE_ID
		left join ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK p3 on
			p3.PLANT_PRODUCT_ID = substr(p.PLANT_PRODUCT_ID,
			0,
			12)|| ']'
			and p.dt = p3.dt
			AND P.FILE_ID = P3.FILE_ID
		left join (
				select sum (bp) bp,
				trunc (p2.dt,
				'MONTH') dt,
				PLANT_PRODUCT_ID ,
				FILE_ID
			from
				ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK p2
			group by
				trunc (p2.dt,
				'MONTH'),
				PLANT_PRODUCT_ID ,
				FILE_ID) as p2 on
			p2.PLANT_PRODUCT_ID = p.PLANT_PRODUCT_ID
			and trunc (p.dt,
			'MONTH') = p2.dt
			AND P.FILE_ID = P2.FILE_ID
		where
			(1 = 1)
			--and  p.PLANT_PRODUCT_ID = '[MENGE_05_sog]' and p.dt = '2018-04-05'
			group by p.DT,
			p.PLANT_PRODUCT_ID,
			FACTORY,
			PLANT,
			PRODUCT,
			last_day(p1.dt),
			p.PLAN,
			p.PPR,
			p.FORECAST,
			p.FACT,
			p1.BP / DATE_PART('DAY',
			last_day(p1.dt)) ,
			p3.PLAN,
			p3.PPR,
			p3.FORECAST,
			p3.FACT) as UNI
	group by
		DT,
		PLANT_PRODUCT_ID,
		FACTORY,
		PLANT,
		PRODUCT,
		ROUND(BP, 5),
		PLAN,
		PPR,
		FORECAST,
		FACT,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else ROUND(BP_C, 5)
		end,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else PLAN_C
		end,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else PPR_C
		end,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else FORECAST_C
		end,
		case
			when PRODUCT in ('СОГ',
			'ПНГ',
			'ШФЛУ',
			'ПБТ',
			'БГС') then null
			else FACT_C
		end
	having
		count (LOAD_TS) = 1) result
UNION ALL
 select
 	33 as CHECK_NUMBER, 
	DT,
	PLANT_PRODUCT_ID,
	null as FACTORY,
	null as PLANT,
	null as PRODUCT,
	'all' as TYPE,
	null as VALUE,
	null as B_TYPE,
	null as B_VALUE,
	null as DELTA,
	CONCAT(PLANT_PRODUCT_ID, ' Несовпадение данных в STG и ODS') as COMMENT,
	'ERROR' as ERR_TYPE
from (select
	count (File_id),
	DT,
	PLANT_PRODUCT_ID,
	BP,
	PLAN,
	PPR,
	FORECAST,
	FACT
FROM
	(
	SELECT
		1 as FILE_ID,
		*
	FROM
		ODS_OPERSVODKA.STG_ZPP_PROD_REPORT_TRANSP
UNION ALL
	SELECT
		*
	FROM
		ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
	where
		MONTH (DT) = month(sysdate)
		and YEAR (DT) = YEAR(sysdate)
		and BUSINESS_DT in (
			select max(BUSINESS_DT)
		from
			ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
		group by
			DT,
			PLANT_PRODUCT_ID )) as UNI
group by
	DT,
	PLANT_PRODUCT_ID,
	BP,
	PLAN,
	PPR,
	FORECAST,
	FACT
having
	count (File_id) = 1) result
UNION ALL 
SELECT 
	34 as CHECK_NUMBER, 
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	PLANT_PRODUCT_ID,
	FACTORY, 
	PLANT, 
	PRODUCT,
	'PPR' as TYPE,
	sum(PPR) as VALUE,
	'PLAN' as B_TYPE,
	sum(PLAN) as BVALUE,
	sum(PPR) - sum(PLAN) as DELTA,
	PLANT||' '||PRODUCT||' ППР = '||sum(PPR)::numeric(18,0)||' ПЛАН = '||sum(PLAN)::numeric(18,0)||' delta = '||(sum(PPR) - sum(PLAN))::numeric(18,0)||' WARNING: сумма ППР и ПЛАН по месяцу не равны' as COMMENT,
	'WARNING' as ERR_TYPE	
FROM ODS_OPERSVODKA.DM_OPER_MAIN
WHERE (1 = 1)
	and year(dt) = year(getdate()) 
	and month(dt) = month(getdate())
	and PRODUCT not in ('С3+/ПНГ','С3+/СОГ','УВС','С3+ в ПНГ','С3+ в СОГ')
GROUP BY year(dt), month(dt), PLANT_PRODUCT_ID, FACTORY, PLANT, PRODUCT
HAVING ((sum(PPR) - sum(PLAN) >= 1) or (sum(PPR) - sum(PLAN) <= -1))
UNION ALL
SELECT 
	35 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_6]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВСЕ ЗАВОДЫ' as PLANT, 
	'УВС по формуле' as PRODUCT,
	'BP' as TYPE,
	sum(a.BP)::numeric(18,2) as VALUE,
	'УВС(SAP)' as B_TYPE,
	sum(b.BP)::numeric(18,2) as B_VALUE,
	(sum(a.BP)- sum(b.BP))::numeric(18,2) as DELTA,
	'ВСЕ ЗАВОДЫ '||'BP(сумма по месяцу)'||' УВС(формула) = '||sum(a.BP)::numeric(18,0)||' УВС(SAP) = '||sum(b.BP)::numeric(18,0)||' delta = '||(sum(a.BP)- sum(b.BP))::numeric(18,0)||' WARNING: расхождение БП по УВС(SAP) и УВС(ШФЛУ+ПБТ+БГС+БГС(неконд.)+ПТ)' as COMMENT,
	'WARNING' as ERR_TYPE	
FROM (
	SELECT a. BUSINESS_DT, a.DT,  
		sum(a.BP) as BP, sum(a.PLAN) as PLAN, sum(a.PPR) as PPR, sum(a.FORECAST) as FORECAST, sum(a.FACT) as FACT
	FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as a
	WHERE (1 = 1)
		and a.BUSINESS_DT = cast(getdate()-1 as date)
		and (a.PLANT_PRODUCT_ID like '%_A]' or a.PLANT_PRODUCT_ID like '%_2]' or a.PLANT_PRODUCT_ID like '%_3]' or a.PLANT_PRODUCT_ID like '%_5]')
		and a.PLANT_PRODUCT_ID not in ('[MENGE_2]', '[MENGE_3]', '[MENGE_5]')
	GROUP BY a.BUSINESS_DT, a.DT
	)a
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as b ON a.BUSINESS_DT = b.BUSINESS_DT and a.DT = b.DT and b.PLANT_PRODUCT_ID = '[MENGE_6]'
HAVING (sum(a.BP)- sum(b.BP)) != 0
UNION ALL
SELECT 
	35 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_6]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВСЕ ЗАВОДЫ' as PLANT, 
	'УВС по формуле' as PRODUCT,
	'PLAN' as TYPE,
	sum(a.PLAN)::numeric(18,2) as VALUE,
	'УВС(SAP)' as B_TYPE,
	sum(b.PLAN)::numeric(18,2) as B_VALUE,
	(sum(a.PLAN)- sum(b.PLAN))::numeric(18,2) as DELTA,
	'ВСЕ ЗАВОДЫ '||'PLAN(сумма по месяцу)'||' УВС(формула) = '||sum(a.PLAN)::numeric(18,0)||' УВС(SAP) = '||sum(b.PLAN)::numeric(18,0)||' delta = '||(sum(a.PLAN)- sum(b.PLAN))::numeric(18,0)||' WARNING: расхождение ПЛАНА по УВС(SAP) и УВС(ШФЛУ+ПБТ+БГС+БГС(неконд.)+ПТ)' as COMMENT,
	'WARNING' as ERR_TYPE	
FROM (
	SELECT a. BUSINESS_DT, a.DT,  
		sum(a.BP) as BP, sum(a.PLAN) as PLAN, sum(a.PPR) as PPR, sum(a.FORECAST) as FORECAST, sum(a.FACT) as FACT
	FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as a
	WHERE (1 = 1)
		and a.BUSINESS_DT = cast(getdate()-1 as date)
		and (a.PLANT_PRODUCT_ID like '%_A]' or a.PLANT_PRODUCT_ID like '%_2]' or a.PLANT_PRODUCT_ID like '%_3]' or a.PLANT_PRODUCT_ID like '%_5]')
		and a.PLANT_PRODUCT_ID not in ('[MENGE_2]', '[MENGE_3]', '[MENGE_5]')
	GROUP BY a.BUSINESS_DT, a.DT
	)a
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as b ON a.BUSINESS_DT = b.BUSINESS_DT and a.DT = b.DT and b.PLANT_PRODUCT_ID = '[MENGE_6]'
HAVING (sum(a.PLAN)- sum(b.PLAN)) != 0
UNION ALL
SELECT 
	35 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_6]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВСЕ ЗАВОДЫ' as PLANT, 
	'УВС по формуле' as PRODUCT,
	'PPR' as TYPE,
	sum(a.PPR)::numeric(18,2) as VALUE,
	'УВС(SAP)' as B_TYPE,
	sum(b.PPR)::numeric(18,2) as B_VALUE,
	(sum(a.PPR)- sum(b.PPR))::numeric(18,2) as DELTA,
	'ВСЕ ЗАВОДЫ '||'PPR(сумма по месяцу)'||' УВС(формула) = '||sum(a.PPR)::numeric(18,0)||' УВС(SAP) = '||sum(b.PPR)::numeric(18,0)||' delta = '||(sum(a.PPR)- sum(b.PPR))::numeric(18,0)||' WARNING: расхождение ППР по УВС(SAP) и УВС(ШФЛУ+ПБТ+БГС+БГС(неконд.)+ПТ)' as COMMENT,
	'WARNING' as ERR_TYPE	
FROM (
	SELECT a. BUSINESS_DT, a.DT,  
		sum(a.BP) as BP, sum(a.PLAN) as PLAN, sum(a.PPR) as PPR, sum(a.FORECAST) as FORECAST, sum(a.FACT) as FACT
	FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as a
	WHERE (1 = 1)
		and a.BUSINESS_DT = cast(getdate()-1 as date)
		and (a.PLANT_PRODUCT_ID like '%_A]' or a.PLANT_PRODUCT_ID like '%_2]' or a.PLANT_PRODUCT_ID like '%_3]' or a.PLANT_PRODUCT_ID like '%_5]')
		and a.PLANT_PRODUCT_ID not in ('[MENGE_2]', '[MENGE_3]', '[MENGE_5]')
	GROUP BY a.BUSINESS_DT, a.DT
	)a
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as b ON a.BUSINESS_DT = b.BUSINESS_DT and a.DT = b.DT and b.PLANT_PRODUCT_ID = '[MENGE_6]'
HAVING (sum(a.PPR)- sum(b.PPR)) != 0
UNION ALL
SELECT 
	35 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_6]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВСЕ ЗАВОДЫ' as PLANT, 
	'УВС по формуле' as PRODUCT,
	'FORECAST' as TYPE,
	sum(a.FORECAST)::numeric(18,2) as VALUE,
	'УВС(SAP)' as B_TYPE,
	sum(b.FORECAST)::numeric(18,2) as B_VALUE,
	(sum(a.FORECAST)- sum(b.FORECAST))::numeric(18,2) as DELTA,
	'ВСЕ ЗАВОДЫ '||'FORECAST(сумма по месяцу)'||' УВС(формула) = '||sum(a.FORECAST)::numeric(18,0)||' УВС(SAP) = '||sum(b.FORECAST)::numeric(18,0)||' delta = '||(sum(a.FORECAST)- sum(b.FORECAST))::numeric(18,0)||' WARNING: расхождение ПРОГНОЗА по УВС(SAP) и УВС(ШФЛУ+ПБТ+БГС+БГС(неконд.)+ПТ)' as COMMENT,
	'WARNING' as ERR_TYPE	
FROM (
	SELECT a. BUSINESS_DT, a.DT,  
		sum(a.BP) as BP, sum(a.PLAN) as PLAN, sum(a.PPR) as PPR, sum(a.FORECAST) as FORECAST, sum(a.FACT) as FACT
	FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as a
	WHERE (1 = 1)
		and a.BUSINESS_DT = cast(getdate()-1 as date)
		and (a.PLANT_PRODUCT_ID like '%_A]' or a.PLANT_PRODUCT_ID like '%_2]' or a.PLANT_PRODUCT_ID like '%_3]' or a.PLANT_PRODUCT_ID like '%_5]')
		and a.PLANT_PRODUCT_ID not in ('[MENGE_2]', '[MENGE_3]', '[MENGE_5]')
	GROUP BY a.BUSINESS_DT, a.DT
	)a
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as b ON a.BUSINESS_DT = b.BUSINESS_DT and a.DT = b.DT and b.PLANT_PRODUCT_ID = '[MENGE_6]'
HAVING (sum(a.FORECAST)- sum(b.FORECAST)) != 0
UNION ALL
SELECT 
	35 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_6]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВСЕ ЗАВОДЫ' as PLANT, 
	'УВС по формуле' as PRODUCT,
	'FACT' as TYPE,
	sum(a.FACT)::numeric(18,2) as VALUE,
	'УВС(SAP)' as B_TYPE,
	sum(b.FACT)::numeric(18,2) as B_VALUE,
	(sum(a.FACT)- sum(b.FACT))::numeric(18,2) as DELTA,
	'ВСЕ ЗАВОДЫ '||'FACT(сумма по месяцу)'||' УВС(формула) = '||sum(a.FACT)::numeric(18,0)||' УВС(SAP) = '||sum(b.FACT)::numeric(18,0)||' delta = '||(sum(a.FACT)- sum(b.FACT))::numeric(18,0)||' WARNING: расхождение ФАКТА по УВС(SAP) и УВС(ШФЛУ+ПБТ+БГС+БГС(неконд.)+ПТ)' as COMMENT,
	'WARNING' as ERR_TYPE	
FROM (
	SELECT a. BUSINESS_DT, a.DT,  
		sum(a.BP) as BP, sum(a.PLAN) as PLAN, sum(a.PPR) as PPR, sum(a.FORECAST) as FORECAST, sum(a.FACT) as FACT
	FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as a
	WHERE (1 = 1)
		and a.BUSINESS_DT = cast(getdate()-1 as date)
		and (a.PLANT_PRODUCT_ID like '%_A]' or a.PLANT_PRODUCT_ID like '%_2]' or a.PLANT_PRODUCT_ID like '%_3]' or a.PLANT_PRODUCT_ID like '%_5]')
		and a.PLANT_PRODUCT_ID not in ('[MENGE_2]', '[MENGE_3]', '[MENGE_5]')
	GROUP BY a.BUSINESS_DT, a.DT
	)a
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as b ON a.BUSINESS_DT = b.BUSINESS_DT and a.DT = b.DT and b.PLANT_PRODUCT_ID = '[MENGE_6]'
HAVING (sum(a.FACT)- sum(b.FACT)) != 0
-----------------------------------------------------36 check_____________
UNION ALL
SELECT 
	36 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_01_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'НВГПК' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'FACT' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.FACT::numeric(18,2) as B_VALUE,
	(c.abc - d.FACT)::numeric(18,2) as DELTA,
	'НВГПК '||'ФАКТ'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.FACT::numeric(18,0)||' delta = '||(c.abc - d.FACT)::numeric(18,0)||' WARNING: НВГПК расхождение ФАКТ по ПНГ(SAP) и ПНГ(НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(fact) ELSE sum(fact)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_09_1]', '[MENGE_09_7]', '[MENGE_10_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_09_1]', '[MENGE_09_7]', '[MENGE_10_1]','[MENGE_10_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_01_1]'
WHERE c.abc - d.FACT != 0
UNION ALL
SELECT 
	36 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_01_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'НВГПК' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'BP(month)' as TYPE,
	sum(c.abc)::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	sum(d.BP)::numeric(18,2) as B_VALUE,
	(sum(c.abc) - sum(d.BP))::numeric(18,2) as DELTA,
	'НВГПК '||'БП(месяц)'||' ПНГ(формула) = '||sum(c.abc)::numeric(18,0)||' ПНГ(SAP) = '||sum(d.BP)::numeric(18,0)||' delta = '||(sum(c.abc) - sum(d.BP))::numeric(18,0)||' WARNING: НВГПК расхождение БП(месяц) по ПНГ(SAP) и ПНГ(НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
		(SELECT dt, sum(abc) as abc FROM 
			(SELECT 
				dt, 
				gr, 
				CASE WHEN gr = 1 THEN sum(BP) ELSE sum(BP)*-1 END as abc
			FROM
				(
				SELECT
					DT,  
					PLANT_PRODUCT_ID, 
					BP, PLAN, PPR, FORECAST, FACT, 
					CASE
						WHEN PLANT_PRODUCT_ID in ('[MENGE_09_1]', '[MENGE_09_7]', '[MENGE_10_1]') then 1 else 2
					END as gr
				FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
				WHERE (1 = 1)
					and BUSINESS_DT = cast(getdate()-1 as date)
					and PLANT_PRODUCT_ID in ('[MENGE_09_1]', '[MENGE_09_7]', '[MENGE_10_1]','[MENGE_10_9]')
				)a
			GROUP BY DT, gr
			)b
			GROUP BY dt
		)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_01_1]'
GROUP BY d.PLANT_PRODUCT_ID
HAVING (sum(c.abc) - sum(d.BP)) != 0
UNION ALL
SELECT 
	36 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_01_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'НВГПК' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'PLAN' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.PLAN::numeric(18,2) as B_VALUE,
	(c.abc - d.PLAN)::numeric(18,2) as DELTA,
	'НВГПК '||'ПЛАН'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.PLAN::numeric(18,0)||' delta = '||(c.abc - d.PLAN)::numeric(18,0)||' WARNING: НВГПК расхождение ПЛАН по ПНГ(SAP) и ПНГ(НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(PLAN) ELSE sum(PLAN)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_09_1]', '[MENGE_09_7]', '[MENGE_10_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_09_1]', '[MENGE_09_7]', '[MENGE_10_1]','[MENGE_10_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_01_1]'
WHERE c.abc - d.PLAN != 0
UNION ALL
SELECT 
	36 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_01_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'НВГПК' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'PPR' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.PPR::numeric(18,2) as B_VALUE,
	(c.abc - d.PPR)::numeric(18,2) as DELTA,
	'НВГПК '||'ППР'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.PPR::numeric(18,0)||' delta = '||(c.abc - d.PPR)::numeric(18,0)||' WARNING: НВГПК расхождение ППР по ПНГ(SAP) и ПНГ(НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(PPR) ELSE sum(PPR)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_09_1]', '[MENGE_09_7]', '[MENGE_10_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_09_1]', '[MENGE_09_7]', '[MENGE_10_1]','[MENGE_10_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_01_1]'
WHERE c.abc - d.PPR != 0
UNION ALL
SELECT 
	37 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_02_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'БГПК' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'FACT' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.FACT::numeric(18,2) as B_VALUE,
	(c.abc - d.FACT)::numeric(18,2) as DELTA,
	'БГПК '||'ФАКТ'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.FACT::numeric(18,0)||' delta = '||(c.abc - d.FACT)::numeric(18,0)||' WARNING: БГПК расхождение ФАКТ по ПНГ(SAP) и ПНГ(БГПЗ:ПНГ+БГПЗ:НГ+БКС:ПНГ-БКС:НГ(отг)+ВКС:ПНГ-ВКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(fact) ELSE sum(fact)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_11_1]', '[MENGE_11_7]', '[MENGE_12_1]', '[MENGE_13_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_11_1]', '[MENGE_11_7]', '[MENGE_12_1]', '[MENGE_13_1]', '[MENGE_12_9]', '[MENGE_13_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_02_1]'
WHERE c.abc - d.FACT != 0
UNION ALL
SELECT 
	37 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_02_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'БГПК' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'BP(month)' as TYPE,
	sum(c.abc)::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	sum(d.BP)::numeric(18,2) as B_VALUE,
	(sum(c.abc) - sum(d.BP))::numeric(18,2) as DELTA,
	'БГПК '||'БП(месяц)'||' ПНГ(формула) = '||sum(c.abc)::numeric(18,0)||' ПНГ(SAP) = '||sum(d.BP)::numeric(18,0)||' delta = '||(sum(c.abc) - sum(d.BP))::numeric(18,0)||' WARNING: БГПК расхождение БП(месяц) по ПНГ(SAP) и ПНГ(БГПЗ:ПНГ+БГПЗ:НГ+БКС:ПНГ-БКС:НГ(отг)+ВКС:ПНГ-ВКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
		(SELECT dt, sum(abc) as abc FROM 
			(SELECT 
				dt, 
				gr, 
				CASE WHEN gr = 1 THEN sum(BP) ELSE sum(BP)*-1 END as abc
			FROM
				(
				SELECT
					DT,  
					PLANT_PRODUCT_ID, 
					BP, PLAN, PPR, FORECAST, FACT, 
					CASE
						WHEN PLANT_PRODUCT_ID in ('[MENGE_11_1]', '[MENGE_11_7]', '[MENGE_12_1]', '[MENGE_13_1]') then 1 else 2
					END as gr
				FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
				WHERE (1 = 1)
					and BUSINESS_DT = cast(getdate()-1 as date)
					and PLANT_PRODUCT_ID in ('[MENGE_11_1]', '[MENGE_11_7]', '[MENGE_12_1]', '[MENGE_13_1]', '[MENGE_12_9]', '[MENGE_13_9]')
				)a
			GROUP BY DT, gr
			)b
			GROUP BY dt
		)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_02_1]'
GROUP BY d.PLANT_PRODUCT_ID
HAVING (sum(c.abc) - sum(d.BP)) != 0
UNION ALL
SELECT 
	37 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_02_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'БГПК' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'PLAN' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.PLAN::numeric(18,2) as B_VALUE,
	(c.abc - d.PLAN)::numeric(18,2) as DELTA,
	'БГПК '||'ПЛАН'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.PLAN::numeric(18,0)||' delta = '||(c.abc - d.PLAN)::numeric(18,0)||' WARNING: БГПК расхождение ПЛАН по ПНГ(SAP) и ПНГ(БГПЗ:ПНГ+БГПЗ:НГ+БКС:ПНГ-БКС:НГ(отг)+ВКС:ПНГ-ВКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(PLAN) ELSE sum(PLAN)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_11_1]', '[MENGE_11_7]', '[MENGE_12_1]', '[MENGE_13_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_11_1]', '[MENGE_11_7]', '[MENGE_12_1]', '[MENGE_13_1]', '[MENGE_12_9]', '[MENGE_13_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_02_1]'
WHERE c.abc - d.PLAN != 0
UNION ALL
SELECT 
	37 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_02_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'БГПК' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'PPR' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.PPR::numeric(18,2) as B_VALUE,
	(c.abc - d.PPR)::numeric(18,2) as DELTA,
	'БГПК '||'ППР'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.PPR::numeric(18,0)||' delta = '||(c.abc - d.PPR)::numeric(18,0)||' WARNING: БГПК расхождение ППР по ПНГ(SAP) и ПНГ(БГПЗ:ПНГ+БГПЗ:НГ+БКС:ПНГ-БКС:НГ(отг)+ВКС:ПНГ-ВКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(PPR) ELSE sum(PPR)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_11_1]', '[MENGE_11_7]', '[MENGE_12_1]', '[MENGE_13_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_11_1]', '[MENGE_11_7]', '[MENGE_12_1]', '[MENGE_13_1]', '[MENGE_12_9]', '[MENGE_13_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_02_1]'
WHERE c.abc - d.PPR != 0
UNION ALL
SELECT 
	38 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_04_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'МГПЗ' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'FACT' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.FACT::numeric(18,2) as B_VALUE,
	(c.abc - d.FACT)::numeric(18,2) as DELTA,
	'МГПЗ '||'ФАКТ'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.FACT::numeric(18,0)||' delta = '||(c.abc - d.FACT)::numeric(18,0)||' WARNING: МГПЗ расхождение ФАКТ по ПНГ(SAP) и ПНГ(НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(fact) ELSE sum(fact)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_14_1]', '[MENGE_15_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_14_1]', '[MENGE_15_1]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_04_1]'
WHERE c.abc - d.FACT != 0
UNION ALL
SELECT 
	38 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_04_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'МГПЗ' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'BP(month)' as TYPE,
	sum(c.abc)::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	sum(d.BP)::numeric(18,2) as B_VALUE,
	(sum(c.abc) - sum(d.BP))::numeric(18,2) as DELTA,
	'МГПЗ '||'БП(месяц)'||' ПНГ(формула) = '||sum(c.abc)::numeric(18,0)||' ПНГ(SAP) = '||sum(d.BP)::numeric(18,0)||' delta = '||(sum(c.abc) - sum(d.BP))::numeric(18,0)||' WARNING: МГПЗ расхождение БП(месяц) по ПНГ(SAP) и ПНГ(НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
		(SELECT dt, sum(abc) as abc FROM 
			(SELECT 
				dt, 
				gr, 
				CASE WHEN gr = 1 THEN sum(BP) ELSE sum(BP)*-1 END as abc
			FROM
				(
				SELECT
					DT,  
					PLANT_PRODUCT_ID, 
					BP, PLAN, PPR, FORECAST, FACT, 
					CASE
						WHEN PLANT_PRODUCT_ID in ('[MENGE_14_1]', '[MENGE_15_1]') then 1 else 2
					END as gr
				FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
				WHERE (1 = 1)
					and BUSINESS_DT = cast(getdate()-1 as date)
					and PLANT_PRODUCT_ID in ('[MENGE_14_1]', '[MENGE_15_1]')
				)a
			GROUP BY DT, gr
			)b
			GROUP BY dt
		)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_04_1]'
GROUP BY d.PLANT_PRODUCT_ID
HAVING (sum(c.abc) - sum(d.BP)) != 0
UNION ALL
SELECT 
	38 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_04_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'МГПЗ' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'PLAN' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.PLAN::numeric(18,2) as B_VALUE,
	(c.abc - d.PLAN)::numeric(18,2) as DELTA,
	'МГПЗ '||'ПЛАН'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.PLAN::numeric(18,0)||' delta = '||(c.abc - d.PLAN)::numeric(18,0)||' WARNING: МГПЗ расхождение ПЛАН по ПНГ(SAP) и ПНГ(НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(PLAN) ELSE sum(PLAN)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_14_1]', '[MENGE_15_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_14_1]', '[MENGE_15_1]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_04_1]'
WHERE c.abc - d.PLAN != 0
UNION ALL
SELECT 
	38 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_04_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'МГПЗ' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'PPR' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.PPR::numeric(18,2) as B_VALUE,
	(c.abc - d.PPR)::numeric(18,2) as DELTA,
	'МГПЗ '||'ППР'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.PPR::numeric(18,0)||' delta = '||(c.abc - d.PPR)::numeric(18,0)||' WARNING: МГПЗ расхождение ППР по ПНГ(SAP) и ПНГ(НВГПЗ:ПНГ+НВГПЗ:НГ+ТКС:ПНГ-ТКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(PPR) ELSE sum(PPR)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_14_1]', '[MENGE_15_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_14_1]', '[MENGE_15_1]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_04_1]'
WHERE c.abc - d.PPR != 0
UNION ALL
SELECT 
	39 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_05_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВГПЗ' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'FACT' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.FACT::numeric(18,2) as B_VALUE,
	(c.abc - d.FACT)::numeric(18,2) as DELTA,
	'ВГПЗ '||'ФАКТ'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.FACT::numeric(18,0)||' delta = '||(c.abc - d.FACT)::numeric(18,0)||' WARNING: ВГПЗ расхождение ФАКТ по ПНГ(SAP) и ПНГ(ВГПЗ:ПНГ+ВГПЗ:НГ+ВяКС:ПНГ-ВяКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(fact) ELSE sum(fact)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_16_1]', '[MENGE_16_7]', '[MENGE_17_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_16_1]', '[MENGE_16_7]', '[MENGE_17_1]','[MENGE_17_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_05_1]'
WHERE c.abc - d.FACT != 0
UNION ALL
SELECT 
	39 as CHECK_NUMBER,
	cast(concat(concat(concat(concat(year(getdate()), '-'), case when month(getdate()) > 9 then '' else '0' end), month(getdate())), '-01') as date) as DT,
	'[MENGE_05_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВГПЗ' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'BP(month)' as TYPE,
	sum(c.abc)::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	sum(d.BP)::numeric(18,2) as B_VALUE,
	(sum(c.abc) - sum(d.BP))::numeric(18,2) as DELTA,
	'ВГПЗ '||'БП(месяц)'||' ПНГ(формула) = '||sum(c.abc)::numeric(18,0)||' ПНГ(SAP) = '||sum(d.BP)::numeric(18,0)||' delta = '||(sum(c.abc) - sum(d.BP))::numeric(18,0)||' WARNING: ВГПЗ расхождение БП(месяц) по ПНГ(SAP) и ПНГ(ВГПЗ:ПНГ+ВГПЗ:НГ+ВяКС:ПНГ-ВяКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
		(SELECT dt, sum(abc) as abc FROM 
			(SELECT 
				dt, 
				gr, 
				CASE WHEN gr = 1 THEN sum(BP) ELSE sum(BP)*-1 END as abc
			FROM
				(
				SELECT
					DT,  
					PLANT_PRODUCT_ID, 
					BP, PLAN, PPR, FORECAST, FACT, 
					CASE
						WHEN PLANT_PRODUCT_ID in ('[MENGE_16_1]', '[MENGE_16_7]', '[MENGE_17_1]') then 1 else 2
					END as gr
				FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
				WHERE (1 = 1)
					and BUSINESS_DT = cast(getdate()-1 as date)
					and PLANT_PRODUCT_ID in ('[MENGE_16_1]', '[MENGE_16_7]', '[MENGE_17_1]','[MENGE_17_9]')
				)a
			GROUP BY DT, gr
			)b
			GROUP BY dt
		)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_05_1]'
GROUP BY d.PLANT_PRODUCT_ID
HAVING (sum(c.abc) - sum(d.BP)) != 0
UNION ALL
SELECT 
	39 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_05_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВГПЗ' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'PLAN' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.PLAN::numeric(18,2) as B_VALUE,
	(c.abc - d.PLAN)::numeric(18,2) as DELTA,
	'ВГПЗ '||'ПЛАН'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.PLAN::numeric(18,0)||' delta = '||(c.abc - d.PLAN)::numeric(18,0)||' WARNING: ВГПЗ расхождение ПЛАН по ПНГ(SAP) и ПНГ(ВГПЗ:ПНГ+ВГПЗ:НГ+ВяКС:ПНГ-ВяКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(PLAN) ELSE sum(PLAN)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_16_1]', '[MENGE_16_7]', '[MENGE_17_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_16_1]', '[MENGE_16_7]', '[MENGE_17_1]','[MENGE_17_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_05_1]'
WHERE c.abc - d.PLAN != 0
UNION ALL
SELECT 
	39 as CHECK_NUMBER,
	c.dt as DT, 
	'[MENGE_05_1]' as PLANT_PRODUCT_ID,
	'СТГ' as FACTORY, 
	'ВГПЗ' as PLANT, 
	'ПНГ(формула)' as PRODUCT,
	'PPR' as TYPE,
	c.abc::numeric(18,2) as VALUE,
	'ПНГ(SAP)' as B_TYPE,
	d.PPR::numeric(18,2) as B_VALUE,
	(c.abc - d.PPR)::numeric(18,2) as DELTA,
	'ВГПЗ '||'ППР'||' ПНГ(формула) = '||c.abc::numeric(18,0)||' ПНГ(SAP) = '||d.PPR::numeric(18,0)||' delta = '||(c.abc - d.PPR)::numeric(18,0)||' WARNING: ВГПЗ расхождение ППР по ПНГ(SAP) и ПНГ(ВГПЗ:ПНГ+ВГПЗ:НГ+ВяКС:ПНГ-ВяКС:НГ(отг)' as COMMENT,
	'WARNING' as ERR_TYPE
FROM
	(SELECT dt, sum(abc) as abc FROM 
		(SELECT 
			dt, 
			gr, 
			CASE WHEN gr = 1 THEN sum(PPR) ELSE sum(PPR)*-1 END as abc
		FROM
			(
			SELECT
				DT,  
				PLANT_PRODUCT_ID, 
				BP, PLAN, PPR, FORECAST, FACT, 
				CASE
					WHEN PLANT_PRODUCT_ID in ('[MENGE_16_1]', '[MENGE_16_7]', '[MENGE_17_1]') then 1 else 2
				END as gr
			FROM ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK
			WHERE (1 = 1)
				and BUSINESS_DT = cast(getdate()-1 as date)
				and PLANT_PRODUCT_ID in ('[MENGE_16_1]', '[MENGE_16_7]', '[MENGE_17_1]','[MENGE_17_9]')
			)a
		GROUP BY DT, gr
		)b
		GROUP BY dt
	)c 
JOIN ODS_OPERSVODKA.ODS_ZPP_PROD_REPORT_TRANSP_MASK as d 
	ON d.BUSINESS_DT = cast(getdate()-1 as date) 
	and c.DT = d.DT 
	and d.PLANT_PRODUCT_ID = '[MENGE_05_1]'
WHERE c.abc - d.PPR != 0
)main