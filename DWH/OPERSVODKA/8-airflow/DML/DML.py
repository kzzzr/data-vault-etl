STG_OPERSVODKA_STG_COMMENTS_DQ_SOURCE_TARGET = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_COMMENTS_DQ_SOURCE_TARGET',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DML/STG_OPERSVODKA.STG_COMMENTS_DQ_SOURCE_TARGET.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_COMMENTS',
        'step' : 'STG_OPERSVODKA.STG_COMMENTS_DQ_SOURCE_TARGET',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_COMMENTS_DQ_SOURCE_TARGET.sql'
    }
)

STG_OPERSVODKA_STG_COMMENTS_DQ_FILE_EXISTS = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_COMMENTS_DQ_FILE_EXISTS',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DML/STG_OPERSVODKA.STG_COMMENTS_DQ_FILE_EXISTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_COMMENTS',
        'step' : 'STG_OPERSVODKA.STG_COMMENTS_DQ_FILE_EXISTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_COMMENTS_DQ_FILE_EXISTS.sql'
    }
)

STG_OPERSVODKA_STG_COMMENTS_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_COMMENTS_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DML/STG_OPERSVODKA.STG_COMMENTS_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_COMMENTS',
        'step' : 'STG_OPERSVODKA.STG_COMMENTS_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_COMMENTS_ETL_FILE_LOAD.sql'
    }
)

STG_OPERSVODKA_STG_COMMENTS_DQ_FILE_REGISTERED = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_COMMENTS_DQ_FILE_REGISTERED',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DML/STG_OPERSVODKA.STG_COMMENTS_DQ_FILE_REGISTERED.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_COMMENTS',
        'step' : 'STG_OPERSVODKA.STG_COMMENTS_DQ_FILE_REGISTERED',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_COMMENTS_DQ_FILE_REGISTERED.sql'
    }
)

STG_OPERSVODKA_STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DML/STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET.sql'
    }
)

STG_OPERSVODKA_STG_DUMMY_CALENDAR_DQ_FILE_EXISTS = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_DUMMY_CALENDAR_DQ_FILE_EXISTS',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DML/STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_FILE_EXISTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_FILE_EXISTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_FILE_EXISTS.sql'
    }
)

STG_OPERSVODKA_STG_DUMMY_CALENDAR_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_DUMMY_CALENDAR_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DML/STG_OPERSVODKA.STG_DUMMY_CALENDAR_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'STG_OPERSVODKA.STG_DUMMY_CALENDAR_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_DUMMY_CALENDAR_ETL_FILE_LOAD.sql'
    }
)

STG_OPERSVODKA_STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DML/STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED.sql'
    }
)

STG_OPERSVODKA_STG_REFERENCE_DQ_SOURCE_TARGET = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_REFERENCE_DQ_SOURCE_TARGET',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DML/STG_OPERSVODKA.STG_REFERENCE_DQ_SOURCE_TARGET.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_REFERENCE',
        'step' : 'STG_OPERSVODKA.STG_REFERENCE_DQ_SOURCE_TARGET',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_REFERENCE_DQ_SOURCE_TARGET.sql'
    }
)

STG_OPERSVODKA_STG_REFERENCE_DQ_FILE_EXISTS = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_REFERENCE_DQ_FILE_EXISTS',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DML/STG_OPERSVODKA.STG_REFERENCE_DQ_FILE_EXISTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_REFERENCE',
        'step' : 'STG_OPERSVODKA.STG_REFERENCE_DQ_FILE_EXISTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_REFERENCE_DQ_FILE_EXISTS.sql'
    }
)

STG_OPERSVODKA_STG_REFERENCE_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_REFERENCE_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DML/STG_OPERSVODKA.STG_REFERENCE_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_REFERENCE',
        'step' : 'STG_OPERSVODKA.STG_REFERENCE_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_REFERENCE_ETL_FILE_LOAD.sql'
    }
)

STG_OPERSVODKA_STG_REFERENCE_DQ_FILE_REGISTERED = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_REFERENCE_DQ_FILE_REGISTERED',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DML/STG_OPERSVODKA.STG_REFERENCE_DQ_FILE_REGISTERED.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_REFERENCE',
        'step' : 'STG_OPERSVODKA.STG_REFERENCE_DQ_FILE_REGISTERED',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_REFERENCE_DQ_FILE_REGISTERED.sql'
    }
)

STG_OPERSVODKA_STG_STG_HQ_REPORT_DQ_SOURCE_TARGET = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_STG_HQ_REPORT_DQ_SOURCE_TARGET',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DML/STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_SOURCE_TARGET.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_SOURCE_TARGET',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_SOURCE_TARGET.sql'
    }
)

STG_OPERSVODKA_STG_STG_HQ_REPORT_DQ_FILE_EXISTS = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_STG_HQ_REPORT_DQ_FILE_EXISTS',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DML/STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_FILE_EXISTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_FILE_EXISTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_FILE_EXISTS.sql'
    }
)

STG_OPERSVODKA_STG_STG_HQ_REPORT_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_STG_HQ_REPORT_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DML/STG_OPERSVODKA.STG_STG_HQ_REPORT_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'STG_OPERSVODKA.STG_STG_HQ_REPORT_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_STG_HQ_REPORT_ETL_FILE_LOAD.sql'
    }
)

STG_OPERSVODKA_STG_STG_HQ_REPORT_DQ_FILE_REGISTERED = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_STG_HQ_REPORT_DQ_FILE_REGISTERED',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DML/STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_FILE_REGISTERED.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_FILE_REGISTERED',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : STG_OPERSVODKA.STG_STG_HQ_REPORT_DQ_FILE_REGISTERED.sql'
    }
)

ODS_OPERSVODKA_ODS_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_ODS_COMMENTS',
    sql='DWH/OPERSVODKA/2-ODS_COMMENTS/DML/ODS_OPERSVODKA.ODS_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'ODS_COMMENTS',
        'step' : 'ODS_OPERSVODKA.ODS_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.ODS_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_ODS_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_ODS_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/2-ODS_STG_HQ_REPORT/DML/ODS_OPERSVODKA.ODS_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'ODS_STG_HQ_REPORT',
        'step' : 'ODS_OPERSVODKA.ODS_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.ODS_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_COMMENTS/DML/ODS_OPERSVODKA.DDS_HUB_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_COMMENTS',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HUB_COMMENTS_STG_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_DATE_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_DATE_STG_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DML/ODS_OPERSVODKA.DDS_HUB_DATE_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_DATE_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HUB_DATE_STG_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_DATE_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_DATE_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DML/ODS_OPERSVODKA.DDS_HUB_DATE_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_DATE_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HUB_DATE_STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DUMMY_CALENDAR/DML/ODS_OPERSVODKA.DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_DUMMY_CALENDAR',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DML/ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT_STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT_STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT_STG_REFERENCE.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DML/ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/4-DDS_LK_PLANT_PRODUCT_METRIC/DML/ODS_OPERSVODKA.DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_LK_PLANT_PRODUCT_METRIC',
        'step' : 'ODS_OPERSVODKA.DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_DDS_HST_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HST_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/5-DDS_HST_COMMENTS/DML/ODS_OPERSVODKA.DDS_HST_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HST_COMMENTS',
        'step' : 'ODS_OPERSVODKA.DDS_HST_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HST_COMMENTS_STG_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DML/ODS_OPERSVODKA.DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HST_DUMMY_CALENDAR',
        'step' : 'ODS_OPERSVODKA.DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql'
    }
)

ODS_OPERSVODKA_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE',
    sql='DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DML/ODS_OPERSVODKA.DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HST_PLANT_PRODUCT_REFERENCE',
        'step' : 'ODS_OPERSVODKA.DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE.sql'
    }
)

ODS_OPERSVODKA_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DML/ODS_OPERSVODKA.DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_LST_PLANT_PRODUCT_METRIC',
        'step' : 'ODS_OPERSVODKA.DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : ODS_OPERSVODKA.DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql'
    }
)

