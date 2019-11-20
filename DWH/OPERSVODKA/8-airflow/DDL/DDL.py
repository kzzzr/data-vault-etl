STG_OPERSVODKA_ETL_LOAD_LOG = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_ETL_LOAD_LOG',
    sql='DWH/OPERSVODKA/0-META/DDL/STG_OPERSVODKA.ETL_LOAD_LOG.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'STG_OPERSVODKA.ETL_LOAD_LOG',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : STG_OPERSVODKA.ETL_LOAD_LOG.sql'
    }
)

STG_OPERSVODKA_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/0-META/DDL/STG_OPERSVODKA.ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'STG_OPERSVODKA.ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : STG_OPERSVODKA.ETL_FILE_LOAD.sql'
    }
)

STG_OPERSVODKA_V_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_V_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/0-META/DDL/STG_OPERSVODKA.V_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'STG_OPERSVODKA.V_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : STG_OPERSVODKA.V_ETL_FILE_LOAD.sql'
    }
)

STG_OPERSVODKA_DQ_LOG = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_DQ_LOG',
    sql='DWH/OPERSVODKA/0-META/DDL/STG_OPERSVODKA.DQ_LOG.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'STG_OPERSVODKA.DQ_LOG',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : STG_OPERSVODKA.DQ_LOG.sql'
    }
)

STG_OPERSVODKA_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_COMMENTS',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DDL/STG_OPERSVODKA.STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'STG_COMMENTS',
        'step' : 'STG_OPERSVODKA.STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : STG_OPERSVODKA.STG_COMMENTS.sql'
    }
)

STG_OPERSVODKA_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DDL/STG_OPERSVODKA.STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'STG_OPERSVODKA.STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : STG_OPERSVODKA.STG_DUMMY_CALENDAR.sql'
    }
)

STG_OPERSVODKA_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_REFERENCE',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DDL/STG_OPERSVODKA.STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'STG_REFERENCE',
        'step' : 'STG_OPERSVODKA.STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : STG_OPERSVODKA.STG_REFERENCE.sql'
    }
)

STG_OPERSVODKA_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='STG_OPERSVODKA_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DDL/STG_OPERSVODKA.STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'STG_OPERSVODKA.STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : STG_OPERSVODKA.STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_ODS_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_ODS_COMMENTS',
    sql='DWH/OPERSVODKA/2-ODS_COMMENTS/DDL/ODS_OPERSVODKA.ODS_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'ODS_COMMENTS',
        'step' : 'ODS_OPERSVODKA.ODS_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.ODS_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_V_ODS_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_ODS_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/2-ODS_COMMENTS/DDL/ODS_OPERSVODKA.V_ODS_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'ODS_COMMENTS',
        'step' : 'ODS_OPERSVODKA.V_ODS_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_ODS_COMMENTS_STG_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_ODS_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_ODS_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/2-ODS_STG_HQ_REPORT/DDL/ODS_OPERSVODKA.ODS_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'ODS_STG_HQ_REPORT',
        'step' : 'ODS_OPERSVODKA.ODS_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.ODS_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/2-ODS_STG_HQ_REPORT/DDL/ODS_OPERSVODKA.V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'ODS_STG_HQ_REPORT',
        'step' : 'ODS_OPERSVODKA.V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_COMMENTS/DDL/ODS_OPERSVODKA.DDS_HUB_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_COMMENTS',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_HUB_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HUB_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HUB_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_COMMENTS/DDL/ODS_OPERSVODKA.V_DDS_HUB_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_COMMENTS',
        'step' : 'ODS_OPERSVODKA.V_DDS_HUB_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HUB_COMMENTS_STG_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_DATE = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_DATE',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/ODS_OPERSVODKA.DDS_HUB_DATE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_DATE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_HUB_DATE.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HUB_DATE_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HUB_DATE_STG_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/ODS_OPERSVODKA.V_DDS_HUB_DATE_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'ODS_OPERSVODKA.V_DDS_HUB_DATE_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HUB_DATE_STG_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HUB_DATE_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HUB_DATE_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/ODS_OPERSVODKA.V_DDS_HUB_DATE_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'ODS_OPERSVODKA.V_DDS_HUB_DATE_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HUB_DATE_STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DUMMY_CALENDAR/DDL/ODS_OPERSVODKA.DDS_HUB_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DUMMY_CALENDAR',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_HUB_DUMMY_CALENDAR.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DUMMY_CALENDAR/DDL/ODS_OPERSVODKA.V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DUMMY_CALENDAR',
        'step' : 'ODS_OPERSVODKA.V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql'
    }
)

ODS_OPERSVODKA_DDS_HUB_PLANT_PRODUCT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HUB_PLANT_PRODUCT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_HUB_PLANT_PRODUCT.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/ODS_OPERSVODKA.V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'ODS_OPERSVODKA.V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/ODS_OPERSVODKA.V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'ODS_OPERSVODKA.V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_DDS_LK_PLANT_PRODUCT_METRIC = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_LK_PLANT_PRODUCT_METRIC',
    sql='DWH/OPERSVODKA/4-DDS_LK_PLANT_PRODUCT_METRIC/DDL/ODS_OPERSVODKA.DDS_LK_PLANT_PRODUCT_METRIC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LK_PLANT_PRODUCT_METRIC',
        'step' : 'ODS_OPERSVODKA.DDS_LK_PLANT_PRODUCT_METRIC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_LK_PLANT_PRODUCT_METRIC.sql'
    }
)

ODS_OPERSVODKA_V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/4-DDS_LK_PLANT_PRODUCT_METRIC/DDL/ODS_OPERSVODKA.V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LK_PLANT_PRODUCT_METRIC',
        'step' : 'ODS_OPERSVODKA.V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_DDS_HST_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HST_COMMENTS',
    sql='DWH/OPERSVODKA/5-DDS_HST_COMMENTS/DDL/ODS_OPERSVODKA.DDS_HST_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_COMMENTS',
        'step' : 'ODS_OPERSVODKA.DDS_HST_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_HST_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HST_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HST_COMMENTS',
    sql='DWH/OPERSVODKA/5-DDS_HST_COMMENTS/DDL/ODS_OPERSVODKA.V_DDS_HST_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_COMMENTS',
        'step' : 'ODS_OPERSVODKA.V_DDS_HST_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HST_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HST_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HST_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/5-DDS_HST_COMMENTS/DDL/ODS_OPERSVODKA.V_DDS_HST_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_COMMENTS',
        'step' : 'ODS_OPERSVODKA.V_DDS_HST_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HST_COMMENTS_STG_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_DDS_HST_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HST_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/ODS_OPERSVODKA.DDS_HST_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_DUMMY_CALENDAR',
        'step' : 'ODS_OPERSVODKA.DDS_HST_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_HST_DUMMY_CALENDAR.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HST_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HST_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/ODS_OPERSVODKA.V_DDS_HST_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_DUMMY_CALENDAR',
        'step' : 'ODS_OPERSVODKA.V_DDS_HST_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HST_DUMMY_CALENDAR.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/ODS_OPERSVODKA.V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_DUMMY_CALENDAR',
        'step' : 'ODS_OPERSVODKA.V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql'
    }
)

ODS_OPERSVODKA_DDS_HST_PLANT_PRODUCT_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_HST_PLANT_PRODUCT_REFERENCE',
    sql='DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/ODS_OPERSVODKA.DDS_HST_PLANT_PRODUCT_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_PLANT_PRODUCT_REFERENCE',
        'step' : 'ODS_OPERSVODKA.DDS_HST_PLANT_PRODUCT_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_HST_PLANT_PRODUCT_REFERENCE.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HST_PLANT_PRODUCT_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HST_PLANT_PRODUCT_REFERENCE',
    sql='DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/ODS_OPERSVODKA.V_DDS_HST_PLANT_PRODUCT_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_PLANT_PRODUCT_REFERENCE',
        'step' : 'ODS_OPERSVODKA.V_DDS_HST_PLANT_PRODUCT_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HST_PLANT_PRODUCT_REFERENCE.sql'
    }
)

ODS_OPERSVODKA_V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE',
    sql='DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/ODS_OPERSVODKA.V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_PLANT_PRODUCT_REFERENCE',
        'step' : 'ODS_OPERSVODKA.V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE.sql'
    }
)

ODS_OPERSVODKA_DDS_LST_PLANT_PRODUCT_METRIC = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DDS_LST_PLANT_PRODUCT_METRIC',
    sql='DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/ODS_OPERSVODKA.DDS_LST_PLANT_PRODUCT_METRIC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_PLANT_PRODUCT_METRIC',
        'step' : 'ODS_OPERSVODKA.DDS_LST_PLANT_PRODUCT_METRIC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DDS_LST_PLANT_PRODUCT_METRIC.sql'
    }
)

ODS_OPERSVODKA_V_DDS_LST_PLANT_PRODUCT_METRIC = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_LST_PLANT_PRODUCT_METRIC',
    sql='DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/ODS_OPERSVODKA.V_DDS_LST_PLANT_PRODUCT_METRIC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_PLANT_PRODUCT_METRIC',
        'step' : 'ODS_OPERSVODKA.V_DDS_LST_PLANT_PRODUCT_METRIC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_LST_PLANT_PRODUCT_METRIC.sql'
    }
)

ODS_OPERSVODKA_V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/ODS_OPERSVODKA.V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_PLANT_PRODUCT_METRIC',
        'step' : 'ODS_OPERSVODKA.V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql'
    }
)

ODS_OPERSVODKA_V_DDS_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_COMMENTS',
    sql='DWH/OPERSVODKA/6-DM_COMMENTS/DDL/ODS_OPERSVODKA.V_DDS_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_COMMENTS',
        'step' : 'ODS_OPERSVODKA.V_DDS_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_DM_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DM_COMMENTS',
    sql='DWH/OPERSVODKA/6-DM_COMMENTS/DDL/ODS_OPERSVODKA.DM_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_COMMENTS',
        'step' : 'ODS_OPERSVODKA.DM_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DM_COMMENTS.sql'
    }
)

ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC.sql'
    }
)

ODS_OPERSVODKA_V_DDS_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.V_DDS_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.V_DDS_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_DUMMY_CALENDAR.sql'
    }
)

ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_BASE = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_BASE',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_BASE.sql'
    }
)

ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_PNG = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_PNG',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_PNG.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_PNG',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_PNG.sql'
    }
)

ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_SOG = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_SOG',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_SOG.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_SOG',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_SOG.sql'
    }
)

ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_UVS = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_UVS',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_UVS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_UVS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_UVS.sql'
    }
)

ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_CALC = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_CALC',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_CALC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_CALC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_CALC.sql'
    }
)

ODS_OPERSVODKA_DM_PLANT_PRODUCT_METRIC_CALC = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DM_PLANT_PRODUCT_METRIC_CALC',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.DM_PLANT_PRODUCT_METRIC_CALC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.DM_PLANT_PRODUCT_METRIC_CALC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DM_PLANT_PRODUCT_METRIC_CALC.sql'
    }
)

ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_CALC_C = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_V_DDS_PLANT_PRODUCT_METRIC_CALC_C',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_CALC_C.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_CALC_C',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.V_DDS_PLANT_PRODUCT_METRIC_CALC_C.sql'
    }
)

ODS_OPERSVODKA_DM_OPER_STRAIT = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DM_OPER_STRAIT',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.DM_OPER_STRAIT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.DM_OPER_STRAIT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DM_OPER_STRAIT.sql'
    }
)

ODS_OPERSVODKA_DM_OPER_MAIN = VerticaOperatorExtended(
    dag=dag,
    task_id='ODS_OPERSVODKA_DM_OPER_MAIN',
    sql='DWH/OPERSVODKA/6-DM_OPER/DDL/ODS_OPERSVODKA.DM_OPER_MAIN.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DM_OPER',
        'step' : 'ODS_OPERSVODKA.DM_OPER_MAIN',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : ODS_OPERSVODKA.DM_OPER_MAIN.sql'
    }
)

1_DQ_DDL_BENCHMARK = VerticaOperatorExtended(
    dag=dag,
    task_id='1_DQ_DDL_BENCHMARK',
    sql='DWH/OPERSVODKA/7-DQ/DDL/1_DQ_DDL_BENCHMARK.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DQ',
        'step' : '1_DQ_DDL_BENCHMARK',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : 1_DQ_DDL_BENCHMARK.sql'
    }
)

2_DQ_DDL_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='2_DQ_DDL_REFERENCE',
    sql='DWH/OPERSVODKA/7-DQ/DDL/2_DQ_DDL_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DQ',
        'step' : '2_DQ_DDL_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : 2_DQ_DDL_REFERENCE.sql'
    }
)

3_DQ_DDL_TEMP = VerticaOperatorExtended(
    dag=dag,
    task_id='3_DQ_DDL_TEMP',
    sql='DWH/OPERSVODKA/7-DQ/DDL/3_DQ_DDL_TEMP.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DQ',
        'step' : '3_DQ_DDL_TEMP',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : 3_DQ_DDL_TEMP.sql'
    }
)

4_DQ_DDL_STRAIT = VerticaOperatorExtended(
    dag=dag,
    task_id='4_DQ_DDL_STRAIT',
    sql='DWH/OPERSVODKA/7-DQ/DDL/4_DQ_DDL_STRAIT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DQ',
        'step' : '4_DQ_DDL_STRAIT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : 4_DQ_DDL_STRAIT.sql'
    }
)

5_DQ_DDL_MAIN = VerticaOperatorExtended(
    dag=dag,
    task_id='5_DQ_DDL_MAIN',
    sql='DWH/OPERSVODKA/7-DQ/DDL/5_DQ_DDL_MAIN.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DQ',
        'step' : '5_DQ_DDL_MAIN',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : 5_DQ_DDL_MAIN.sql'
    }
)

