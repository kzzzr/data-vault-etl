from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.vertica_extended import VerticaOperatorExtended
from airflow.operators.dummy_operator import DummyOperator


args = {
    'owner': 'kozyras@sibur.ru',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 10),
    'email': ['kozyras@sibur.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
    'catch_up': True
    # 'schedule_interval': @once,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id='OPERSVODKA_DWH',
    default_args=args,
    schedule_interval=None
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

stage_done = DummyOperator(
    task_id='stage_done',
    dag=dag
)

ods_done = DummyOperator(
    task_id='ods_done',
    dag=dag
)

dwh_done = DummyOperator(
    task_id='dwh_done',
    dag=dag
)

finish = DummyOperator(
    task_id='finish',
    dag=dag
)

OPERSVOD_STG_STG_COMMENTS_DQ_SOURCE_TARGET = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_COMMENTS_DQ_SOURCE_TARGET',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DML/OPERSVOD_STG.STG_COMMENTS_DQ_SOURCE_TARGET.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_COMMENTS',
        'step' : 'OPERSVOD_STG.STG_COMMENTS_DQ_SOURCE_TARGET',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_COMMENTS_DQ_SOURCE_TARGET.sql'
    }
)

OPERSVOD_STG_STG_COMMENTS_DQ_FILE_EXISTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_COMMENTS_DQ_FILE_EXISTS',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DML/OPERSVOD_STG.STG_COMMENTS_DQ_FILE_EXISTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_COMMENTS',
        'step' : 'OPERSVOD_STG.STG_COMMENTS_DQ_FILE_EXISTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_COMMENTS_DQ_FILE_EXISTS.sql'
    }
)

OPERSVOD_STG_STG_COMMENTS_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_COMMENTS_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DML/OPERSVOD_STG.STG_COMMENTS_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_COMMENTS',
        'step' : 'OPERSVOD_STG.STG_COMMENTS_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_COMMENTS_ETL_FILE_LOAD.sql'
    }
)

OPERSVOD_STG_STG_COMMENTS_DQ_FILE_REGISTERED = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_COMMENTS_DQ_FILE_REGISTERED',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DML/OPERSVOD_STG.STG_COMMENTS_DQ_FILE_REGISTERED.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_COMMENTS',
        'step' : 'OPERSVOD_STG.STG_COMMENTS_DQ_FILE_REGISTERED',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_COMMENTS_DQ_FILE_REGISTERED.sql'
    }
)

OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DML/OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET.sql'
    }
)

OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_FILE_EXISTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_FILE_EXISTS',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DML/OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_FILE_EXISTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_FILE_EXISTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_FILE_EXISTS.sql'
    }
)

OPERSVOD_STG_STG_DUMMY_CALENDAR_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_DUMMY_CALENDAR_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DML/OPERSVOD_STG.STG_DUMMY_CALENDAR_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.STG_DUMMY_CALENDAR_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_DUMMY_CALENDAR_ETL_FILE_LOAD.sql'
    }
)

OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DML/OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED.sql'
    }
)

OPERSVOD_STG_STG_REFERENCE_DQ_SOURCE_TARGET = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_REFERENCE_DQ_SOURCE_TARGET',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DML/OPERSVOD_STG.STG_REFERENCE_DQ_SOURCE_TARGET.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_REFERENCE',
        'step' : 'OPERSVOD_STG.STG_REFERENCE_DQ_SOURCE_TARGET',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_REFERENCE_DQ_SOURCE_TARGET.sql'
    }
)

OPERSVOD_STG_STG_REFERENCE_DQ_FILE_EXISTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_REFERENCE_DQ_FILE_EXISTS',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DML/OPERSVOD_STG.STG_REFERENCE_DQ_FILE_EXISTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_REFERENCE',
        'step' : 'OPERSVOD_STG.STG_REFERENCE_DQ_FILE_EXISTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_REFERENCE_DQ_FILE_EXISTS.sql'
    }
)

OPERSVOD_STG_STG_REFERENCE_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_REFERENCE_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DML/OPERSVOD_STG.STG_REFERENCE_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_REFERENCE',
        'step' : 'OPERSVOD_STG.STG_REFERENCE_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_REFERENCE_ETL_FILE_LOAD.sql'
    }
)

OPERSVOD_STG_STG_REFERENCE_DQ_FILE_REGISTERED = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_REFERENCE_DQ_FILE_REGISTERED',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DML/OPERSVOD_STG.STG_REFERENCE_DQ_FILE_REGISTERED.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_REFERENCE',
        'step' : 'OPERSVOD_STG.STG_REFERENCE_DQ_FILE_REGISTERED',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_REFERENCE_DQ_FILE_REGISTERED.sql'
    }
)

OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_SOURCE_TARGET = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_SOURCE_TARGET',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DML/OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_SOURCE_TARGET.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_SOURCE_TARGET',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_SOURCE_TARGET.sql'
    }
)

OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_FILE_EXISTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_FILE_EXISTS',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DML/OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_FILE_EXISTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_FILE_EXISTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_FILE_EXISTS.sql'
    }
)

OPERSVOD_STG_STG_STG_HQ_REPORT_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_STG_HQ_REPORT_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DML/OPERSVOD_STG.STG_STG_HQ_REPORT_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.STG_STG_HQ_REPORT_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_STG_HQ_REPORT_ETL_FILE_LOAD.sql'
    }
)

OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_FILE_REGISTERED = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_FILE_REGISTERED',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DML/OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_FILE_REGISTERED.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_FILE_REGISTERED',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.STG_STG_HQ_REPORT_DQ_FILE_REGISTERED.sql'
    }
)

OPERSVOD_STG_ODS_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_ODS_COMMENTS',
    sql='DWH/OPERSVODKA/2-ODS_COMMENTS/DML/OPERSVOD_STG.ODS_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'ODS_COMMENTS',
        'step' : 'OPERSVOD_STG.ODS_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.ODS_COMMENTS.sql'
    }
)

OPERSVOD_STG_ODS_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_ODS_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/2-ODS_STG_HQ_REPORT/DML/OPERSVOD_STG.ODS_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'ODS_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.ODS_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.ODS_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_DDS_HUB_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_COMMENTS/DML/OPERSVOD_STG.DDS_HUB_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_COMMENTS',
        'step' : 'OPERSVOD_STG.DDS_HUB_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_HUB_COMMENTS_STG_COMMENTS.sql'
    }
)

OPERSVOD_STG_DDS_HUB_DATE_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_DATE_STG_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DML/OPERSVOD_STG.DDS_HUB_DATE_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'OPERSVOD_STG.DDS_HUB_DATE_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_HUB_DATE_STG_COMMENTS.sql'
    }
)

OPERSVOD_STG_DDS_HUB_DATE_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_DATE_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DML/OPERSVOD_STG.DDS_HUB_DATE_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'OPERSVOD_STG.DDS_HUB_DATE_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_HUB_DATE_STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DUMMY_CALENDAR/DML/OPERSVOD_STG.DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql'
    }
)

OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DML/OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT_STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT_STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT_STG_REFERENCE.sql'
    }
)

OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DML/OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/4-DDS_LK_PLANT_PRODUCT_METRIC/DML/OPERSVOD_STG.DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_LK_PLANT_PRODUCT_METRIC',
        'step' : 'OPERSVOD_STG.DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DML/OPERSVOD_STG.DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HST_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql'
    }
)

OPERSVOD_STG_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE',
    sql='DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DML/OPERSVOD_STG.DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_HST_PLANT_PRODUCT_REFERENCE',
        'step' : 'OPERSVOD_STG.DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE.sql'
    }
)

OPERSVOD_STG_DDS_LST_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_LST_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/5-DDS_LST_COMMENTS/DML/OPERSVOD_STG.DDS_LST_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_LST_COMMENTS',
        'step' : 'OPERSVOD_STG.DDS_LST_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_LST_COMMENTS_STG_COMMENTS.sql'
    }
)

OPERSVOD_STG_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DML/OPERSVOD_STG.DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DML',
        'obj' : 'DDS_LST_PLANT_PRODUCT_METRIC',
        'step' : 'OPERSVOD_STG.DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'LOAD DATA WAREHOUSE : OPERSVOD_STG.DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql'
    }
)


start >> OPERSVOD_STG_STG_COMMENTS_DQ_SOURCE_TARGET >> OPERSVOD_STG_STG_COMMENTS_DQ_FILE_EXISTS >> OPERSVOD_STG_STG_COMMENTS_ETL_FILE_LOAD >> OPERSVOD_STG_STG_COMMENTS_DQ_FILE_REGISTERED >> stage_done
start >> OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_SOURCE_TARGET >> OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_FILE_EXISTS >> OPERSVOD_STG_STG_DUMMY_CALENDAR_ETL_FILE_LOAD >> OPERSVOD_STG_STG_DUMMY_CALENDAR_DQ_FILE_REGISTERED >> stage_done
start >> OPERSVOD_STG_STG_REFERENCE_DQ_SOURCE_TARGET >> OPERSVOD_STG_STG_REFERENCE_DQ_FILE_EXISTS >> OPERSVOD_STG_STG_REFERENCE_ETL_FILE_LOAD >> OPERSVOD_STG_STG_REFERENCE_DQ_FILE_REGISTERED >> stage_done
start >> OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_SOURCE_TARGET >> OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_FILE_EXISTS >> OPERSVOD_STG_STG_STG_HQ_REPORT_ETL_FILE_LOAD >>  OPERSVOD_STG_STG_STG_HQ_REPORT_DQ_FILE_REGISTERED >> stage_done

stage_done >> OPERSVOD_STG_ODS_COMMENTS >> ods_done
stage_done >> OPERSVOD_STG_ODS_STG_HQ_REPORT >> ods_done

ods_done >> OPERSVOD_STG_DDS_HUB_COMMENTS_STG_COMMENTS >> dwh_done
ods_done >> OPERSVOD_STG_DDS_HUB_DATE_STG_COMMENTS >> dwh_done
ods_done >> OPERSVOD_STG_DDS_HUB_DATE_STG_STG_HQ_REPORT >> dwh_done
ods_done >> OPERSVOD_STG_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR >> dwh_done
ods_done >> OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE >> dwh_done
ods_done >> OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT >> dwh_done
ods_done >> OPERSVOD_STG_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT >> dwh_done
ods_done >> OPERSVOD_STG_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR >> dwh_done
ods_done >> OPERSVOD_STG_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE >> dwh_done
ods_done >> OPERSVOD_STG_DDS_LST_COMMENTS_STG_COMMENTS >> dwh_done
ods_done >> OPERSVOD_STG_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT >> dwh_done

dwh_done >> finish







