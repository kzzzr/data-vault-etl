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
    dag_id='OPERSVODKA_CREATE_OBJECTS',
    default_args=args,
    schedule_interval=None
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

meta_done = DummyOperator(
    task_id='meta_done',
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

hubs_done = DummyOperator(
    task_id='hubs_done',
    dag=dag
)

links_done = DummyOperator(
    task_id='links_done',
    dag=dag
)

satellites_done = DummyOperator(
    task_id='satellites_done',
    dag=dag
)

finish = DummyOperator(
    task_id='finish',
    dag=dag
)

OPERSVOD_STG_ETL_LOAD_LOG = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_ETL_LOAD_LOG',
    sql='DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.ETL_LOAD_LOG.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.ETL_LOAD_LOG.sql'
    }
)

OPERSVOD_STG_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'OPERSVOD_STG.ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.ETL_FILE_LOAD.sql'
    }
)

OPERSVOD_STG_V_ETL_FILE_LOAD = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_ETL_FILE_LOAD',
    sql='DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.V_ETL_FILE_LOAD.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'OPERSVOD_STG.V_ETL_FILE_LOAD',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.V_ETL_FILE_LOAD.sql'
    }
)

OPERSVOD_STG_DQ_LOG = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DQ_LOG',
    sql='DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.DQ_LOG.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'META',
        'step' : 'OPERSVOD_STG.DQ_LOG',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/0-META/DDL/OPERSVOD_STG.DQ_LOG.sql'
    }
)

OPERSVOD_STG_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_COMMENTS',
    sql='DWH/OPERSVODKA/1-STG_COMMENTS/DDL/OPERSVOD_STG.STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'STG_COMMENTS',
        'step' : 'OPERSVOD_STG.STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/1-STG_COMMENTS/DDL/OPERSVOD_STG.STG_COMMENTS.sql'
    }
)

OPERSVOD_STG_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DDL/OPERSVOD_STG.STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'STG_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/1-STG_DUMMY_CALENDAR/DDL/OPERSVOD_STG.STG_DUMMY_CALENDAR.sql'
    }
)

OPERSVOD_STG_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_REFERENCE',
    sql='DWH/OPERSVODKA/1-STG_REFERENCE/DDL/OPERSVOD_STG.STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'STG_REFERENCE',
        'step' : 'OPERSVOD_STG.STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/1-STG_REFERENCE/DDL/OPERSVOD_STG.STG_REFERENCE.sql'
    }
)

OPERSVOD_STG_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DDL/OPERSVOD_STG.STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/DDL/OPERSVOD_STG.STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_ODS_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_ODS_COMMENTS',
    sql='DWH/OPERSVODKA/2-ODS_COMMENTS/DDL/OPERSVOD_STG.ODS_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'ODS_COMMENTS',
        'step' : 'OPERSVOD_STG.ODS_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/2-ODS_COMMENTS/DDL/OPERSVOD_STG.ODS_COMMENTS.sql'
    }
)

OPERSVOD_STG_V_ODS_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_ODS_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/2-ODS_COMMENTS/DDL/OPERSVOD_STG.V_ODS_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'ODS_COMMENTS',
        'step' : 'OPERSVOD_STG.V_ODS_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/2-ODS_COMMENTS/DDL/OPERSVOD_STG.V_ODS_COMMENTS_STG_COMMENTS.sql'
    }
)

OPERSVOD_STG_ODS_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_ODS_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/2-ODS_STG_HQ_REPORT/DDL/OPERSVOD_STG.ODS_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'ODS_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.ODS_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/2-ODS_STG_HQ_REPORT/DDL/OPERSVOD_STG.ODS_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/2-ODS_STG_HQ_REPORT/DDL/OPERSVOD_STG.V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'ODS_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/2-ODS_STG_HQ_REPORT/DDL/OPERSVOD_STG.V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_DDS_HUB_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_COMMENTS/DDL/OPERSVOD_STG.DDS_HUB_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_COMMENTS',
        'step' : 'OPERSVOD_STG.DDS_HUB_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_COMMENTS/DDL/OPERSVOD_STG.DDS_HUB_COMMENTS.sql'
    }
)

OPERSVOD_STG_V_DDS_HUB_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HUB_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_COMMENTS/DDL/OPERSVOD_STG.V_DDS_HUB_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_COMMENTS',
        'step' : 'OPERSVOD_STG.V_DDS_HUB_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_COMMENTS/DDL/OPERSVOD_STG.V_DDS_HUB_COMMENTS_STG_COMMENTS.sql'
    }
)

OPERSVOD_STG_DDS_HUB_DATE = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_DATE',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/OPERSVOD_STG.DDS_HUB_DATE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'OPERSVOD_STG.DDS_HUB_DATE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/OPERSVOD_STG.DDS_HUB_DATE.sql'
    }
)

OPERSVOD_STG_V_DDS_HUB_DATE_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HUB_DATE_STG_COMMENTS',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/OPERSVOD_STG.V_DDS_HUB_DATE_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'OPERSVOD_STG.V_DDS_HUB_DATE_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/OPERSVOD_STG.V_DDS_HUB_DATE_STG_COMMENTS.sql'
    }
)

OPERSVOD_STG_V_DDS_HUB_DATE_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HUB_DATE_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/OPERSVOD_STG.V_DDS_HUB_DATE_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DATE',
        'step' : 'OPERSVOD_STG.V_DDS_HUB_DATE_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_DATE/DDL/OPERSVOD_STG.V_DDS_HUB_DATE_STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_DDS_HUB_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DUMMY_CALENDAR/DDL/OPERSVOD_STG.DDS_HUB_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.DDS_HUB_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_DUMMY_CALENDAR/DDL/OPERSVOD_STG.DDS_HUB_DUMMY_CALENDAR.sql'
    }
)

OPERSVOD_STG_V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/3-DDS_HUB_DUMMY_CALENDAR/DDL/OPERSVOD_STG.V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_DUMMY_CALENDAR/DDL/OPERSVOD_STG.V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql'
    }
)

OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/OPERSVOD_STG.DDS_HUB_PLANT_PRODUCT.sql'
    }
)

OPERSVOD_STG_V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/OPERSVOD_STG.V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'OPERSVOD_STG.V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/OPERSVOD_STG.V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE.sql'
    }
)

OPERSVOD_STG_V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/OPERSVOD_STG.V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HUB_PLANT_PRODUCT',
        'step' : 'OPERSVOD_STG.V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/3-DDS_HUB_PLANT_PRODUCT/DDL/OPERSVOD_STG.V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_DDS_LK_PLANT_PRODUCT_METRIC = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_LK_PLANT_PRODUCT_METRIC',
    sql='DWH/OPERSVODKA/4-DDS_LK_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.DDS_LK_PLANT_PRODUCT_METRIC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LK_PLANT_PRODUCT_METRIC',
        'step' : 'OPERSVOD_STG.DDS_LK_PLANT_PRODUCT_METRIC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/4-DDS_LK_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.DDS_LK_PLANT_PRODUCT_METRIC.sql'
    }
)

OPERSVOD_STG_V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/4-DDS_LK_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LK_PLANT_PRODUCT_METRIC',
        'step' : 'OPERSVOD_STG.V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/4-DDS_LK_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_DDS_HST_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HST_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/OPERSVOD_STG.DDS_HST_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.DDS_HST_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/OPERSVOD_STG.DDS_HST_DUMMY_CALENDAR.sql'
    }
)

OPERSVOD_STG_V_DDS_HST_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HST_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/OPERSVOD_STG.V_DDS_HST_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.V_DDS_HST_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/OPERSVOD_STG.V_DDS_HST_DUMMY_CALENDAR.sql'
    }
)

OPERSVOD_STG_V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
    sql='DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/OPERSVOD_STG.V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_DUMMY_CALENDAR',
        'step' : 'OPERSVOD_STG.V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_HST_DUMMY_CALENDAR/DDL/OPERSVOD_STG.V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR.sql'
    }
)

OPERSVOD_STG_DDS_HST_PLANT_PRODUCT_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_HST_PLANT_PRODUCT_REFERENCE',
    sql='DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/OPERSVOD_STG.DDS_HST_PLANT_PRODUCT_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_PLANT_PRODUCT_REFERENCE',
        'step' : 'OPERSVOD_STG.DDS_HST_PLANT_PRODUCT_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/OPERSVOD_STG.DDS_HST_PLANT_PRODUCT_REFERENCE.sql'
    }
)

OPERSVOD_STG_V_DDS_HST_PLANT_PRODUCT_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HST_PLANT_PRODUCT_REFERENCE',
    sql='DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/OPERSVOD_STG.V_DDS_HST_PLANT_PRODUCT_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_PLANT_PRODUCT_REFERENCE',
        'step' : 'OPERSVOD_STG.V_DDS_HST_PLANT_PRODUCT_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/OPERSVOD_STG.V_DDS_HST_PLANT_PRODUCT_REFERENCE.sql'
    }
)

OPERSVOD_STG_V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE',
    sql='DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/OPERSVOD_STG.V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_HST_PLANT_PRODUCT_REFERENCE',
        'step' : 'OPERSVOD_STG.V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_HST_PLANT_PRODUCT_REFERENCE/DDL/OPERSVOD_STG.V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE.sql'
    }
)

OPERSVOD_STG_DDS_LST_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_LST_COMMENTS',
    sql='DWH/OPERSVODKA/5-DDS_LST_COMMENTS/DDL/OPERSVOD_STG.DDS_LST_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_COMMENTS',
        'step' : 'OPERSVOD_STG.DDS_LST_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_LST_COMMENTS/DDL/OPERSVOD_STG.DDS_LST_COMMENTS.sql'
    }
)

OPERSVOD_STG_V_DDS_LST_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_LST_COMMENTS',
    sql='DWH/OPERSVODKA/5-DDS_LST_COMMENTS/DDL/OPERSVOD_STG.V_DDS_LST_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_COMMENTS',
        'step' : 'OPERSVOD_STG.V_DDS_LST_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_LST_COMMENTS/DDL/OPERSVOD_STG.V_DDS_LST_COMMENTS.sql'
    }
)

OPERSVOD_STG_V_DDS_LST_COMMENTS_STG_COMMENTS = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_LST_COMMENTS_STG_COMMENTS',
    sql='DWH/OPERSVODKA/5-DDS_LST_COMMENTS/DDL/OPERSVOD_STG.V_DDS_LST_COMMENTS_STG_COMMENTS.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_COMMENTS',
        'step' : 'OPERSVOD_STG.V_DDS_LST_COMMENTS_STG_COMMENTS',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_LST_COMMENTS/DDL/OPERSVOD_STG.V_DDS_LST_COMMENTS_STG_COMMENTS.sql'
    }
)

OPERSVOD_STG_DDS_LST_PLANT_PRODUCT_METRIC = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_DDS_LST_PLANT_PRODUCT_METRIC',
    sql='DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.DDS_LST_PLANT_PRODUCT_METRIC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_PLANT_PRODUCT_METRIC',
        'step' : 'OPERSVOD_STG.DDS_LST_PLANT_PRODUCT_METRIC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.DDS_LST_PLANT_PRODUCT_METRIC.sql'
    }
)

OPERSVOD_STG_V_DDS_LST_PLANT_PRODUCT_METRIC = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_LST_PLANT_PRODUCT_METRIC',
    sql='DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.V_DDS_LST_PLANT_PRODUCT_METRIC.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_PLANT_PRODUCT_METRIC',
        'step' : 'OPERSVOD_STG.V_DDS_LST_PLANT_PRODUCT_METRIC',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.V_DDS_LST_PLANT_PRODUCT_METRIC.sql'
    }
)

OPERSVOD_STG_V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'DDL',
        'obj' : 'DDS_LST_PLANT_PRODUCT_METRIC',
        'step' : 'OPERSVOD_STG.V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'CREATING OBJECT : /Users/artemiykozyr/Documents/sibur/ETL/DWH/OPERSVODKA/5-DDS_LST_PLANT_PRODUCT_METRIC/DDL/OPERSVOD_STG.V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT.sql'
    }
)

start >> OPERSVOD_STG_ETL_LOAD_LOG

OPERSVOD_STG_ETL_LOAD_LOG >> OPERSVOD_STG_ETL_FILE_LOAD >> OPERSVOD_STG_V_ETL_FILE_LOAD >> meta_done
OPERSVOD_STG_ETL_LOAD_LOG >> OPERSVOD_STG_DQ_LOG >> meta_done

meta_done >> OPERSVOD_STG_STG_COMMENTS >> stage_done
meta_done >> OPERSVOD_STG_STG_DUMMY_CALENDAR >> stage_done
meta_done >> OPERSVOD_STG_STG_REFERENCE >> stage_done
meta_done >> OPERSVOD_STG_STG_STG_HQ_REPORT >> stage_done

stage_done >> OPERSVOD_STG_ODS_COMMENTS >> OPERSVOD_STG_V_ODS_COMMENTS_STG_COMMENTS >> ods_done
stage_done >> OPERSVOD_STG_ODS_STG_HQ_REPORT >> OPERSVOD_STG_V_ODS_STG_HQ_REPORT_STG_STG_HQ_REPORT >> ods_done

ods_done >> OPERSVOD_STG_DDS_HUB_COMMENTS >> OPERSVOD_STG_V_DDS_HUB_COMMENTS_STG_COMMENTS >> hubs_done
ods_done >> OPERSVOD_STG_DDS_HUB_DATE >> OPERSVOD_STG_V_DDS_HUB_DATE_STG_COMMENTS >> hubs_done
ods_done >> OPERSVOD_STG_DDS_HUB_DATE >> OPERSVOD_STG_V_DDS_HUB_DATE_STG_STG_HQ_REPORT >> hubs_done
ods_done >> OPERSVOD_STG_DDS_HUB_DUMMY_CALENDAR >> OPERSVOD_STG_V_DDS_HUB_DUMMY_CALENDAR_STG_DUMMY_CALENDAR >> hubs_done
ods_done >> OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT >> OPERSVOD_STG_V_DDS_HUB_PLANT_PRODUCT_STG_REFERENCE >> hubs_done
ods_done >> OPERSVOD_STG_DDS_HUB_PLANT_PRODUCT >> OPERSVOD_STG_V_DDS_HUB_PLANT_PRODUCT_STG_STG_HQ_REPORT >> hubs_done

hubs_done >> OPERSVOD_STG_DDS_LK_PLANT_PRODUCT_METRIC >> OPERSVOD_STG_V_DDS_LK_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT >> links_done

links_done >> OPERSVOD_STG_DDS_HST_DUMMY_CALENDAR >> OPERSVOD_STG_V_DDS_HST_DUMMY_CALENDAR >> OPERSVOD_STG_V_DDS_HST_DUMMY_CALENDAR_STG_DUMMY_CALENDAR >> satellites_done
links_done >> OPERSVOD_STG_DDS_HST_PLANT_PRODUCT_REFERENCE >> OPERSVOD_STG_V_DDS_HST_PLANT_PRODUCT_REFERENCE >> OPERSVOD_STG_V_DDS_HST_PLANT_PRODUCT_REFERENCE_STG_REFERENCE >> satellites_done
links_done >> OPERSVOD_STG_DDS_LST_COMMENTS >> OPERSVOD_STG_V_DDS_LST_COMMENTS >> OPERSVOD_STG_V_DDS_LST_COMMENTS_STG_COMMENTS >> satellites_done
links_done >> OPERSVOD_STG_DDS_LST_PLANT_PRODUCT_METRIC >> OPERSVOD_STG_V_DDS_LST_PLANT_PRODUCT_METRIC >> OPERSVOD_STG_V_DDS_LST_PLANT_PRODUCT_METRIC_STG_STG_HQ_REPORT >> satellites_done

satellites_done >> finish









