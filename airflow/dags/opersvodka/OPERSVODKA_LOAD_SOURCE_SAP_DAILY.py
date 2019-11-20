from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.vertica_extended import VerticaOperatorExtended
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from opersvodka.etlfw.vertica.worker import Worker
from opersvodka.etlfw.source.load_source import load_sap_mii

def load_sap_mii_STG_HQ_REPORT():
    with Worker() as worker:
        load_sap_mii(worker)

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
    dag_id='OPERSVODKA_LOAD_SOURCE_SAP_DAILY',
    default_args=args,
    schedule_interval=None
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

truncate_done = DummyOperator(
    task_id='truncate_done',
    dag=dag
)

load_done = DummyOperator(
    task_id='load_done',
    dag=dag
)

finish = DummyOperator(
    task_id='finish',
    dag=dag
)

OPERSVOD_STG_TRUNCATE_OPERSVOD_STG_STG_STG_HQ_REPORT = VerticaOperatorExtended(
    dag=dag,
    task_id='OPERSVOD_STG_TRUNCATE_OPERSVOD_STG_STG_STG_HQ_REPORT',
    sql='DWH/OPERSVODKA/1-STG_STG_HQ_REPORT/TRUNCATE/OPERSVOD_STG.TRUNCATE_OPERSVOD_STG.STG_STG_HQ_REPORT.sql',
    log_meta={
        'log_table' : 'OPERSVOD_STG.ETL_LOAD_LOG',
        'stage' : 'TRUNCATE',
        'obj' : 'STG_STG_HQ_REPORT',
        'step' : 'OPERSVOD_STG.TRUNCATE_OPERSVOD_STG.STG_STG_HQ_REPORT',
        'rowcount' : '',
        'status' : 'OK',
        'descr' : 'TRUNCATE STAGE TABLE : OPERSVOD_STG.TRUNCATE_OPERSVOD_STG.STG_STG_HQ_REPORT.sql'
    }
)

OPERSVOD_STG_LOAD_STG_HQ_REPORT = PythonOperator(
    task_id='OPERSVOD_STG_LOAD_STG_HQ_REPORT',
    provide_context=False,
    python_callable=load_sap_mii_STG_HQ_REPORT,
    dag=dag,
)

trigger_dag_OPERSVODKA_DWH = TriggerDagRunOperator(
    task_id='trigger_dag_OPERSVODKA_DWH',
    trigger_dag_id="OPERSVODKA_DWH",
    dag=dag,
)

start >> OPERSVOD_STG_TRUNCATE_OPERSVOD_STG_STG_STG_HQ_REPORT >> truncate_done

truncate_done >> OPERSVOD_STG_LOAD_STG_HQ_REPORT >> load_done

load_done >> trigger_dag_OPERSVODKA_DWH

trigger_dag_OPERSVODKA_DWH >> finish