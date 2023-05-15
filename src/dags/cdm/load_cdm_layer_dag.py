from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.variable import Variable
from helpers.task_factories import create_postgres_operator
from airflow.sensors.external_task_sensor import ExternalTaskSensor


sql_path = Variable.get('CDM_SQL_FILES_PATH')

args = {
    'owner': 'ionovv-ilya',
    'start_date': datetime(2023, 5, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
        'load_cdm_layer',
        default_args=args,
        schedule_interval='0 5 * * *',
        template_searchpath=sql_path,
        catchup=False,
        description='Даг формирует CDM витрины',
        tags=['dm', 'cdm', ]
) as dag:
    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')
    cpr = create_postgres_operator

    load_dds_layer_sensor = ExternalTaskSensor(
        task_id='load_cdm_layer_sensor',
        external_dag_id='load_dds_layer',
        mode='poke'
    )

    build_cdm = [cpr(table) for table in (
        'cdm.dm_settlement_report',
        'cdm.dm_courier_ledger', )
    ]

    start >> load_dds_layer_sensor >> build_cdm >> finish
