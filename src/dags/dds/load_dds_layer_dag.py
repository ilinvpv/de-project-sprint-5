from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.variable import Variable
from helpers.task_factories import create_postgres_operator

sql_path = Variable.get('DDS_SQL_FILES_PATH')

args = {
    'owner': 'ionovv-ilya',
    'start_date': datetime(2023, 5, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'load_dds_layer',
        default_args=args,
        schedule_interval='0 5 * * *',
        template_searchpath=sql_path,
        catchup=False,
        description='Даг формирует DDS витрины',
        tags=['dm', 'dds', ]
) as dag:
    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    cpr = create_postgres_operator

    atomic_dm = [cpr(table) for table in (
        'dds.dm_users',
        'dds.dm_restaurants',
        'dds.dm_timestamps',
        'dds.dm_products',
        'dds.dm_couriers',
    )]

    start >> atomic_dm >> cpr('dds.dm_couriers_rewards') >> cpr('dds.dm_orders') >> cpr('dds.fct_product_sales') >> finish
