from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from helpers.dwh_helper import DWHHelper
from helpers.bonussystem_helper import get_data_from_bonussystem

args = {
    'owner': 'ionovv-ilya',
    'start_date': datetime(2023, 5, 11),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def create_transfer_data_tasks():
    source_tables = {
        'ranks': ('id', 'name', 'bonus_percent', 'min_payment_threshold', ),
        'users': ('id', 'order_user_id', ),
        'outbox': ('id', 'event_ts', 'event_type', 'event_value', ),
    }

    dwh_mapping = {
        'ranks': 'stg.bonussystem_ranks',
        'users': 'stg.bonussystem_users',
        'outbox': 'stg.bonussystem_events',
    }

    dwh_helper = DWHHelper(postgres_conn_id='PG_WAREHOUSE_CONNECTION')

    def _transfer_data(src_table, dwh_table, columns, task_id):

        settings = dwh_helper.get_settings(task_id)

        if not settings:
            settings = {"last_loaded_id": 0}

        data = get_data_from_bonussystem(
            src_table, columns, settings.get("last_loaded_id")
        )

        if not data:
            logging.warning(f"Данные из таблицы {src_table} не были загружены."
                            f" id = {settings.get('last_loaded_id')}")
            return

        settings["last_loaded_id"] = data[-1]["id"]

        with dwh_helper as transaction:
            transaction.load_data(table=dwh_table, data=data, replace=True, by_field="id")
            transaction.set_settings(task_id, settings)

    for src_table, columns in source_tables.items():
        dwh_table = dwh_mapping[src_table]
        task_id = f"load_{src_table}_to_{dwh_table.replace('.', '_')}"

        yield PythonOperator(
            task_id=task_id,
            python_callable=_transfer_data,
            op_kwargs={
                "src_table": src_table,
                "dwh_table": dwh_table,
                "columns": columns,
                "task_id": task_id
            }
        )


with DAG(
        dag_id='load_data_from_bonus_system',
        default_args=args,
        schedule_interval='0/15 * * * *',
        description='Даг загружает данные из сервиса бонусов в stage-слой DWH',
        catchup=False,
        tags=['stage', 'postgres', 'bonus_system']
) as dag:
    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')
    load_bonussystem = list(create_transfer_data_tasks())

    start >> load_bonussystem >> finish
