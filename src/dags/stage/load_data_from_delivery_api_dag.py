from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.state import State

from helpers.dwh_helper import DWHHelper
from helpers.api_helper import APIHelper


args = {
    'owner': 'ionovv-ilya',
    'start_date': datetime(2023, 5, 11),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


def create_api_task(api_method, table):
    task_id = f"{api_method}_to_{table.replace('.', '_')}"

    def _load_data(**context):
        pg_helper = DWHHelper(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        api_helper = APIHelper(connection_id='API_DELIVERY_CONNECTION')

        dict_settings = pg_helper.get_settings(task_id)
        params = api_helper.get_params(api_method, **dict_settings)

        step = int(params.limit)
        params.offset_inc(step)

        data = api_helper.get_data(api_method, params)

        if not data:
            logging.warning("Данные не были загружены, потому что апи метод "
                            f"`/{api_method}` не вернул результат")

            ti = context['ti']
            ti.set_state(State.SKIPPED)
            return

        # корректируем оффсет, если в ответе пришло меньше запрашиваемых строк
        params.offset = str(int(params.offset) - (step - len(data)))

        with pg_helper as transaction:
            transaction.load_data(table=table, data=data)
            transaction.set_settings(task_id, params.as_dict())

    return PythonOperator(
        task_id=task_id,
        python_callable=_load_data,
        provide_context=True
    )


with DAG(
        dag_id='load_data_from_delivery_api',
        default_args=args,
        schedule_interval='0/15 * * * *',
        description='Даг загружает данные из АПИ доставки в stage-слой DWH',

        tags=['stage', 'api', 'delivery']
) as dag:
    start = DummyOperator(task_id='start')
    load_couriers = create_api_task('couriers', 'stg.api_couriers')
    load_deliveries = create_api_task('deliveries', 'stg.api_deliveries')

    start >> [load_couriers, load_deliveries]
