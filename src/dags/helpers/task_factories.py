from airflow.operators.postgres_operator import PostgresOperator


def create_postgres_operator(table_name, conn_id='PG_WAREHOUSE_CONNECTION'):
    return PostgresOperator(
        task_id=f"load_{table_name.replace('.', '_')}",
        postgres_conn_id=conn_id,
        sql=f"{table_name}.sql"
    )
