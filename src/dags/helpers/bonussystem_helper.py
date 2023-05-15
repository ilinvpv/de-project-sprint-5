from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_data_from_bonussystem(table, columns, threshold=0):
    hook = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')

    select_query = f"SELECT {', '.join(col for col in columns)} " \
                   f"FROM {table} " \
                   f"WHERE id > %(threshold)s " \
                   f"ORDER BY id ASC"

    with hook.get_conn() as conn, conn.cursor() as cursor:
        cursor.execute(select_query, {"threshold": threshold})
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
