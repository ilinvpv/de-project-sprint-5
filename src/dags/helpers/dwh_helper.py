import logging
from airflow.hooks.postgres_hook import PostgresHook
from typing import Any, Dict, List
import json


class DWHHelper:
    SETTINGS_TABLE = 'meta.srv_wf_settings'

    def __init__(self, postgres_conn_id: str):
        """
        :param postgres_conn_id: The PostgreSQL connection ID in Airflow.
        """

        self.postgres_conn_id = postgres_conn_id
        self.hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    def __enter__(self):
        self._executor = self.executor()
        self._executor.send(None)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.close()

    def executor(self):
        with self.hook.get_conn() as conn, conn.cursor() as cursor:
            try:
                while True:
                    q, data = (yield)
                    cursor.executemany(q, data)

            except GeneratorExit:
                logging.info(f"Обновлено {cursor.rowcount} строк")

        logging.info(f"Статус транзакции {conn.get_transaction_status()}")

    def get_settings(self, wf_key: str) -> Dict[str, Any]:
        query = f"""
                SELECT workflow_settings
                FROM {self.SETTINGS_TABLE}
                WHERE workflow_key = %(workflow_key)s;
        """

        records = self.hook.get_records(
            query, {"workflow_key": wf_key}
        )

        if records:
            workflow_settings = records[0][0]
            return workflow_settings

        return {}

    def set_settings(self, wf_key: str, wf_settings: Dict):
        data_settings = [{
            "workflow_key": wf_key,
            "workflow_settings": json.dumps(wf_settings)
        }, ]

        self.load_data(
            table=self.SETTINGS_TABLE,
            data=data_settings,
            replace=True,
            by_field='workflow_key'
        )

    def load_data(self, table: str, data: List[Dict[str, Any]], replace=False,
                  by_field: str = None):
        if replace and not by_field:
            self._executor.throw(ValueError)
            raise ValueError(
                "PostgreSQL ON CONFLICT upsert syntax requires an unique index"
            )

        try:
            columns = tuple(data[0].keys())
            values = [tuple(row.values()) for row in data]
            query = self._get_insert_query(table, columns, by_field, replace)
            self._executor.send((query, values,))
        except Exception as e:
            self._executor.throw(Exception(e))

    @staticmethod
    def _get_insert_query(table: str, columns: tuple, by_field: str,
                          replace: bool) -> str:

        columns_line = ', '.join(col for col in columns)
        values_line = ', '.join(['%s' for _ in columns])

        q = f"INSERT INTO {table} ({columns_line}) VALUES ({values_line})"

        if replace:
            set_line = ', '.join(
                [f'{col} = EXCLUDED.{col}' for col in columns
                 if col != by_field]
            )
            q += f"ON CONFLICT ({by_field}) DO UPDATE SET {set_line};"

        return q
