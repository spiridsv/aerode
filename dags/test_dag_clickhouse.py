import datetime
import pendulum
import os

from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator, ClickHouseHook
from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0].lstrip('dag_').upper()

DEFAULT_ARGS = {
    'owner': 'ssv',
    'start_date': pendulum.datetime(2023, 3, 30, tz="UTC"),
    'task_concurrency': 1
}

with DAG(
        dag_id=DAG_NAME,
        schedule=None,
        catchup=False,
        tags=['TEST', 'CLICKHOUSE'],
        description='Clickhouse test DAG',
        max_active_runs=1,
        concurrency=1,
        default_args=DEFAULT_ARGS,
        render_template_as_native_obj=True,
) as dag:
    def _check_count(task_id, **kwargs):
        data = kwargs['ti'].xcom_pull(task_ids=task_id)
        if not data:
            raise RuntimeError('No data was returned')
        print(f'Returned data: {data}')

        h = ClickHouseHook('ch')
        print(h.get_records('select 1,2,3;'))


    get_clickhouse_data = ClickHouseOperator(sql=(f"""select now();"""),
                                             clickhouse_conn_id='ch', task_id='get_clickhouse_data')

    check_clickhouse_data = PythonOperator(task_id='check_clickhouse_data', python_callable=_check_count,
                                           op_args=['get_clickhouse_data'])

    get_clickhouse_data >> check_clickhouse_data
