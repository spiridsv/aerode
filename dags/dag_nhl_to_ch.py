import pendulum
import os

from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator, ClickHouseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from aerode_op import AeroDEAPItoClickhouseOperator

from utils import read_sql_query_from_file

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0].lstrip('dag_').upper()

DEFAULT_ARGS = {
    'owner': 'ssv',
    'start_date': pendulum.datetime(2023, 6, 19, tz="UTC"),
}

with DAG(
        dag_id=DAG_NAME,
        schedule='0 */12 * * *',  # None, -- debug
        catchup=False,
        tags=['EL', 'CLICKHOUSE'],
        description='Extract and Load data to Clickhouse',
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        render_template_as_native_obj=True,
        doc_md="simple EL dag"
) as dag:
    def _validate_uploaded_raw_json(ch_conn_id: str, api_id: int, uuid: str, src: str):
        ch_hook = ClickHouseHook(ch_conn_id)
        data = ch_hook.get_records(read_sql_query_from_file(
            os.path.join(os.path.dirname(__file__), 'sql', 'ch_validate_raw_json.sql')).format(src=src,
                                                                                               uuid=uuid,
                                                                                               api_id=api_id))
        if not data[0]:
            raise RuntimeError(f'Loaded JSON is wrong, UUID:{uuid}')


    extract_and_load_to_stg = AeroDEAPItoClickhouseOperator(connection_id='ch',
                                                            target_db_tn='{{ var.json.CH_SETTINGS.STG_DB_TN }}',
                                                            api_id='{{ var.json.CH_SETTINGS.API_ID }}',
                                                            task_id='extract_and_load_to_stg'
                                                            )
    validate_uploaded_raw_json = PythonOperator(task_id='check_raw_json', python_callable=_validate_uploaded_raw_json,
                                                op_args=['ch',
                                                         '{{ var.json.CH_SETTINGS.API_ID }}',
                                                         "{{ task_instance.xcom_pull(task_ids='extract_and_load_to_stg') }}",
                                                         '{{ var.json.CH_SETTINGS.STG_DB_TN }}'])

    load_json_to_ods = ClickHouseOperator(task_id='load_json_to_ods',
                                          clickhouse_conn_id='ch',
                                          sql=read_sql_query_from_file(
                                              os.path.join(os.path.dirname(__file__),
                                                           'sql',
                                                           'ch_insert_from_stg_to_ods.sql')
                                          ).format(
                                              dst='{{ var.json.CH_SETTINGS.ODS_DB_TN }}',
                                              src='{{ var.json.CH_SETTINGS.STG_DB_TN }}',
                                              uuid="{{ task_instance.xcom_pull(task_ids='extract_and_load_to_stg') }}",
                                              api_id='{{ var.json.CH_SETTINGS.API_ID }}'
                                          )
                                          )

    extract_and_load_to_stg >> validate_uploaded_raw_json >> load_json_to_ods
