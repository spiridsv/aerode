from airflow.models.baseoperator import BaseOperator
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseHook
import requests
from typing import Union
import logging
from uuid import uuid4
from datetime import datetime as dt


def _extract_json_from_api_url(api_url: str) -> str:
    req = requests.get(url=api_url)
    return req.text  # or return json natively


class AeroDEAPItoClickhouseOperator(BaseOperator):
    template_fields = ('connection_id', 'api_id', 'target_db_tn')

    insert_sql: str = """insert into {target_db_tn} values"""

    api_url: str = "https://statsapi.web.nhl.com/api/v1/teams/{api_id}/stats"

    def __init__(self,
                 connection_id: str,
                 target_db_tn: str,
                 api_id: int,
                 **kwargs) -> None:
        """
        Extract json from API (https://statsapi.web.nhl.com/api/v1/teams/21/stats)
        and load data to Clickhouse DB to target table
        :param connection_id: ch conn id name
        :param target_db_tn: dst STG table
        :param api_id: team id
        :param kwargs: opt
        """
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.api_id = api_id
        self.target_db_tn = target_db_tn
        self.uuid = str(uuid4())
        self.ch_hook = None

    def _load_raw_json_as_str_to_clickhouse(self, raw_json: str):
        if not raw_json:
            raise 'No data'
        return self.ch_hook.run(sql=self.insert_sql.format(target_db_tn=self.target_db_tn),
                                parameters=(
                                    (raw_json, self.api_id, self.uuid, dt.now()),
                                ))

    def execute(self, context):
        self.ch_hook = ClickHouseHook(self.connection_id)

        if not self.ch_hook:
            raise 'Cant init ClickHouseHook'

        self.api_url = self.api_url.format(api_id=self.api_id)

        logging.info(f"Extract JSON from {self.api_url}")

        raw_json = _extract_json_from_api_url(self.api_url)
        logging.info(f"Content len:{len(raw_json)}")

        r = self._load_raw_json_as_str_to_clickhouse(raw_json)
        logging.info(f"Inserted rows:{r}, UUID:{self.uuid}")

        return self.uuid
