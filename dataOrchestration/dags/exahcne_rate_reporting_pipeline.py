import json
import os
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models.xcom import XCom
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from google.cloud import bigquery
from datetime import datetime

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "auth.json"
os.environ["AIRFLOW_CONN_FRANKFURTER_API"] = "http://https://api.frankfurter.app/"
PROJECT_ID = json.load(open("auth.json", "rb"))["quota_project_id"]


@dag(
    schedule="* * * * 1-5",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"],
    template_searchpath=["/usercode/sql"],
    user_defined_macros={"from_cur": "EUR", "to_cur": "USD"},
)
def exchange_rate_ingestion_pipeline():
    fetcher = SimpleHttpOperator(
        task_id="fetch_exchange_rate_from_frankfurter",
        method="GET",
        http_conn_id="frankfurter_api",  # Airflow connection_id of Frankfurter API
        endpoint="{{ data_interval_end | ds }}?&from={{ from_cur }}&to={{ to_cur }}",
    )

    @task(
        templates_dict={
            "from_cur": "{{ from_cur }}",
            "to_cur": "{{ to_cur }}",
            "date": "{{ data_interval_end | ds }}",
        }
    )
    def load_api_res_to_temp_table(**kwargs):
        ti = kwargs["task_instance"]
        from_cur = kwargs["templates_dict"]["from_cur"]
        to_cur = kwargs["templates_dict"]["to_cur"]
        date = kwargs["templates_dict"]["date"]
        api_res = json.loads(
            ti.xcom_pull(task_ids="fetch_exchange_rate_from_frankfurter")
        )
        client = BigQueryHook().get_client(project_id=PROJECT_ID)
        job = client.load_table_from_dataframe(
            dataframe=pd.DataFrame(
                {
                    "timestamp": [pd.Timestamp.now().round("min")],
                    "date": [datetime.strptime(date, "%Y-%m-%d")],
                    "from_cur": [from_cur],
                    "to_cur": [to_cur],
                    "rate": [api_res["rates"][to_cur]],
                }
            ),
            destination="airflow_challenge.exchange_rate_staging",
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE"
            ),
        )
        job.result()

    merger = BigQueryInsertJobOperator(
        task_id="merge_temp_table_into_dest",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'exchange_rate.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    fetcher >> load_api_res_to_temp_table() >> merger


exchange_rate_ingestion_pipeline()