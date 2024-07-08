import os
import json
import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor


def get_execution_date_fn(logical_date, **kwargs):
    return kwargs["data_interval_end"]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "auth.json"
os.environ["AIRFLOW_CONN_FRANKFURTER_API"] = "http://https://api.frankfurter.app/"
PROJECT_ID = json.load(open("auth.json","rb"))["quota_project_id"]

@dag(
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["educative"],
    template_searchpath=["/usercode/sql"],
)
def exchange_rate_reporting_pipeline():
    sensor = ExternalTaskSensor(
        task_id="sensor_exchange_rate_ingestion_pipeline",
        external_dag_id="exchange_rate_ingestion_pipeline",
        external_task_id="merge_temp_table_into_dest",
        execution_date_fn=get_execution_date_fn,
        timeout=300,
    )

    calculate_avg_rates = BigQueryInsertJobOperator(
        task_id="calculate_avg_rates",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'exchange_rate_report.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    sensor >> calculate_avg_rates


exchange_rate_reporting_pipeline()