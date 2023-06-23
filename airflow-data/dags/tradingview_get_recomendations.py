from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
import FinanceLib as fl
dag = DAG(
    dag_id="Tradingview get recomendations",
    schedule_interval="@hourly",
    max_active_runs=1,
)

get_data = PythonOperator(
    task_id="get_data",
    python_callable=fl.daily_update_quote,
    op_kwargs={
    },
    dag=dag,
)