import logging
import datetime as dt

import pytz

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator

logger = logging.getLogger(__file__)

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 1, 1)
}

cron_express = '5 23,3,7 * * *'
dag = DAG(
    dag_id='multi_hour_schedule_test',
    default_args=default_args,
    schedule=cron_express,
    catchup=False)

# trigger 입력 창에 { "run_date": "20230925" } 과 같이  입력시 해당 기준일로 실행 가능
today = "{{ dag_run.conf.run_date if dag_run.conf.run_date else ds_nodash }}"
# trigger DAG w/config 입력창에 { "execuation_date": "2024-01-27T07:05:00" } 과 같이 입력시
# Data interval end (data_interval_end) 값이 해당 입력값으로 실행 됨
# execuation_date template variable 은 depricated 예정으로 logical_date 로 변경 필요
target_datetime = "{{ data_interval_end | ts_nodash}}"

templated_command = """
    echo "ds_nodash : {{ ds_nodash }}"
    echo "data_interval_start: {{ data_interval_start }}"
    echo "data_interval_end: {{ data_interval_end }}"
    echo "data_interval_end_ts_noash: {{ data_interval_end | ts_nodash}}"
"""


def templated_test(d1):
    logger.info("{{ ds }}")
    logger.info(f"d1 test: {d1}")
    input_dt = dt.datetime.strptime(d1, "%Y%m%dT%H%M%S")
    input_dt_1dayago = input_dt - dt.timedelta(days=1)
    logger.info(f"input_dt: {input_dt}")
    logger.info(f"input_dt_1dayago: {input_dt_1dayago}")
    kst_zone = pytz.timezone('Asia/Seoul')
    input_dt_kst = input_dt + dt.timedelta(hours=9)
    logger.info(f"input_dt_kst: {input_dt_kst}")


t1 = BashOperator(
    task_id='bash_templated',
    bash_command=templated_command,
    dag=dag,
)

t2 = PythonOperator(
    task_id='python_task',
    python_callable=templated_test,
    op_args=[target_datetime],
    dag=dag,
)

t1 >> t2

