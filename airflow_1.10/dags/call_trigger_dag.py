from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from comm.default_args import default_args

# default_args 에 키 값 변경 혹은 추가 변수 시
# 하단 dag instance 생성시 변수 값 조정

dag = DAG(
    dag_id='call_trigger_dag',
    default_args=default_args,
    start_date=datetime(2021, 11, 6, 0, 0, 0),
    schedule_interval="@once",
    catchup=False,
    tags=['trigger', 'mp'],
)

t1 = DummyOperator(task_id='start', dag=dag)


# TriggerDagRunOperator doc
# https://airflow.apache.org/docs/apache-airflow/1.10.13/_api/airflow/operators/dagrun_operator/index.html
# https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/trigger_dagrun/index.html#airflow.operators.trigger_dagrun.TriggerDagRunOperator

t2_1 = TriggerDagRunOperator(
    task_id='trigger_mp1',
    trigger_dag_id='run_trigger_mp1_dag',
    execution_date='{{ execution_date }}',
    wait_for_completion=False,
    # poke_interval=30,  # Poke interval to check dag run status when wait_for_completion=True.
    reset_dag_run=True,
    dag=dag
)

t2_2 = TriggerDagRunOperator(
    task_id='trigger_mp2',
    trigger_dag_id='run_trigger_mp2_dag',
    execution_date='{{ execution_date }}',
    wait_for_completion=False,
    # poke_interval=30,  # Poke interval to check dag run status when wait_for_completion=True.
    reset_dag_run=True,
    dag=dag
)

t3 = DummyOperator(task_id='end', dag=dag)

t1 >> [t2_1, t2_2] >> t3
