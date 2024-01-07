import logging
import datetime as dt

import pytz


from airflow import DAG
from airflow.models import xcom
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from comm.default_args import default_args

# default_args 에 키 값 변경 혹은 추가 변수 시
# 하단 dag instance 생성시 변수 값 조정

dag = DAG(
    dag_id='trigger_dev_dag',
    default_args=default_args,
    start_date=dt.datetime(2023, 9, 22, 0, 0, 0),
    schedule_interval=None,
    # schedule_interval="0 0 * * 6,0",  # 토요일, 일요일 스캐쥴 실행
    catchup=False,
    tags=['trigger', 'fo', 'cd', 'cm'],
)

# tigger 입력 창에 다음과 같이 입력시 { "run_date": "20230925" }
# 해당 기준일로 실행 가능
today = "{{ dag_run.conf.run_date if dag_run.conf.run_date else ds_nodash}}"
kst_tz = pytz.timezone('Asia/Seoul')
now_dt = dt.datetime.now(tz=kst_tz)
if len(today) == 8:
    now_dt = dt.datetime.strptime(today, "%Y%m%d")
base_2days_ago_dt = now_dt - dt.timedelta(days=2)
base_2days_ago_dt = base_2days_ago_dt.strftime("%Y%m%d")


def is_working_date(base_ymd: str):
    logging.info("base_ymd: " + base_ymd)
    base_dt = dt.datetime.strptime(base_ymd, "%Y%m%d")
    weekday = base_dt.isoweekday()
    return 1 <= weekday <= 5


def is_monday(base_ymd: str):
    logging.info("base_ymd: " + base_ymd)
    base_dt = dt.datetime.strptime(base_ymd, "%Y%m%d")
    weekday = base_dt.isoweekday()
    return weekday == 1


def make_2days_ago(base_ymd: str, **kwargs):
    logging.info("base_ymd: " + base_ymd)
    base_dt = dt.datetime.strptime(base_ymd, "%Y%m%d")
    # base_2days_ago_dt = base_dt - dt.timedelta(days=2)
    # base_2days_ago_dt = base_2days_ago_dt.strftime("%Y%m%d")
    logging.info("base_2days_ago_dt: " + base_2days_ago_dt)
    kwargs['task_instance'].xcom_push(key='make_2days_ago', value=base_2days_ago_dt)
    kwargs['dag_run'].conf['base_2days_ago_dt'] = base_2days_ago_dt
    return 'make_2days_ago'


start = DummyOperator(task_id='start', dag=dag)

t1 = ShortCircuitOperator(
    task_id='1.is_monday',
    python_callable=is_monday,
    op_kwargs={'base_ymd': today},
    dag=dag
)

t2 = PythonOperator(
    task_id='2.make_2days_ago',
    python_callable=make_2days_ago,
    op_kwargs={'base_ymd': today},
    provide_context=True,
    dag=dag
)

t3_1 = TriggerDagRunOperator(
    task_id='3_1.trigger_fo_day02',
    trigger_dag_id='run_trigger_mp2_dag',
    execution_date=base_2days_ago_dt,   # exec_date (금) = today(월) - 3 day
    # execution_date='{{ dag_run.conf.base_2days_ago_dt }}',
    wait_for_completion=False,
    # poke_interval=30,  # Poke interval to check dag run status when wait_for_completion=True.
    reset_dag_run=True,
    provide_context=True,
    dag=dag
)


end = DummyOperator(task_id='end', dag=dag)

start >> t1 >> t2 >> t3_1 >> end
