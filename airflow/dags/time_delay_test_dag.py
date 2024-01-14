import logging
import datetime

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.sensors.time_delta import TimeDeltaSensor


logger = logging.getLogger(__file__)


def print_weekday(base_ymd):
    logger.info(f"execution_date=> {base_ymd}")
    day_of_week = base_ymd.weekday()
    # return day_of_week in (5, 6)


dag = DAG(
    dag_id='time_delay_test_dag',
    catchup=False,
    schedule_interval="@daily",
    start_date=datetime.datetime(2024, 1, 1),
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

task_1 = BashOperator(
    task_id='task_1',
    bash_command='echo task_1',
    dag=dag,
)

task_2 = BashOperator(
    task_id='task_2',
    bash_command='echo task_1',
    dag=dag,
)

# time_delay_1 = TimeDeltaSensor(task_id="time_delay_1",
#                                delta=pendulum.duration(seconds=10),
#                                dag=dag,
#                                )
#
#
# time_delay_2 = TimeDeltaSensor(task_id="time_delay_2",
#                                delta=pendulum.duration(seconds=20),
#                                dag=dag,
#                                )

# 각 분기에서 다르게 sleep 을 설정 하더라도
# 각 분기의 첫번째 task 가 모두 종료되어야 각 분기의 두번째 task 가 실행
# 즉 아래 의 경우 time_delay_2 가 종료된 후 task_1 과 task_2 가 실행됨
# start >> time_delay_1 >> task_1 >> end
# start >> time_delay_2 >> task_2 >> end
time_delay_1 = BashOperator(task_id="time_delay_1",
                            dag=dag,
                            bash_command="echo sleep_10s && sleep 10s")

time_delay_2 = BashOperator(task_id="time_delay_2",
                            dag=dag,
                            bash_command="echo sleep_20s && sleep 20s")

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

start >> time_delay_1 >> task_1 >> end
start >> time_delay_2 >> task_2 >> end
