import datetime

import pendulum

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.time_delta_sensor import TimeDeltaSensor

from comm.default_args import default_args

# default_args 에 키 값 변경 혹은 추가 변수 시
# 하단 dag instance 생성시 변수 값 조정

dag = DAG(
    dag_id='timedelta_test_dag',
    default_args=default_args,
    start_date=datetime.datetime(2021, 11, 6, 0, 0, 0),
    schedule_interval="@daily",
    catchup=False,
    tags=['timedelta', 'cm'],
)


# wait_pendulum = TimeDeltaSensor(task_id="wait_some_seconds", delta=pendulum.duration(seconds=10))

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

for cm_batch_index in range(3):
    task_run_cm_batch = BashOperator(
            task_id=f"cm_batch_{cm_batch_index+1}",
            bash_command=f"echo run cm_batch_{cm_batch_index+1}",
            dag=dag
    )

    # airflow 의존성 으로 인해 pendulum.duration 생성 안됨
    # wait = TimeDeltaSensor(task_id=f"wait_cm_batch_{cm_batch_index}", delta=pendulum.duration(seconds=60*cm_batch_index))

    if cm_batch_index == 0:
        wait = DummyOperator(task_id=f"wait_{cm_batch_index}_seconds_cm_batch", dag=dag)
        start >> wait >> task_run_cm_batch >> end
    elif cm_batch_index >= 0:
        # wait = BashOperator(
        #     task_id=f"wait_{cm_batch_index}_minute_cm_batch",
        #     bash_command=f"sleep 60*{cm_batch_index}",
        #     dag=dag
        # )
        wait = TimeDeltaSensor(task_id=f"wait_{cm_batch_index}_seconds_cm_batch",
                               delta=datetime.timedelta(seconds=1 * cm_batch_index),
                               mode='reschedule'
                               )

        start >> wait >> task_run_cm_batch >> end
