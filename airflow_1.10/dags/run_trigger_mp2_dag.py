from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from comm.default_args import default_args

# default_args 에 키 값 변경 혹은 추가 변수 시
# 하단 dag instance 생성시 변수 값 조정

dag = DAG(
    dag_id='run_trigger_mp2_dag',
    default_args=default_args,
    start_date=datetime(2021, 11, 6, 0, 0, 0),
    schedule_interval=None,
    catchup=False,
    tags=['trigger', 'mp'],
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

b1_1 = BashOperator(
    task_id='1_1_mp1_main',
    bash_command='echo MP1 main',
    dag=dag
)

b1_2 = BashOperator(
    task_id='1_2_mp1_bm_equal',
    bash_command='echo MP1 bm_equal',
    dag=dag
)

b1_3 = BashOperator(
    task_id='1_3_mp1_bm_mvo',
    bash_command='echo MP1 bm_mvo',
    dag=dag
)

b2_1_520 = BashOperator(task_id='2_1_520', bash_command='echo MP1 520', dag=dag)
b2_1_521 = BashOperator(task_id='2_1_521', bash_command='echo MP1 521', dag=dag)
b2_1_522 = BashOperator(task_id='2_1_522', bash_command='echo MP1 522', dag=dag)

b2_2_520 = BashOperator(task_id='2_2_520', bash_command='echo MP1 520', dag=dag)
b2_2_521 = BashOperator(task_id='2_2_521', bash_command='echo MP1 521', dag=dag)
b2_2_522 = BashOperator(task_id='2_2_522', bash_command='echo MP1 522', dag=dag)

b2_3_520 = BashOperator(task_id='2_3_520', bash_command='echo MP1 520', dag=dag)
b2_3_521 = BashOperator(task_id='2_3_521', bash_command='echo MP1 521', dag=dag)
b2_3_522 = BashOperator(task_id='2_3_522', bash_command='echo MP1 522', dag=dag)

main_end = DummyOperator(task_id='4_1_mp_main_end', dag=dag)
bm_equal_end = DummyOperator(task_id='4_2_mp_bm_equal_end', dag=dag)
bm_mvo_end = DummyOperator(task_id='4_3_mp_bm_mvo_end', dag=dag)


start >> b1_1 >> [b2_1_520, b2_1_521, b2_1_522] >> main_end
start >> b1_2 >> [b2_2_520, b2_2_521, b2_2_522] >> bm_equal_end
start >> b1_3 >> [b2_3_520, b2_3_521, b2_3_522] >> bm_mvo_end
