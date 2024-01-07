import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.weekday import WeekDay
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__file__)


def print_weekday(base_ymd):
    logger.info(f"execution_date=> {base_ymd}")
    day_of_week = base_ymd.weekday()
    # return day_of_week in (5, 6)


dag = DAG(
    dag_id='branch_weekday_test_dag',
    catchup=False,
    schedule_interval="@daily",
    start_date=datetime.datetime(2024, 1, 1),
)

# tigger 입력 창에 다음과 같이 입력시 { "run_date": "20230925" }
# 해당 기준일로 실행 가능
today = "{{ dag_run.conf.run_date if dag_run.conf.run_date else ds_nodash}}"

branch_weekday = BranchDayOfWeekOperator(
    task_id="branch_weekday",
    follow_task_ids_if_true="task_empty_weekend",
    follow_task_ids_if_false="task_print_weekday",
    week_day={WeekDay.SATURDAY, WeekDay.SUNDAY},
    dag=dag
)

task_print_weekday = PythonOperator(
    task_id='task_print_weekday',
    python_callable=print_weekday,
    op_kwargs={'base_ymd': today},
    dag=dag
)

task_empty_weekend = EmptyOperator(
    task_id='task_empty_weekend',
    dag=dag,
)

end = EmptyOperator(
    task_id='end',
    dag=dag,
)

branch_weekday >> [task_print_weekday, task_empty_weekend]
task_print_weekday >> end
task_empty_weekend >> end
