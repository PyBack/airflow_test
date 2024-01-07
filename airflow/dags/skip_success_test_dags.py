import datetime
import logging

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__file__)

dag = DAG(
    dag_id="skip_success_test_dag",
    start_date=datetime.datetime(2024, 1, 7),
    schedule_interval="@daily",
    catchup=False,
)

# tigger 입력 창에 다음과 같이 입력시 { "run_date": "20230925" }
# 해당 기준일로 실행 가능
today = "{{ dag_run.conf.run_date if dag_run.conf.run_date else ds_nodash}}"


def is_weekend(base_ymd):
    logger.info(f"execution_date=> {base_ymd}")
    day_of_week = base_ymd.weekday()
    return day_of_week in (5, 6)


start_task = EmptyOperator(task_id="start_task",
                           dag=dag)


skip_task = PythonOperator(
    task_id="skip_task",
    python_callable=is_weekend,
    op_kwargs={'base_ymd': today},
    # skip_on_failure=True,
    dag=dag,
)

success_task = PythonOperator(
    task_id="success_task",
    python_callable=lambda: None,
    dag=dag,
)

end_task = EmptyOperator(task_id="end_task",
                         dag=dag,
                         )

start_task >> [skip_task, success_task]
skip_task >> end_task
success_task >> end_task
