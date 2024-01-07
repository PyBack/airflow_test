import logging
import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

logger = logging.getLogger(__file__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 1, 1)
}

dag = DAG(
    'branch_skip_work_test_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False)

# tigger 입력 창에 다음과 같이 입력시 { "run_date": "20230925" }
# 해당 기준일로 실행 가능
today = "{{ dag_run.conf.run_date if dag_run.conf.run_date else ds_nodash}}"

# these two are only here to protect tasks from getting skipped as direct dependencies of branch operator
to_weekday_do_work = EmptyOperator(dag=dag, task_id='to_weekday_do_work')
to_weekend_do_work = EmptyOperator(dag=dag, task_id='to_weekend_do_work')


def is_weekday(base_ymd):
    logger.info(f"execution_date=> {base_ymd}")
    base_ymd = datetime.datetime.strptime(base_ymd, "%Y%m%d")
    day_of_week = base_ymd.weekday()
    # return day_of_week not in (5, 6)
    if day_of_week not in (5, 6):
        return to_weekday_do_work.task_id
    else:
        return to_weekend_do_work.task_id


start = EmptyOperator(dag=dag, task_id='start')
task_something = EmptyOperator(dag=dag, task_id='task_something')

work = EmptyOperator(dag=dag, task_id='work')
branch_chk_weekday = BranchPythonOperator(dag=dag,
                                          task_id='branch_chk_weekday',
                                          python_callable=is_weekday,
                                          op_kwargs={'base_ymd': today},
                                          )
final = EmptyOperator(dag=dag, task_id="final", trigger_rule="none_failed")

start >> branch_chk_weekday >> to_weekday_do_work >> work >> final
branch_chk_weekday >> to_weekend_do_work >> final
start >> task_something >> final
