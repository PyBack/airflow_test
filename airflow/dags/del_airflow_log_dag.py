# 30일 지난 Airflow 로그를 삭제함
#
# 사용 명령어 : find [airflow log dir] -type f -mtime +30 -delete
#     : ex. find /opt/airflow/logs -type f -mtime +30 -delete
#     : 위에 명령어를 BashOperator로 실행
#
# 옵션 설명 :
#     : -type f : 일반 파일만 검색
#         * 추가 설명 :  d: 디렉토리, l: 링크 파일, b: 블록디바이스, c: 캐릭터 디바이스 ...
#     : -mtime +30 : 생성한지 720시간 이상 지난 파일만 검색
#     : -delete : 삭제

import pendulum
import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator  # Import BashOperator

"""Set DAG"""
local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'dev-user',
    'email': 'your@email.com',
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=10)
}

dag = DAG(
    dag_id='del_airflow_log',
    default_args=default_args,
    start_date=dt.datetime(2021, 9, 17, tzinfo=local_tz),
    catchup=False,                  # dag 트리거 시 현재 시각과 start_date 사이에 미실행된 스캐쥴 실행 안함
    schedule_interval='* 2 * * *',  # Crontime : min hour day month week / 매일 02시에 삭제
    max_active_runs=3,
    tags=['operation']
)

"""Task"""

start = BashOperator(
        task_id='start',
        bash_command='echo "start!"',
        dag=dag,
)

del_log = BashOperator(
    task_id='delete_log',
    bash_command='find /Users/assa/Documents/python/airflow_test/airflow/logs -type f -mtime +30 -delete',
    dag=dag
)

start >> del_log
