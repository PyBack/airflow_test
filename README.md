# airflow 설치 및 환경설정 (로컬환경)


## 1) 가상환경 생성 및 실행
### pyenv, virtualenv 를 통해 가상환경 생성하는 방법
```sh
# python version, 가상환경 이름은 원하는대로 생성
$ pyenv virtualenv {python-version} {가상환경이름}

# 예시
$ pyenv virtualenv 3.8.6 venv-test-airflow

# 가상환경 실행
$ pyenv activate venv-test-airflow
```

### python3 를 통해 가상환경 생성하는 방법
```sh
# 가상환경 생성
$ python3 -m virtualenv venv

# 가상환경 실행
$ source venv/bin/activate
```


## 2) Airflow 설치
참고: https://airflow.readthedocs.io/en/1.10.14/installation.html  
가상환경을 실행했다면 Airflow 를 설치
```sh
$ pip install -r apache-airflow-requirements.txt
```

## 3) Airflow 환경변수 설정
airflow root 디렉토리 설정은 아래와 같이 가능  
별도 python 경로가 필요하면 PYTHONPATH 환경변수에 경로지정을 해줘야함
```sh
$ export AIRFLOW_HOME=~/Documents/python/airflow_test/airflow
```

## 3-1) DB 초기화
Airflow는 기본으로 sqllite를 사용한다.  
아래 명령어를 실행하면 자신의 Home Directory에 airflow 폴더 (~/airflow) 가 생성되는 것을 볼 수 있다.
```sh
$ airflow db init
```

## 4) 유저 계정 생성

Airflow Webserver 를 사용하기 위해서는 계정이 필요하다.
```sh
# 계정 생성 명령어 (\ 앞에 띄어쓰기 있어야함)
$ airflow users create \ 
> --username {Login_ID} \
> --firstname {First_NAME} \ 
> --lastname {Last_NAME} \
> --role Admin \              # 해당 부분은 고정
> --password {Password} \     # 따로 명령어 입력을 안하면 입력 프롬프트 나옴
> --email {Email}
```

```sh
# 한줄 명령어
$ airflow users create --username {Login_ID} --firstname {First_NAME} --lastname {Last_NAME} --role Admin --password {Password} --email {Email}
```

## 5) example dags unload
airflow 기본 예제가 dags list 에 안보이도록 하기 위해 아래와 같이 설정을 변경하면 된다.
```sh
$ cd ~/airflow 혹은 airflow_home 위치로 이동
$ ls airflow.cfg 
```
airflow.cfg 파일 에서 load_examples=True -> False 로 변경

## 6) Webserver 실행
```sh
# 기본 port = 8080
$ airflow webserver --port 8080

# 이 명령어로 해도 됨.
$ airflow webserver 
```

## 7) Scheduler 실행

webserver를 띄웠다면 해당 터미널을 그대로 남겨두고, 새로운 터미널에서 scheduler를 실행시켜보자.
```sh
$ airflow scheduler
```

# airflow dag 파일 실행 테스트

## 1) airflow dag 파일 로딩 테스트
새로 만든 dag 파일이 컴파일이 잘되지 확인하기 위해서 우선 dag 파일을 ~/airflow/dags/ 으로 복사    
복사 후 dags list 업 하면 airflow 에 등록 하기 위해 기본 컴파일 체크를 한다.  
```sh
$ cp ./my_dags.dags ~/airflow/dags/
$ airflow dags list                   # List all the DAGs (airflow 1.10 버전)
$ airflow dags list-import-errors     # List all the DAGs that have import errors (airflow 2.0 이상 버전)

```
## 2) airflow dag 파일 그래프 뷰 이미지 생성

```sh
# --imgcat 은 iterm 에서 만 가능
# -s output.png 옵션을 활용하여 이미지 파일 저장 후 확인
# 해당 명령을 수행하기 위해서 graphviz/bin/dot or local/bin/dot 실행파일 필요
# 다운로드 링크: https://graphviz.org/download/
$ airflow dags show [-h] [--imgcat] [-s SAVE] [-S SUBDIR] [-v] dag_id
```


## 3) airflow dag 파일 테스트

```sh
# 2.5.1 버전 
# Execute one single DagRun for a given DAG and execution date.
$ airflow dags test [-h] [-c CONF] [--imgcat-dagrun] [--save-dagrun SAVE_DAGRUN]
                  [--show-dagrun] [-S SUBDIR] [-v]
                  dag_id [execution_date]
# 1.10 버전 
# Test a task instance. This will run a task without checking for dependencies or recording its state in the database.
$ airflow test [-h] [-sd SUBDIR] [-dr] [-tp TASK_PARAMS] [-pm]  dag_id task_id execution_date
```

참고: https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html   
https://airflow.apache.org/docs/apache-airflow/1.10.13/cli-ref.html   
