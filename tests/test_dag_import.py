# simple unittest command
# $ python3.8 -m pytest tests/test_dag_import.py -v --disable-warnings

from airflow.models import DagBag


def test_no_import_errors():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
