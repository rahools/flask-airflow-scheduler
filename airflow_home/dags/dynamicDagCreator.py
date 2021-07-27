from datetime import timedelta, datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from operators.requestToDagOperator import RequestToDagOperator

default_args = {
    # Only trigger this DAG based on external trigger
    'schedule_interval': None,
    'start_date': days_ago(2),
    'catchup': False,
    'owner': 'admin',
}

dag = DAG(
    'dynamic-dag-creator',
    default_args = default_args,
    description = 'DAG that helps creating dynamic DAGs',
    schedule_interval=None
)

t1 = RequestToDagOperator(task_id = 'generate-dag', dag = dag)

dag.doc_md = __doc__
t1.doc_md = 'Dynamically generated DAG. created by dynamic-dag-creator.'