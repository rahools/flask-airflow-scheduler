from operators.templateToDagOperator import TemplateToDagOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
import os
from pathlib import Path


class RequestToDagOperator(TemplateToDagOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.script_dir = os.path.dirname(__file__)
        self.dynamic_dag_dir = os.environ.get('DYNAMIC_DAG_DIR', './airflow_home/dags/dynamicDags')

    def execute(self, context):
        conf = context["dag_run"].conf

        namespace = conf['namespace']
        dag_id = conf["dag_id"]
        template = conf['template']

        Path(os.path.join(self.dynamic_dag_dir, namespace)).mkdir(parents=True, exist_ok=True)

        self.template_file_path = os.path.join(self.script_dir, '..', '..','templates', template)

        self.destination_file_path = os.path.join(self.dynamic_dag_dir, namespace, f'{dag_id}.py')

        self.search_and_replace = {
            '#SCHEDULE_INTERVAL': conf["schedule_interval"],
            '#DAG_ID': dag_id,
            '#TASK_ID_1': 'run-docker'
        }
        TemplateToDagOperator.execute(self)