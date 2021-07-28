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
        schedule_id = conf["schedule_id"]
        template = conf['template']
        chunk_size = conf['chunk_size']
        mongo_query = conf['mongo_query']

        Path(os.path.join(self.dynamic_dag_dir, namespace)).mkdir(parents=True, exist_ok=True)

        self.template_file_path = os.path.join(self.script_dir, '..', '..','templates', template)

        self.destination_file_path = os.path.join(self.dynamic_dag_dir, namespace, f'{schedule_id}.py')

        self.search_and_replace = {
            '#schedule_interval': conf["schedule_interval"],
            '#schedule_id': schedule_id,
            '#chunk_size': chunk_size,
            '#mongo_query': mongo_query,
        }
        TemplateToDagOperator.execute(self)