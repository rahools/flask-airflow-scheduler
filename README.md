# Dynamic Airflow Scheduler Using Flask [WIP]

An airflow instalation that creastes and schedules dynamic DAG files based upon request send to flask.


# WIP Material
To add user to airflow
> airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin


{
"dag_id": "your-tag-id-1",
        "schedule_interval": "@daily",
        "namespace":"sub-directory-name-1",
        "template":"restDag.template"}