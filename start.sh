export AIRFLOW_HOME=/home/rahools/codes-p/flask-airflow-scheduler/airflow_home
pipenv run airflow webserver &
pipenv run airflow scheduler &
sudo docker start mongodb
sudo docker start postgresql