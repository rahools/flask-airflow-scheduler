# Dynamic Airflow Scheduler Using Flask [WIP]

An airflow installation that creates and schedules dynamic DAG files based upon request send to Flask.

## TODO

- convert seconds into cron expression

# Introduction

Do you need a wrapper-restful API to schedule a task in airflow? That is, a Flask API that listens to scheduling requests and dynamically creates airflow DAGs? Then this example is the solution for you. In this example, I have taken a database migration task(from mongo to Postgres) which is to be scheduled in batches. As simple as that, so let's get in on the action.

# Prerequisite

Since we require to migrate from mongo to postgresql, we would require both of these dbs up and running. For that I'm using docker by:
'''# docker run -it -v mongodata:/data/db -p 27017:27017 --name mongodb mongo'''
'''# docker run -it --name postgresql -p 5432:5432 bitnami/postgresql:latest'''

if you're using running the container for the second time, you could get by with:
'''# docker start -i mongodb'''
'''# docker start -i postgresql'''

Now you would have both the dbs running in the terminal. if you want, you add -d or remove -i to run these containers in the background. Now let's move on to configuring python and airflow. 

To install all the python dependencies I'm using pipenv. If you don't have it you can get it by:
'''pip install pipenv'''

after that install all the dependencies by running this command in the git directory:
'''pipenv install'''

Now you have to change various script variables to match the exact location of airflow directory. script/config that requires this change are:
- webStart.sh (1 instance)
- schedulerStart.sh (1 instance)
- resetAirflowDb.sh (1 instance)
- airflow_home/airflow.cfg (6 instance)

Since git's db file may have some data already on it, let's reset airflow db:
'''bash resetAirflowDb.sh'''

Finally, create an airflow user with username = admin and password = admin:
'''pipenv run airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin'''

# Running

With your docker dbs up and running, let's start up airflow and the flask API itself. For this you will need to execute these commands in three different terminals:
'''bash webStart.sh'''
'''bash schedulerStart.sh'''
'''bash flaskStart.sh'''

These bash scripts will take care of all the environment variables, commands, and start the applications.

Now you can send a scheduling request to the flask API and see the solution in action:
'''
curl --location --request POST 'http://localhost:5000/scheduler' \
--header 'Content-Type: application/json' \
--d '{
    "schedule_id": "001",
    "mongo_query": {"country": "USA"},
    "chunk_size": 10,
    "frequency": 3600
}'
'''