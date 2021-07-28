from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pymongo
import json
import psycopg2

default_args = {
    'start_date': days_ago(2),
    'catchup': False,
    'owner': 'admin',
}

dag = DAG(
    'mongoPostgres-001',
    default_args = default_args,
    schedule_interval='@daily'
)

def MongoToPostgres(**context):
    # mongo connection
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["flask-airflow-scheduler"]
    mycol = mydb["sample-data"]

    # postgres connection
    postgresConn = psycopg2.connect(database = "flask-airflow-scheduler", user = "postgres", password = "", host = "127.0.0.1", port = "5432")
    postgresCur = postgresConn.cursor()

    # get number of records already migrated
    try:
        numberOfMigratedRecords = Variable.get("mongoPostgres-001")
    except:
        numberOfMigratedRecords = 0
        Variable.set("mongoPostgres-001", numberOfMigratedRecords)
    numberOfMigratedRecords = int(numberOfMigratedRecords)
        

    # get data from mongo
    myquery = {'country': 'USA'} #json.loads({'country': 'USA'})
    mongoData = mycol.find(myquery).sort("timestamp", pymongo.ASCENDING).skip(numberOfMigratedRecords).limit(10)

    # columns in postgres table
    postgresColumns = [
        'timestamp', 
        'ver', 
        'product_family', 
        'country', 
        'device_type', 
        'os', 
        'checkout_failure_count', 
        'payment_api_failure_count', 
        'purchase_count', 
        'revenue'
    ]

    # postgres query starter
    postgresQuery = '''
        INSERT INTO sample_data (
            timestamp, 
            ver, 
            product_family, 
            country, 
            device_type, 
            os, 
            checkout_failure_count, 
            payment_api_failure_count, 
            purchase_count, 
            revenue
        ) VALUES 
    '''

    # construct and execute postgres insert query
    for dataDict in mongoData:
        tempQuery = '('
        for col in postgresColumns:
            tempQuery += f"'{dataDict[col]}', "
        tempQuery = tempQuery[:-2] + ')'
        postgresCur.execute(postgresQuery + tempQuery)

    # commit and close postgres
    postgresConn.commit()
    postgresConn.close()

    # increase migrated record count
    numberOfMigratedRecords += 10
    numberOfMigratedRecords = Variable.set("mongoPostgres-001", numberOfMigratedRecords)

t1 = PythonOperator(task_id = f"mongoPostgres-001", python_callable = MongoToPostgres, dag = dag, provide_context = True)