from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import  BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from google_drive_downloader import GoogleDriveDownloader as gdd

from datetime import datetime
import docker
import os
    
def _branch():
    if os.path.exists('/usr/local/data/Answers.csv') and os.path.exists('/usr/local/data/Questions.csv'):
        return 'end'
    else:
        return 'clear'
    
def _download_answer():
    gdd.download_file_from_google_drive(file_id='1FflYh-YxXDvJ6NyE9GNhnMbB-M87az0Y',
                                    dest_path='/usr/local/data/Answers.csv')
def _download_question():
    gdd.download_file_from_google_drive(file_id='1pzhWKoKV3nHqmC7FLcr5SzF7qIChsy9B',
                                    dest_path='/usr/local/data/Questions.csv')
    
with DAG('etl_pipeline', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
    
    start = DummyOperator(task_id='start')

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=_branch
    )
    clear = BashOperator(
        task_id='clear',
        bash_command='rm -rf /usr/local/data/*'
    )

    download_answer = PythonOperator(
        task_id='download_answer',
        python_callable=_download_answer
    )
    download_question = PythonOperator(
        task_id='download_question',
        python_callable=_download_question
    )
    
    import_answer = BashOperator(
        task_id='import_answer',
        bash_command='mongoimport "mongodb://admin:password@mongo:27017" --db stackoverflow \
            --collection answers \
            --authenticationDatabase=admin \
            --type csv --headerline --file /usr/local/data/Answers.csv'
    )
    import_question = BashOperator(
        task_id='import_question',
        bash_command='mongoimport "mongodb://admin:password@mongo:27017" --db stackoverflow \
            --collection questions \
            --authenticationDatabase=admin \
            --type csv --headerline --file /usr/local/data/Questions.csv'
    )

    spark_submit = SparkSubmitOperator(
        task_id='spark_process',
        application = '/opt/airflow/dags/processor/number_of_answers.py',
        packages = 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1',
        conn_id = 'spark_master',
        verbose = True
    )

    import_output = BashOperator(
        task_id='import_output',
        bash_command='cat /usr/local/data/number_of_answers/*.csv | mongoimport "mongodb://admin:password@mongo:27017" --db stackoverflow \
            --collection number_of_answers \
            --authenticationDatabase=admin \
            --type csv --headerline ',
        trigger_rule='all_success'
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )

    start >> branch >> [clear, end]
    clear >> [download_answer, download_question]
    download_answer >> import_answer
    download_question >> import_question
    [import_answer, import_question] >> spark_submit  >> import_output >> end
    