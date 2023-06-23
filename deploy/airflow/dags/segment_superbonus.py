from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonVirtualenvOperator

default_args = {
    'start_date': datetime(2023, 6, 1),
    'retries': 3,
}

with DAG(dag_id='result_dag', default_args=default_args, schedule_interval='0 0/12 * * *') as dag:

    start = DummyOperator(task_id='start_load_data')

    @task.virtualenv(
        task_id="result_task_load_date",
        requirements=["requests, hydra-core == 1.1.0, pandas, psycopg2, openpyxl,sqlalchemy"],
        system_site_packages=False,
    )
    def resutl_task_load_to_date_posgres():
        import json
        from time import sleep

        import pandas as pd
        import psycopg2
        import requests
        from colorama import Back, Fore, Style
        from sqlalchemy import create_engine

        response_API = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=100')
        first_data = response_API.text
        data = json.loads(first_data)
        df = pd.json_normalize(data)
        df = df.reset_index(drop=True)

        setting_posgres = Variable.get('setting_posgres')  # храним в Varibale в Airflow
        # формат: {'host': 'test_host', 'database':'test_database'}
        conn = psycopg2.connect(
            host=setting_posgres['host'],
            database=setting_posgres['database'],
            user=Variable.get('tesh_user_posgres'),
            password=Variable.get('tesh_password_posgres'),
        )

        engine = create_engine(
            f'postgresql://{Variable.get("tesh_user_posgres")}:{Variable.get("tesh_password_posgres")}@{setting_posgres["host"]}/{setting_posgres["database"]}'
        )

        try:
            df.to_sql('result_table', engine, if_exists='append', index=False)
        except:
            print('Table not found')
            cur = conn.cursor()
            command = ''''
                        CREATE TABLE result_table (
                            id int ,
                            uid VARCHAR(255) NOT NULL,
                            strain VARCHAR(255),
                            cannabinoid_abbreviation VARCHAR(255),
                            cannabinoid VARCHAR(255),
                            terpene VARCHAR(255),
                            medical_use VARCHAR(255),
                            health_benefit VARCHAR(255),
                            category VARCHAR(255),
                            type VARCHAR(255),
                            buzzword VARCHAR(255),
                            brand VARCHAR(255)
                        )

                '''
            df.to_sql('result_table', engine, if_exists='append', index=False)

        print('Finish work')

    resutl_task_load_to_date_posgres_task = resutl_task_load_to_date_posgres()
    end = DummyOperator(task_id='end_load_data')

    start >> resutl_task_load_to_date_posgres_task >> end
