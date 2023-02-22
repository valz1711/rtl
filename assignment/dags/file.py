from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.mysql_operator import MySqlOperator
import pandas as pd
import requests
import json
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 15),
    'email': ['valz1711@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('file', schedule_interval="@once", default_args=default_args)
def get_data(**kwargs):
    url="https://sdw-wsrest.ecb.europa.eu/service/data/EXR/M.USD.EUR.SP00.A?format=jsondata"
    response = requests.get(url)
    if response.status_code ==200:
        data = response.json()
        data_str = json.dumps(data)
        return data_str
    return -1
def save_db(**kwargs):
    query = 'select * from json_url'
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='db')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

def check_table_exists(**kwargs):
    query = 'select count(*) from information_schema.tables where table_name="json_url"'
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='db')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return results
def store_data(**kwargs):
    res = get_data()
    table_status = check_table_exists()
    mysql_hook = MySqlHook(mysql_conn_id='mysql_conn', schema='db')
    if table_status[0][0] == 0: 
        create_sql = 'create table json_url(json_data JSON)'
        mysql_hook.run(create_sql)
    if res != -1:
        query = "INSERT INTO json_url (json_data) VALUES (%s)"
        mysql_hook.run(query, parameters=(res))
py_opt = PythonOperator(
    task_id='py_opt',
    dag=dag,
    python_callable=save_db,
)
store_opt = PythonOperator(
    task_id='store_opt',
    dag=dag,
    python_callable=store_data,
)
store_opt >> py_opt