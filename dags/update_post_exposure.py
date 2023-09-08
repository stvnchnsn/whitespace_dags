from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash  import BashOperator


from datetime import datetime
from random import randint
import os

from lib.big_query_access import get_big_query


def get_post_ids():
    print(os.listdir())
    query = """
            SELECT name
            FROM reddit_db.raw_reddit_pulls"""
    df = get_big_query(query)
    print(df.head())
    return df
def get_exposure_information():
    pass



# with DAG("update_post_exposure", start_date = datetime(2021,1,1),
#                 schedule_interval="@daily", catchup=False) as dag:

#                 get_post_ids = PythonOperator(
#                     task_id = 'get_post_ids',
#                     python_callable = get_post_ids
#                 )
#                 get_post_ids

