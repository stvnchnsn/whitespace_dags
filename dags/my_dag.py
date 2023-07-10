from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash  import BashOperator
from datetime import datetime
from random import randint

from lib.big_query_access import get_big_query

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator,
)


def get_seed_df():
    query = """
            SELECT *
            FROM reddit_db.raw_reddit_pulls"""
    df = get_big_query(query)
    print(df.head())


def _training_model():
    return randint(1, 10)

def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


with DAG("my_dag", start_date = datetime(2021,1,1),
                schedule_interval="@daily", catchup=False) as dag:

                get_seed_data = PythonOperator(
                    task_id = 'get_seed_df',
                    python_callable = get_seed_df
                )


                training_model_A = PythonOperator(
                    task_id = "training_model_A",
                    python_callable = _training_model
                )

                training_model_B = PythonOperator(
                    task_id = "training_model_B",
                    python_callable = _training_model
                )
                training_model_C = PythonOperator(
                    task_id = "training_model_C",
                    python_callable = _training_model
                )

                choose_best_model = BranchPythonOperator(
                    task_id = 'choose_best_model',
                    python_callable=_choose_best_model
                )

                accurate = BashOperator(
                    task_id = "accurate",
                    bash_command = "echo accurate"
                )
                inaccurate = BashOperator(
                    task_id = "inaccurate",
                    bash_command = "echo inaccurate"
                )
                #[training_model_A, training_model_B,training_model_C] >> choose_best_model >> [accurate, inaccurate]
                get_seed_df

