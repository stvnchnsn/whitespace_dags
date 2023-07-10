from datetime import datetime
import datetime as dt
from random import randint
import os

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash  import BashOperator

from lib.big_query_access import get_big_query, update_bq_from_df
from lib.reddit_api import Reddit_API

# replace SEARCH_TERMS dictionary with table
SEARCH_TERMS = {
    'alcoholic_beverages':
    ["alcohol","drinks","cocktails","beer","wine","liquor","spirits",
    "happyhour","mixology","bartender","craftbeer","winelover","drinkstagram",
    "whiskey","vodka","gin","rum","tequila","drinking","drinkresponsibly"]
}
# SEARCH_TERMS = {
#     'test':
#     ["tests"]
# }

def get_posts():
    reddit = Reddit_API()
    print(os.listdir())
    reddit.connect_reddit(pickle_name = './plugins/monkey')
    raw_reddit_posts = pd.DataFrame()
    for topic, search_terms in SEARCH_TERMS.items():
        for search_term in search_terms:
            print(f'searching {search_term}...')
            results_df = reddit.post_search(search_term)
            results_df['search_term'] = search_term
            results_df['topic'] = topic
            raw_reddit_posts = pd.concat([raw_reddit_posts, results_df])
            raw_reddit_posts = raw_reddit_posts[~raw_reddit_posts.duplicated()].reset_index(drop = True)
    raw_reddit_posts['created'] = pd.to_datetime(raw_reddit_posts['created_utc'], unit='s', utc=True)
    # Convert the UTC timestamps to local time
    local_timezone = 'America/Chicago'  # Replace with the desired timezone
    raw_reddit_posts['created'] = raw_reddit_posts['created'].dt.tz_convert(local_timezone)#.dt.strftime('%Y_%b_%d %H_%M')
    print(f'''
    raw_reddit_posts.shape: {raw_reddit_posts.shape}
    ''')
    print(raw_reddit_posts.head())
    return raw_reddit_posts

def get_post_ids():
    print(os.listdir())
    query = """
            SELECT name
            FROM reddit_db.raw_reddit_pulls"""
    df = get_big_query(query)
    print(df.head())
    return df

def update_db(ti):
    fresh_data = ti.xcom_pull(task_ids = "get_posts")
    old_data = ti.xcom_pull(task_ids = 'get_post_ids')
    update_data = fresh_data[~fresh_data.name.isin(old_data.name)]
    update_data['created'] = pd.to_datetime(update_data['created'] )
    print(f'''
    fresh_data.shape: {fresh_data.shape}
    old_data.shape: {old_data.shape}
    update_data.shape: {update_data.shape}
    ''')
    print(update_data.head())
    # update raw_reddit_pulls
    raw_reddit_cols = ['id','created','subreddit','subreddit_id','selftext','author_fullname','title',
                        'name','url']
    raw_reddit_pulls = update_data[raw_reddit_cols]
    table_name = 'reddit_db.raw_reddit_pulls'
    raw_reddit_pulls['created'] = pd.to_datetime(raw_reddit_pulls['created']).dt.strftime("%Y-%m-%d %H:%M:%S")

    values = str([tuple(value) for value in raw_reddit_pulls.values]).replace('[','').replace(']','')
    query = f"""
        INSERT INTO {table_name} {str(tuple(raw_reddit_pulls.columns)).replace("'","")}
        VALUES {values}
        """
    _ = get_big_query(query)

    # update post_exposure
    table_name = 'reddit_db.post_exposure'
    post_exposure_cols = ['id','num_comments','ups','upvote_ratio']
    post_exposures = fresh_data[post_exposure_cols]
    post_exposures['measurement_date'] = int(dt.datetime.now().strftime("%Y%m%d%H%M"))

    # TODO: query is too 'complex or long'. try breaking it up into chunks
    for i in range(len(post_exposures)):
        start_i = i*500
        if start_i > len(post_exposures)-1:
            break
        end_i = start_i+499
        print(start_i, end_i)
        temp_df = post_exposures.iloc[start_i:end_i]

        values = str([tuple(value) for value in temp_df.values]).replace('[','').replace(']','')
        query = f"""
            INSERT INTO {table_name} {str(tuple(temp_df.columns)).replace("'","")}
            VALUES {values}
            """
        _ = get_big_query(query)

    # update post_search_terms
    table_name= 'reddit_db.post_search_terms'
    post_search_terms=cols = ['id','search_term']
    post_search_terms = fresh_data[post_search_terms]
    for i in range(len(post_search_terms)):
        start_i = i*500
        if start_i > len(post_search_terms)-1:
            break
        end_i = start_i+499
        print(start_i, end_i)
        temp_df = post_search_terms.iloc[start_i:end_i]

        values = str([tuple(value) for value in temp_df.values]).replace('[','').replace(']','')
        query = f"""
            INSERT INTO {table_name} {str(tuple(temp_df.columns)).replace("'","")}
            VALUES {values}
            """
        _ = get_big_query(query)



    # update topic_search_terms
    table_name= 'reddit_db.topic_search_terms'
    topic_search_terms_cols = ['id','topic']
    topic_search_terms = fresh_data[topic_search_terms_cols]

    for i in range(len(topic_search_terms)):
        start_i = i*500
        if start_i > len(topic_search_terms)-1:
            break
        end_i = start_i+499
        print(start_i, end_i)
        temp_df = topic_search_terms.iloc[start_i:end_i]

        values = str([tuple(value) for value in temp_df.values]).replace('[','').replace(']','')
        query = f"""
            INSERT INTO {table_name} {str(tuple(temp_df.columns)).replace("'","")}
            VALUES {values}
            """
        _ = get_big_query(query)







with DAG("search_posts", start_date = datetime(2021,1,1),
                schedule_interval="@daily", catchup=False) as dag:

                get_posts = PythonOperator(
                    task_id = 'get_posts',
                    python_callable = get_posts
                )

                get_post_ids = PythonOperator(
                    task_id = 'get_post_ids',
                    python_callable = get_post_ids
                )

                update_db = PythonOperator(
                    task_id = 'update_db',
                    python_callable = update_db
                )
                get_posts  >> get_post_ids >> update_db

