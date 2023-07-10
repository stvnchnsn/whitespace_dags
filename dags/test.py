from lib.big_query_access import get_big_query
import os

def get_seed_df():
    query = """
            SELECT *
            FROM reddit_db.raw_reddit_pulls"""
    df = get_big_query(query)
    print(df.head())
if __name__ == '__main__':
    print(os.listdir())
    get_seed_df()