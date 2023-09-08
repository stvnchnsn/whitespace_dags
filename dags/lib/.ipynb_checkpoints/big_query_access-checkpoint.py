from google.cloud import bigquery
from google.oauth2 import service_account

import os

from lib.bucket_access import get_blob

CREDIENTIALS_PATH = './plugins/oceanic-variety-386313-5611c0e63164.json'
PROJECT_ID = 'oceanic-variety-386313'

CONFIG = {
    'bucket_name':"us-central1-reddit-scrappin-5a19086f-bucket",
    'bg_api_fp':"dags/search_posts/plugins/oceanic-variety-386313-5611c0e63164.json"

}

def get_big_query(query, credientials_path = './plugins/oceanic-variety-386313-5611c0e63164.json', project_id = 'oceanic-variety-386313'):
    
    to_fp = './oceanic-variety-386313-5611c0e63164.json'
    get_blob(CONFIG['bucket_name'], CONFIG['bg_api_fp'], to_fp)
    credentials = service_account.Credentials.from_service_account_file(to_fp)
    client = bigquery.Client(credentials= credentials,project=project_id)
    print(f'executing BigQuery: {query}')
    query_job = client.query(query)
    results = query_job.result()
    return results.to_dataframe()
def update_bq_from_df(dataframe, table_name,credientials_path = './plugins/oceanic-variety-386313-5611c0e63164.json', project_id = 'oceanic-variety-386313'):
    ''''
    
    '''
    to_fp = './oceanic-variety-386313-5611c0e63164.json'
    get_blob(CONFIG['bucket_name'], CONFIG['bg_api_fp'], to_fp)
    credentials = service_account.Credentials.from_service_account_file(to_fp)
    client = bigquery.Client(credentials= credentials, project=project_id)
    
    # Get column names and values from the DataFrame
    columns = ", ".join(dataframe.columns)
    values = ", ".join([f"'{str(value)}'" for value in dataframe.values])

    # Generate the SQL statement
    #sql_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    sql_statement = f"INSERT INTO {table_name} "
    print(sql_statement)
    client.query(sql_statement)

if __name__ == '__main__':
    import pandas as pd
    import os
    credientials_path = './oceanic-variety-386313-5611c0e63164.json'

    query = """
            SELECT *
            FROM reddit_db.raw_reddit_pulls"""
    df_ids = get_big_query(query,credientials_path = credientials_path).head(10)

    for i, row in df_ids.iterrows():
        query = f"""
        INSERT INTO table.name {str(tuple(df_ids.columns)).replace("'","")}
        VALUES {tuple(row.values)}
        """
        print(query)
        if i ==2: break

    #print(query)
    #df = get_big_query(query,credientials_path = credientials_path)

    # print(df.shape)
    # print(df.head())

    # # Create a test DataFrame
    # data = {'Name': ['John', 'Jane', 'Mike'],
    #         'Age': [25, 30, 35],
    #         'City': ['New York', 'London', 'Paris']}
    # dataframe = pd.DataFrame(data)

    # def generate_update_statement(dataframe, table_name):
    #     # Get column names and values from the DataFrame
    #     columns = ", ".join(dataframe.columns)
    #     values = ", ".join([f"'{str(value)}'" for value in dataframe.values])

    #     # Generate the SQL statement
    #     sql_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
    #     return sql_statement
        
    # table_name = 'my_table'

    # print(generate_update_statement(dataframe, table_name))
