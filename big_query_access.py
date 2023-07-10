from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file(
'./oceanic-variety-386313-5611c0e63164.json')

project_id = 'oceanic-variety-386313'
client = bigquery.Client(credentials= credentials,project=project_id)

query_job = client.query("""
   SELECT *
   FROM reddit_db.raw_reddit_pulls
   LIMIT 1000 """)

results = query_job.result()
print(results)