import os
import pandas as pd
from google.cloud import bigquery

directory = '/workspaces/bigquery-load/src/edfi-exports'

for filename in os.listdir(directory):

    full_filename = os.path.join(directory, filename)
    table = filename.replace('EdFi_Ods_edfi_', '').replace('.csv', '')
    df = pd.read_csv(full_filename)

    client = bigquery.Client()
    table_id = f'edfi.{table}'

    job_config = bigquery.LoadJobConfig(autodetect=True)

    print(f'Creating table {table}')

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    job.result()
