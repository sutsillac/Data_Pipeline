
import os
import wget
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from time import time
from sqlalchemy import create_engine
from airflow.decorators import task

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 9, 9),
#     'retries': 1
# }
dag = DAG(dag_id='data_ingestion',
          start_date=datetime(2023, 12, 9))

@task
def download_data():
    # print("DOWNLOAD DATA IS WORKING")

    final_df = []
    month = ['01','02','03','04','05','06','07','08','09','10','11','12']

    for data in month:
        result = wget.download(f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-{data}.parquet")
        df = pd.read_parquet(result)
        final_df.append(df)
        df_final = pd.concat(final_df)
        df_final.to_csv('first_three_months.csv', sep='|', index=False)
    print(df_final.head())

@task       
def local_to_postgres():
    
    engine = create_engine('postgresql://root:root@postgres_callistus:5432/ny_taxi')

    while True:

        df_iter = pd.read_csv('first_three_months.csv', sep='|', chunksize=100000)

        t_start = time()

        df = next(df_iter)

             
        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

    print(df)

with dag:
    DOWNLOAD_DATA = download_data()

    LOCAL_TO_POSTGRES = local_to_postgres()

DOWNLOAD_DATA >> LOCAL_TO_POSTGRES
