
# BROC95
from plugins.sqlCommandB import csvFile, identExt
from plugins.connectionDag import configDag, configLog
from plugins.dataTrasB import data_transform
from datetime import datetime, timedelta
from plugins.sqlCommandB import sqlCommand, createPath
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from sqlalchemy import create_engine
import logging.config
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import boto3
from botocore.exceptions import ClientError

name_data = 'GBUNComahue'


dag_name = '_dag_elt'
selec = "_select.csv"
sql_ = ".sql"
process = "_process.txt"

dag_ = name_data+dag_name
query_name = name_data+sql_
select_name = name_data+selec
process_name = name_data+process


default_args, POSTGRES_CONN_ID, ACCESS_KEY, SECRET_ACCESS_KEY, AWS_S3_CONN_ID, BUCKET = configDag()


#  Extract data with  hook,pandas .csv

def extract():

    logger = configLog(dag_)

    logger.info(dag_)
    logger.info("Extract")
    logger.info("Connect: % s", POSTGRES_CONN_ID)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    query = sqlCommand(
        file=query_name, point='include')
    conn = hook.get_conn()
    # logging.info(conn)

    df = hook.get_pandas_df(sql=query)

    logger.info(df.head())
    pathCsv = createPath('files')
    # pathCsv = createPath('include')  # Correccion a guardar localmente

    js = df.to_json(orient='columns')
    # print("Create csv")
    df.to_csv(pathCsv+'/'+select_name)
    conn.close()


#  Transform data with pandas

def transform():
    logger = configLog(dag_)
    logger.info("Transform")

    pathfile = createPath('files')

    fileSelect = csvFile(pathfile, select_name)

    data_transform(fileSelect)

    # print(POSTGRES_CONN_ID)


#  Load data with S3 amazon .txt

def load():
    logger = configLog(dag_)
    logger.info("Load: %s", dag_)
    logger.info("Connect: % s", AWS_S3_CONN_ID)

    # def upload_file(file_name, bucket, object_name):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    dest_file_path = createPath('datasets')
    file_name = dest_file_path+'/' + process_name
    bucket = BUCKET
    object_name = process_name

    session = boto3.Session(
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS_KEY,
    )
    # client = session.client("s3")
    s3_client = session.client('s3')
    try:
        # response = s3_client.upload_file(file_name, bucket, object_name)
        response = s3_client.upload_file(Filename=file_name,Bucket= bucket['Name'],Key= object_name)
    except ClientError as e:
        logger.error(e)
        return False
    return True


with DAG(dag_id=dag_, start_date=datetime(2022, 11, 4), schedule_interval=timedelta(hours=1), default_args=default_args, catchup=False) as dag:
    #  Extract
    task1 = PythonOperator(task_id="TaskExtract",
                           python_callable=extract)
    #  Transform
    task2 = PythonOperator(task_id="TaskTransform",
                           python_callable=transform)
    #  Load
    task3 = PythonOperator(task_id="TaskLoad",
                           python_callable=load)

task1 >> task2 >> task3
# task1
