# -*- coding: utf-8 -*-
import os
import sys
import warnings
from datetime import datetime, timedelta
from airflow import DAG, version
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator


from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator

### custom lib
from project import airflow_utils

env = Variable.get('environment')
IS_PRODUCTION = env == 'production'
aws_conn_id = 'aws-prod' if env == 'production' or env == 'staging' else 'aws_default'

def generate_dq_config(table):

    config = {
        'table_name': table,
        'is_production': str(IS_PRODUCTION),
        'env_folder_prefix': 'prod' if IS_PRODUCTION else 'stage',
        'current_dt': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
        'env': env
    }

    return config


""" 
    dag tasks 
"""


def add_analyzer_athena_partition(table, sqls, params):
    athena_result_config = {
        'OutputLocation': Variable.get('s3temp_uri_fmt') + '/athena/data_quality/' + table
    }

    analyzer_athena_partition = AWSAthenaOperator(
        task_id='add_analyze_partition',
        database='data_quality',
        query=sqls,
        output_location=athena_result_config['OutputLocation'],
        params=params,
        aws_conn_id=aws_conn_id,
        on_success_callback=None)

    return analyzer_athena_partition



DEFAULT_ARGS = {
    'owner': 'mars.su',
    'depends_on_past': False,
    'email': ['hueiyuansu@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'params': {'period_day_range': 30,
                'is_production': IS_PRODUCTION},
    'retries': 2
}


DEFAULT_ARGS['on_failure_callback'] = lambda context: airflow_utils.task_fail_slack_alert(
    context,
    http_conn_id="gogolook-ml-slack",
    post_channel="#airflow-alerts")

with DAG(dag_id='athena_dag',
        default_args=DEFAULT_ARGS,
        start_date=datetime(2021, 3, 31),
        schedule_interval= None) as dag_subdag:

    dq_config = generate_dq_config(
        table='table_name')

    start_task = DummyOperator(task_id='start_query')
    
    athena_table = dq_config['table_name'] if IS_PRODUCTION else dq_config['table_name']+'_staging'
    athena_partition = add_analyzer_athena_partition(
        table=athena_table,
        sqls='templates/add_partition.sql',
        params={
            'table_name': athena_table
        }
    )
    
    end_task = DummyOperator(task_id='end_query')

    start_task >> athena_partition >> end_task
