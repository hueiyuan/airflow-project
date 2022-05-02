from datetime import timedelta, datetime
from airflow import DAG, version
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from project import airflow_utils

IS_PRODUCTION = Variable.get('environment') == 'production'
ARGS_PARAMS = {'is_production': IS_PRODUCTION}

env = Variable.get('environment')

aws_conn_id = 'production-aws' if env == 'production' or env == 'staging' else 'aws_default'
subdag_pool = 'subdag-pool'

DEFAULT_ARGS = {
    'owner': 'bruce.lin',
    'depends_on_past': False,
    'email': ['hueiyuansu@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'params': ARGS_PARAMS,
    'retries': 2
}

DEFAULT_ARGS['on_failure_callback'] = lambda context: airflow_utils.task_fail_slack_alert(
    context,
    http_conn_id='slack-conn-id',
    post_channel='#airflow-alerts')
    
with DAG(
    dag_id='emr_workflow_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    start_date=datetime(2021, 3, 31),
    schedule_interval='0 15 * * *',
) as dag:

    # [START howto_operator_emr_manual_steps_tasks]
    create_cluster = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=aws_conn_id,
        emr_conn_id='emr_default',
        params={
            'env': env
        }
    )

    add_step = EmrAddStepsOperator(
        task_id='add_convert_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id=aws_conn_id,
        steps=SPARK_CONVERT_STEPS,
        params={
            'env': env
        }
    )

    step_check = EmrStepSensor(
        task_id='watch_convert_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_convert_steps', key='return_value')[0] }}",
        aws_conn_id=aws_conn_id,
    )

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id=aws_conn_id,
    )

    
    create_cluster >> add_step >> step_check >> terminate_cluster
    
