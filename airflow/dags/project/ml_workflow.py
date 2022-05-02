#!/usr/bin/env python3
import os
import warnings
import boto3
from datetime import datetime, timedelta

import pytz

# airfow - common
from airflow import DAG
from airflow.models import Variable
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator

# airfow - aws
from airflow.contrib.operators.emr_add_steps_operator import \
    EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import \
    EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import \
    EmrTerminateJobFlowOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook

# airfow - gcp
from airflow.contrib.operators.gcp_transfer_operator import S3ToGoogleCloudStorageTransferOperator
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.contrib.operators.gcs_to_s3 import GoogleCloudStorageToS3Operator
from airflow.providers.google.cloud.operators.mlengine import MLEngineStartTrainingJobOperator

# customize
from project.subdags.sms_airflow_data_augment import subdag_data_augment
from project.lib.common import loadconfig
from project.lib.custom_sms.aws_emr_spec import EMRSpec
from project import airflow_utils


warnings.filterwarnings("ignore")
service_name = 'dataalchemy'
project_name = 'sms-classifier'
dag_schedule = '00 18 * * 0,6'
is_production = Variable.get('environment') == 'production'
is_local = Variable.get('environment') not in ('production', 'staging')
if is_local:
    env = 'dev'
else:
    env = 'production' if is_production else 'staging'
retries = 2 if is_production else 1


general_config = config = loadconfig.config(service_name=service_name,
                                            config_folder_prefix='config',
                                            config_prefix_name='general')._load()

project_config = loadconfig.config(service_name=service_name,
                                   config_folder_prefix='config',
                                   config_prefix_name='ml')._load()

config = general_config.copy()
config.update(project_config)

curr_date = '{{ (next_execution_date + macros.timedelta(days=1)).strftime("%Y-%m-%d")}}'
curr_year = '{{ (next_execution_date + macros.timedelta(days=1)).year }}'
curr_month = '{{ (next_execution_date + macros.timedelta(days=1)).strftime("%m") }}'
curr_day = '{{ (next_execution_date + macros.timedelta(days=1)).strftime("%d") }}'
region_info_list = [{
    'region': region,
    'start_date': '{{ (next_execution_date - macros.timedelta(days=params.period_day_range) + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}',
    'end_date': '{{ next_execution_date.strftime("%Y-%m-%d") }}'
} for region in config['sms']['region']]

# Define DAG default arguments
args_params = {
    'period_day_range': config['sms']['model_update_cycle']['period_day_range'],
    'is_production': is_production,
    'env': env
}

default_args = {
    'owner': 'mars.su',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'wait_for_downstream': False,
    'params': args_params
}
default_args['on_failure_callback'] = lambda context: airflow_utils.task_fail_slack_alert(
    context,
    http_conn_id=config['airflow_connection_ids']['ml_slack']['conn_id'],
    post_channel=config['airflow_connection_ids']['ml_slack']['post_channel'])

def get_aws_ssm_value(**kwargs):
    ti = kwargs['task_instance']
    client = boto3.client(service_name='ssm', region_name=config['aws']['region'])
    response = client.get_parameter(
        Name=kwargs['templates_dict']['ssm_name'],
        WithDecryption=False)
    ti.xcom_push(key=kwargs['templates_dict']['xcom_key_name'], value=response['Parameter']['Value'])


def get_deploy_files(**kwargs):
    ti = kwargs['task_instance']
    xcom_value = ti.xcom_pull(task_ids='list_deploy_files')
    deploy_files = [
        'model_lib', 'general_config', 'custom_config', 'bootstrap',
        'data_preprocessing']
    deploy_json = dict()
    for v in xcom_value:
        for f in deploy_files:
            if f in v:
                deploy_json[f] = v
    ti.xcom_push(key='deploy_dict', value=deploy_json)

def model_weekday_check_func(**kwargs):
    curr_datetime = datetime.strptime(
        kwargs['templates_dict']['exec_date'], "%Y-%m-%d")
    exec_date = kwargs['execution_date']
    next_exec_date = kwargs['next_execution_date']
    
    if curr_datetime.weekday() == 6: # sunday
        if not kwargs['templates_dict']['augmented_data_setting']:
            next_task = 'skip_operator'
        else:
            next_task = 'get_data_augment_package'
    elif curr_datetime.weekday() == 0: # monday
        next_task = 'list_deploy_files'
    else:
        raise ValueError('Current date time is not correct, please check.')
    return next_task


def basic_tasks():
    task_launch_branch = BranchPythonOperator(
        task_id='launch_branch_check',
        python_callable=model_weekday_check_func,
        provide_context=True,
        templates_dict={
            'exec_date': curr_date,
            'augmented_data_setting': config['sms']['collect_data_condition']['use_augment_data']
        }
    )
    task_skip_operator = DummyOperator(task_id='skip_operator')

    bucket_name, folder_prefix = S3Hook.parse_s3_url(
        os.path.join(config['aws']['ml_bucket'], config['sms'][env]['deploy_folder']))
    task_get_list_s3_files = S3ListOperator(
        task_id='list_deploy_files',
        bucket=bucket_name,
        prefix=folder_prefix + '/',
        aws_conn_id=config['airflow_connection_ids']['aws_conn_id']
    )

    task_convet_records_to_dict = PythonOperator(
        task_id='convet_records_to_dict',
        provide_context=True,
        python_callable=get_deploy_files
    )

    task_get_sms_wandb_apikey = PythonOperator(
        task_id='get_sms_wandb_apikey',
        python_callable=get_aws_ssm_value,
        provide_context=True,
        templates_dict={
            'ssm_name': config['sms']['wandb_ssm_name'],
            'xcom_key_name': 'sms_wandb_apikey'
        }
    )

    

    task_get_gcp_service_account = PythonOperator(
        task_id='get_gcp_service_account',
        python_callable=get_aws_ssm_value,
        provide_context=True,
        templates_dict={
            'ssm_name': config['aws']['ssm_gcp_service_account'],
            'xcom_key_name': 'gcp_service_account'
        }
    )

    task_launch_branch >> task_get_list_s3_files >> task_convet_records_to_dict >> task_get_gcp_service_account >> task_get_sms_wandb_apikey
    return [task_launch_branch, task_get_sms_wandb_apikey]


def data_preprocessing_tasks(setting_info):
    emr_specs = EMRSpec(
        config=config,
        curr_date=curr_date,
        data_start_date=setting_info['start_date'],
        data_end_date=setting_info['end_date'],
        region=setting_info['region'],
        is_production=is_production,
        env=env)

    emr_job_flow = emr_specs.job_flow_overrides
    data_preprocessing_steps = emr_specs.data_preprocessing_steps

    job_train_flow_creator = EmrCreateJobFlowOperator(
        task_id='create_train_job_flow',
        job_flow_overrides=emr_job_flow,
        aws_conn_id=config['airflow_connection_ids']['aws_conn_id'],
        emr_conn_id='emr_default',
        retries=retries,
        retry_delay=timedelta(seconds=300)
    )

    job_add_steps_datapreprocessing = EmrAddStepsOperator(
        task_id='add_step_data_preprocessing',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_train_job_flow', key='return_value') }}",
        steps=data_preprocessing_steps,
        aws_conn_id=config['airflow_connection_ids']['aws_conn_id'],
    )

    job_moniter_steps_datapreprocessing = EmrStepSensor(
        task_id='check_step_data_preprocessing',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_train_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_step_data_preprocessing', key='return_value')[0] }}",
        aws_conn_id=config['airflow_connection_ids']['aws_conn_id'],
    )

    job_flow_terminate = EmrTerminateJobFlowOperator(
        task_id='terminate_job_flow',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_train_job_flow', key='return_value') }}",
        aws_conn_id=config['airflow_connection_ids']['aws_conn_id']
    )

    job_train_flow_creator >> job_add_steps_datapreprocessing >> job_moniter_steps_datapreprocessing >> job_flow_terminate
    
    return [job_train_flow_creator, job_flow_terminate]


def data_transfer_to_gcs():
    request_body = {
        "transfer_spec": {
            "object_conditions": {
                "max_time_elapsed_since_last_modification": "86400s",
                "include_prefixes": [
                    os.path.join(
                        config['sms'][env]['data_folder'],
                        config['sms']['output_data_folder_categ']['cleaned_data'],
                        'year={}/month={}/day={}/'.format(curr_year, curr_month, curr_day)),
                ]
            },
            "transfer_options": {
                "delete_objects_from_source_after_transfer": False,
                "overwrite_objects_already_existing_in_sink": True
            }
        },
        "schedule": {
            "schedule_start_date": {
                "year": (datetime.now(tz=pytz.timezone('Asia/Taipei')) - timedelta(days=1)).year,
                "month": (datetime.now(tz=pytz.timezone('Asia/Taipei')) - timedelta(days=1)).strftime("%m"),
                "day": (datetime.now(tz=pytz.timezone('Asia/Taipei')) - timedelta(days=1)).strftime("%d")
            },
            "schedule_end_date": {
                "year": (datetime.now(tz=pytz.timezone('Asia/Taipei')) - timedelta(days=1)).year,
                "month": (datetime.now(tz=pytz.timezone('Asia/Taipei')) - timedelta(days=1)).strftime("%m"),
                "day": (datetime.now(tz=pytz.timezone('Asia/Taipei')) - timedelta(days=1)).strftime("%d")
            }
        }
    }

    task_create_transfer_service = S3ToGoogleCloudStorageTransferOperator(
        description=(
            '{}-training-data'.format(config['project_name']) if is_production else '{}-training-data-{}'.format(config['project_name'], env)
            ) + '_' + '{{ (next_execution_date + macros.timedelta(days=1)).strftime("%Y-%m-%d") }}',
        task_id='data_transfer_service',
        s3_bucket=config['aws']['ml_bucket'].split('//')[-1],
        project_id=config['gcp']['project_id'],
        gcs_bucket=config['gcp']['ml_bucket'].split('//')[-1],
        aws_conn_id=config['airflow_connection_ids']['gcp_data_transfer_conn_id'],
        gcp_conn_id=config['airflow_connection_ids']['gcp_conn_id'],
        schedule=request_body['schedule'],
        object_conditions=request_body['transfer_spec']['object_conditions'],
        transfer_options=request_body['transfer_spec']['transfer_options'],
        wait=True,
        timeout=config['gcp_data_transfer']['timeout']
    )
    return [task_create_transfer_service]


def model_tasks(setting_info):
    task_gcs_files = GoogleCloudStorageListOperator(
        task_id='get_ml_engine_package_name',
        bucket=config['gcp']['ml_bucket'].split('//')[-1],
        prefix=config['sms'][env]['deploy_folder'] + '/',
        delimiter='.gz',
        google_cloud_storage_conn_id=config['airflow_connection_ids']['gcp_conn_id']
    )

    task_building_model_job = MLEngineStartTrainingJobOperator(
        task_id="building_model_job_{}".format(setting_info['region']),
        project_id=config['gcp']['project_id'],
        job_id="{}_{}".format(
            'sms_model_building' if is_production else 'sms_model_building_{}'.format(env),
            ('{{ (next_execution_date + macros.timedelta(days=1)).strftime("%Y%m%d") }}' + '_' + datetime.now().strftime('%H%M%S'))),
        package_uris=os.path.join(
                config['gcp']['ml_bucket'],
                '{{ task_instance.xcom_pull(task_ids="get_ml_engine_package_name", dag_id=dag.dag_id, key="return_value")[0] }}'),
        training_python_module='trainer.run_classifier',
        training_args=[
            '--job-dir', os.path.join(
                config['gcp']['ml_bucket'], config['gcp_ml_engine'][env]['output_job_dir'], 'output'),
            '--region', setting_info['region'],
            '--exec_date', curr_date,
            '--environment_mode', env,
            '--is_prediction', str(int(False)),
            '--wandb_apikey', '{{ task_instance.xcom_pull(task_ids="get_sms_wandb_apikey", dag_id=dag.dag_id, key="sms_wandb_apikey") }}',
        ],
        region=config['gcp_ml_engine_spec']['region'],
        scale_tier=config['gcp_ml_engine_spec'][setting_info['region']]['train_config']['scaleTier'],
        master_type=config['gcp_ml_engine_spec'][setting_info['region']]['train_config']['masterType'],
        runtime_version=config['gcp_ml_engine_spec']['runtimeVersion'],
        python_version=config['gcp_ml_engine_spec']['pythonVersion'],
        job_dir=os.path.join(config['gcp']['ml_bucket'], config['gcp_ml_engine'][env]['output_job_dir'], 'output'),
        gcp_conn_id=config['airflow_connection_ids']['gcp_conn_id'],
        service_account='{{ task_instance.xcom_pull(task_ids="get_gcp_service_account", dag_id=dag.dag_id, key="gcp_service_account") }}',
        retries=retries,
        retry_delay=timedelta(seconds=300)
    )

    task_predicting_data_job = MLEngineStartTrainingJobOperator(
        task_id="predicting_data_job_{}".format(setting_info['region']),
        project_id=config['gcp']['project_id'],
        job_id="{}_{}".format(
            'sms_prediciting_data' if is_production else 'sms_prediciting_data_{}'.format(env),
            ('{{ (next_execution_date + macros.timedelta(days=1)).strftime("%Y%m%d") }}' + '_' + datetime.now().strftime('%H%M%S'))),
        package_uris=os.path.join(
                config['gcp']['ml_bucket'],
                '{{ task_instance.xcom_pull(task_ids="get_ml_engine_package_name", dag_id=dag.dag_id, key="return_value")[0] }}'),
        training_python_module='trainer.run_classifier',
        training_args=[
            '--job-dir', os.path.join(
                config['gcp']['ml_bucket'], config['gcp_ml_engine'][env]['output_job_dir'], 'output'),
            '--region', setting_info['region'],
            '--exec_date', curr_date,
            '--environment_mode', env,
            '--is_prediction', str(int(True)),
            '--wandb_apikey', '{{ task_instance.xcom_pull(task_ids="get_sms_wandb_apikey", dag_id=dag.dag_id, key="sms_wandb_apikey") }}',
        ],
        region=config['gcp_ml_engine_spec']['region'],
        scale_tier=config['gcp_ml_engine_spec'][setting_info['region']]['pred_config']['scaleTier'],
        master_type=config['gcp_ml_engine_spec'][setting_info['region']]['pred_config']['masterType'],
        runtime_version=config['gcp_ml_engine_spec']['runtimeVersion'],
        python_version=config['gcp_ml_engine_spec']['pythonVersion'],
        job_dir=os.path.join(config['gcp']['ml_bucket'], config['gcp_ml_engine'][env]['output_job_dir'], 'output'),
        gcp_conn_id=config['airflow_connection_ids']['gcp_conn_id'],
        service_account='{{ task_instance.xcom_pull(task_ids="get_gcp_service_account", dag_id=dag.dag_id, key="gcp_service_account") }}',
        retries=retries,
        retry_delay=timedelta(seconds=300)
    )

    task_prediction_data_to_s3 = GoogleCloudStorageToS3Operator(
        task_id='prediction_data_to_s3_{}'.format(setting_info['region']),
        bucket=config['gcp']['ml_bucket'].split('//')[-1],
        prefix=os.path.join(
            config['sms'][env]['prediction_folder'],
            'year={}'.format(curr_year),
            'month={}'.format(curr_month),
            'day={}'.format(curr_day),
            'country={}'.format(setting_info['region'])
        ),
        delimiter=None,
        google_cloud_storage_conn_id=config['airflow_connection_ids']['gcp_conn_id'],
        dest_aws_conn_id=config['airflow_connection_ids']['aws_conn_id'],
        dest_s3_key=config['aws']['ml_bucket'] + '/',
        replace=True,
        retries=retries,
        retry_delay=timedelta(seconds=300)
    )

    task_gcs_files >> task_building_model_job
    task_building_model_job >> task_predicting_data_job >> task_prediction_data_to_s3
    
    return [task_gcs_files, task_prediction_data_to_s3]


def data_operation_tasks(setting_info):
    task_launch_labeling_job = MLEngineStartTrainingJobOperator(
        task_id="launch_labeling_job_{}".format(setting_info['region']),
        project_id=config['gcp']['project_id'],
        job_id="{}_{}".format(
            'sms_labeling_project' if is_production else 'sms_labeling_project_{}'.format(env),
            ('{{ (next_execution_date + macros.timedelta(days=1)).strftime("%Y%m%d") }}' + '_' + datetime.now().strftime('%H%M%S'))),
        package_uris=os.path.join(
                config['gcp']['ml_bucket'],
                '{{ task_instance.xcom_pull(task_ids="get_ml_engine_package_name", dag_id=dag.dag_id, key="return_value")[0] }}'),
        training_python_module='trainer.run_sms_labeling',
        training_args=[
            '--job-dir', os.path.join(
                config['gcp']['ml_bucket'], config['gcp_ml_engine'][env]['output_job_dir'], 'output'),
            '--region', setting_info['region'],
            '--exec_date', curr_date,
            '--environment_mode', env
        ],
        region=config['gcp_ml_engine_spec']['region'],
        scale_tier=config['gcp_ml_engine_spec'][setting_info['region']]['labeling_config']['scaleTier'],
        master_type=config['gcp_ml_engine_spec'][setting_info['region']]['labeling_config']['masterType'],
        runtime_version=config['gcp_ml_engine_spec']['runtimeVersion'],
        python_version=config['gcp_ml_engine_spec']['pythonVersion'],
        job_dir=os.path.join(config['gcp']['ml_bucket'], config['gcp_ml_engine'][env]['output_job_dir'], 'output'),
        gcp_conn_id=config['airflow_connection_ids']['gcp_conn_id'],
        service_account='{{ task_instance.xcom_pull(task_ids="get_gcp_service_account", dag_id=dag.dag_id, key="gcp_service_account") }}',
        retries=retries,
        retry_delay=timedelta(seconds=300)
    )

    task_labeleing_data_to_s3 = GoogleCloudStorageToS3Operator(
        task_id='labeleing_data_to_s3_{}'.format(setting_info['region']),
        bucket=config['gcp']['ml_bucket'].split('//')[-1],
        prefix=os.path.join(
            config['labebling_service'][env]['labeled_data'],
            'year={}'.format(curr_year),
            'month={}'.format(curr_month),
            'day={}'.format(curr_day),
            'country={}'.format(setting_info['region'])
        ),
        delimiter=None,
        google_cloud_storage_conn_id=config['airflow_connection_ids']['gcp_conn_id'],
        dest_aws_conn_id=config['airflow_connection_ids']['aws_conn_id'],
        dest_s3_key=config['aws']['ml_bucket'] + '/',
        replace=True,
        retries=retries,
        retry_delay=timedelta(seconds=300)
    )

    task_launch_labeling_job >> task_labeleing_data_to_s3
    
    return [task_launch_labeling_job, task_labeleing_data_to_s3]



# DAG setting
with DAG(dag_id='%s-%s' % (service_name, project_name),
         description='project-model',
         default_args=default_args,
         schedule_interval='@once' if is_local else dag_schedule) as dag:

    task_latest_only = LatestOnlyOperator(task_id='latest-only')
    basic_stream_list = basic_tasks()
    data_transfer_stream_list = data_transfer_to_gcs()
    task_latest_only >> basic_stream_list[0]

    for vars_info in region_info_list:
        data_preprocessing_stream_list = data_preprocessing_tasks(vars_info)
        model_stream_list = model_tasks(vars_info)
        data_operation_list = data_operation_tasks(vars_info)
        basic_stream_list[1] >> data_preprocessing_stream_list[0]
        data_preprocessing_stream_list[-1] >> data_transfer_stream_list[0] >> model_stream_list[0]
        model_stream_list[-1] >> data_operation_list[0]
        
