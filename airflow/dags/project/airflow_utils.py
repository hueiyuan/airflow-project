from airflow.hooks.base_hook import BaseHook
from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook


# Define function
def task_fail_slack_alert(context, http_conn_id, post_channel):
    slack_msg = """
            :red_circle: Task Failed. 
            *Environment*: `{env}`
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        env='Production' if context['params']['is_production'] else 'Staging/Local',
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    slack_alert = SlackWebhookHook(
        http_conn_id=http_conn_id,
        message=slack_msg,
        channel=post_channel,
        username='Airflow')
    return slack_alert.execute()


def task_retry_slack_alert(context, http_conn_id, post_channel):
    slack_msg = """
            :white_circle: Task to retry.
            *Environment*: `{env}`
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        env='Production' if context['params']['is_production'] else 'Staging/Local',
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    slack_alert = SlackWebhookHook(
        http_conn_id=http_conn_id,
        message=slack_msg,
        channel=post_channel,
        username='Airflow')
    return slack_alert.execute()


def task_success_callback_func(context, http_conn_id, post_channel):
    if bool('message' in context['params'].keys()):
        slack_msg = 'Finished DAG: `{0}`, Exec Data: `{1}`, *{2}*'.format(
            context.get('task_instance').dag_id,
            context['execution_date'].date(),
            context['params']['message']
        )
    else:
        slack_msg = "Finished DAG: `{0}`, Exec Date: `{1}` :dart: ".format(
            context.get('task_instance').dag_id,
            context['execution_date'].date()
        )
    slack_alert = SlackWebhookHook(
        http_conn_id=http_conn_id,
        message=slack_msg,
        channel=post_channel,
        username='Airflow')
    return slack_alert.execute()
