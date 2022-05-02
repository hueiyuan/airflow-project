import time
from typing import Any, Dict, List, Optional
from threading import Lock
from uuid import uuid4

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

class AWSAthenaForQueriesHook(AwsBaseHook):
    INTERMEDIATE_STATES = (
        'QUEUED',
        'RUNNING',
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(client_type='athena', *args, **kwargs)  # type: ignore

        self._lock = Lock()
        self._cur_query_id = None

    def run_query(
        self,
        queries: List[str],
        query_context: Dict[str, str],
        result_configuration: Dict[str, Any],
        client_request_token: Optional[str] = None,
        workgroup: str = 'primary',
    ):
        
        for idx, query in enumerate(queries, 1):
            params = {
                'QueryString': query,
                'QueryExecutionContext': query_context,
                'ResultConfiguration': result_configuration,
                'WorkGroup': workgroup,
            }
            if client_request_token:
                params['ClientRequestToken'] = str(uuid4())

            response = self.get_conn().start_query_execution(**params)
            query_execution_id = response['QueryExecutionId']
            self._cur_query_id = query_execution_id

            self.log.info('Athena query id: #%d %s', idx, query_execution_id)

            try:
                while True:
                    exec_resp = self.get_conn().get_query_execution(
                        QueryExecutionId=query_execution_id)
                    status = exec_resp['QueryExecution']['Status']['State']
                    if status not in self.INTERMEDIATE_STATES:
                        break

                    self.log.info('Athena query is running, wait 3 seconds')
                    time.sleep(3)
            finally:
                with self._lock:
                    self._cur_query_id = None

            self.log.info('Athena query finished: #%d', idx)
            if status != 'SUCCEEDED':
                err_msg = 'Athena Query ({}) Failure: {}'.format(
                    query_execution_id, status)
                raise AirflowException(err_msg)


class AWSAthenaForQueriesOperator(BaseOperator):
    ui_color = '#44b5e2'
    template_fields = ('queries', 'database', 'output_location')
    template_ext = ('.sql',)

    @apply_defaults
    def __init__( 
        self,
        *,
        queries: List[str],
        database: str,
        output_location: str,
        aws_conn_id: str = "aws_default",
        client_request_token: Optional[str] = None,
        workgroup: str = "primary",
        query_execution_context: Optional[Dict[str, str]] = None,
        result_configuration: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.queries = queries
        self.database = database
        self.output_location = output_location
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.workgroup = workgroup
        self.query_execution_context = query_execution_context or {}
        self.result_configuration = result_configuration or {}
        
        self._hook = None

    def get_hook(self):
        return AWSAthenaForQueriesHook(self.aws_conn_id)

    def execute(self, context: dict):
        self.query_execution_context['Database'] = self.database
        self.result_configuration['OutputLocation'] = self.output_location
        self._hook = self.get_hook()

        self._hook.run_query(
            self.queries,
            self.query_execution_context,
            self.result_configuration,
            self.client_request_token,
            self.workgroup,
        )

    def on_kill(self):
        if self._hook:
            self._hook.kill()
