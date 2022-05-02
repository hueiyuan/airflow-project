import logging
import re
import warnings
from typing import Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink
from airflow.models.taskinstance import TaskInstance
from airflow.providers.google.cloud.hooks.mlengine import MLEngineHook
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

def _normalize_mlengine_job_id(job_id: str) -> str:
    """
    Replaces invalid MLEngine job_id characters with '_'.
    This also adds a leading 'z' in case job_id starts with an invalid
    character.
    :param job_id: A job_id str that may have invalid characters.
    :type job_id: str:
    :return: A valid job_id representation.
    :rtype: str
    """
    # Add a prefix when a job_id starts with a digit or a template
    match = re.search(r'\d|\{{2}', job_id)
    if match and match.start() == 0:
        job = f'z_{job_id}'
    else:
        job = job_id

    # Clean up 'bad' characters except templates
    tracker = 0
    cleansed_job_id = ''
    for match in re.finditer(r'\{{2}.+?\}{2}', job):
        cleansed_job_id += re.sub(r'[^0-9a-zA-Z]+', '_', job[tracker : match.start()])
        cleansed_job_id += job[match.start() : match.end()]
        tracker = match.end()

    # Clean up last substring or the full string if no templates
    cleansed_job_id += re.sub(r'[^0-9a-zA-Z]+', '_', job[tracker:])

    return cleansed_job_id

class AIPlatformConsoleLink(BaseOperatorLink):
    """Helper class for constructing AI Platform Console link."""

    name = "AI Platform Console"

    def get_link(self, operator, dttm):
        task_instance = TaskInstance(task=operator, execution_date=dttm)
        gcp_metadata_dict = task_instance.xcom_pull(task_ids=operator.task_id, key="gcp_metadata")
        if not gcp_metadata_dict:
            return ''
        job_id = gcp_metadata_dict['job_id']
        project_id = gcp_metadata_dict['project_id']
        console_link = f"https://console.cloud.google.com/ai-platform/jobs/{job_id}?project={project_id}"
        return console_link

class MLEngineStartTrainingJobOperatorV2(BaseOperator):
    """
    Operator for launching a MLEngine training job.
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MLEngineStartTrainingJobOperator`
    :param job_id: A unique templated id for the submitted Google MLEngine
        training job. (templated)
    :type job_id: str
    :param region: The Google Compute Engine region to run the MLEngine training
        job in (templated).
    :type region: str
    :param package_uris: A list of Python package locations for the training
        job, which should include the main training program and any additional
        dependencies. This is mutually exclusive with a custom image specified
        via master_config. (templated)
    :type package_uris: List[str]
    :param training_python_module: The name of the Python module to run within
        the training job after installing the packages. This is mutually
        exclusive with a custom image specified via master_config. (templated)
    :type training_python_module: str
    :param training_args: A list of command-line arguments to pass to the
        training program. (templated)
    :type training_args: List[str]
    :param scale_tier: Resource tier for MLEngine training job. (templated)
    :type scale_tier: str
    :param master_type: The type of virtual machine to use for the master
        worker. It must be set whenever scale_tier is CUSTOM. (templated)
    :type master_type: str
    :param master_config: The configuration for the master worker. If this is
        provided, master_type must be set as well. If a custom image is
        specified, this is mutually exclusive with package_uris and
        training_python_module. (templated)
    :type master_config: dict
    :param runtime_version: The Google Cloud ML runtime version to use for
        training. (templated)
    :type runtime_version: str
    :param python_version: The version of Python used in training. (templated)
    :type python_version: str
    :param job_dir: A Google Cloud Storage path in which to store training
        outputs and other data needed for training. (templated)
    :type job_dir: str
    :param service_account: Optional service account to use when running the training application.
        (templated)
        The specified service account must have the `iam.serviceAccounts.actAs` role. The
        Google-managed Cloud ML Engine service account must have the `iam.serviceAccountAdmin` role
        for the specified service account.
        If set to None or missing, the Google-managed Cloud ML Engine service account will be used.
    :type service_account: str
    :param project_id: The Google Cloud project name within which MLEngine training job should run.
        If set to None or missing, the default project_id from the Google Cloud connection is used.
        (templated)
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param mode: Can be one of 'DRY_RUN'/'CLOUD'. In 'DRY_RUN' mode, no real
        training job will be launched, but the MLEngine training job request
        will be printed out. In 'CLOUD' mode, a real MLEngine training job
        creation request will be issued.
    :type mode: str
    :param labels: a dictionary containing labels for the job; passed to BigQuery
    :type labels: Dict[str, str]
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = [
        '_project_id',
        '_job_id',
        '_region',
        '_package_uris',
        '_training_python_module',
        '_training_args',
        '_scale_tier',
        '_master_type',
        '_master_config',
        '_runtime_version',
        '_python_version',
        '_job_dir',
        '_service_account',
        '_impersonation_chain',
    ]
    
    operator_extra_links = (AIPlatformConsoleLink(),)

    @apply_defaults
    def __init__(
        self,
        *,
        job_id: str,
        region: str,
        package_uris: List[str] = None,
        training_python_module: str = None,
        training_args: List[str] = None,
        scale_tier: Optional[str] = None,
        master_type: Optional[str] = None,
        master_config: Optional[Dict] = None,
        runtime_version: Optional[str] = None,
        python_version: Optional[str] = None,
        job_dir: Optional[str] = None,
        service_account: Optional[str] = None,
        project_id: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        delegate_to: Optional[str] = None,
        mode: str = 'PRODUCTION',
        labels: Optional[Dict[str, str]] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._project_id = project_id
        self._job_id = job_id
        self._region = region
        self._package_uris = package_uris
        self._training_python_module = training_python_module
        self._training_args = training_args
        self._scale_tier = scale_tier
        self._master_type = master_type
        self._master_config = master_config
        self._runtime_version = runtime_version
        self._python_version = python_version
        self._job_dir = job_dir
        self._service_account = service_account
        self._gcp_conn_id = gcp_conn_id
        self._delegate_to = delegate_to
        self._mode = mode
        self._labels = labels
        self._impersonation_chain = impersonation_chain

        custom = self._scale_tier is not None and self._scale_tier.upper() == 'CUSTOM'
        custom_image = (
            custom
            and self._master_config is not None
            and self._master_config.get('imageUri', None) is not None
        )

        if not self._project_id:
            raise AirflowException('Google Cloud project id is required.')
        if not self._job_id:
            raise AirflowException('An unique job id is required for Google MLEngine training job.')
        if not self._region:
            raise AirflowException('Google Compute Engine region is required.')
        if custom and not self._master_type:
            raise AirflowException('master_type must be set when scale_tier is CUSTOM')
        if self._master_config and not self._master_type:
            raise AirflowException('master_type must be set when master_config is provided')
        if not (package_uris and training_python_module) and not custom_image:
            raise AirflowException(
                'Either a Python package with a Python module or a custom Docker image should be provided.'
            )
        # Issue: SI-2078, Only disable the conflict condition code from original MLEngineStartTrainingJobOperator code.
        #if (package_uris or training_python_module) and custom_image:
        #    raise AirflowException(
        #        'Either a Python package with a Python module or '
        #        'a custom Docker image should be provided but not both.'
        #    )

    def execute(self, context):
        job_id = _normalize_mlengine_job_id(self._job_id)
        training_request = {
            'jobId': job_id,
            'trainingInput': {
                'scaleTier': self._scale_tier,
                'region': self._region,
            },
        }
        if self._package_uris:
            training_request['trainingInput']['packageUris'] = self._package_uris

        if self._training_python_module:
            training_request['trainingInput']['pythonModule'] = self._training_python_module

        if self._training_args:
            training_request['trainingInput']['args'] = self._training_args

        if self._master_type:
            training_request['trainingInput']['masterType'] = self._master_type

        if self._master_config:
            training_request['trainingInput']['masterConfig'] = self._master_config

        if self._runtime_version:
            training_request['trainingInput']['runtimeVersion'] = self._runtime_version

        if self._python_version:
            training_request['trainingInput']['pythonVersion'] = self._python_version

        if self._job_dir:
            training_request['trainingInput']['jobDir'] = self._job_dir

        if self._service_account:
            training_request['trainingInput']['serviceAccount'] = self._service_account

        if self._labels:
            training_request['labels'] = self._labels

        if self._mode == 'DRY_RUN':
            self.log.info('In dry_run mode.')
            self.log.info('MLEngine Training job request is: %s', training_request)
            return
        

        hook = MLEngineHook(
            gcp_conn_id=self._gcp_conn_id,
            delegate_to=self._delegate_to,
            impersonation_chain=self._impersonation_chain,
        )

        # Helper method to check if the existing job's training input is the
        # same as the request we get here.
        def check_existing_job(existing_job):
            existing_training_input = existing_job.get('trainingInput')
            requested_training_input = training_request['trainingInput']
            if 'scaleTier' not in existing_training_input:
                existing_training_input['scaleTier'] = None

            existing_training_input['args'] = existing_training_input.get('args')
            requested_training_input["args"] = (
                requested_training_input['args'] if requested_training_input["args"] else None
            )

            return existing_training_input == requested_training_input

        finished_training_job = hook.create_job(
            project_id=self._project_id, job=training_request, use_existing_job_fn=check_existing_job
        )

        if finished_training_job['state'] != 'SUCCEEDED':
            self.log.error('MLEngine training job failed: %s', str(finished_training_job))
            raise RuntimeError(finished_training_job['errorMessage'])

        gcp_metadata = {
            "job_id": job_id,
            "project_id": self._project_id,
        }
        context['task_instance'].xcom_push("gcp_metadata", gcp_metadata)
