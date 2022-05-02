JOB_FLOW_OVERRIDES = {
    'Name': 'emr-cluster-{}'.format('{{ params.env }}'),
    'LogUri': 's3://aws-logs-473024607515-ap-northeast-1/elasticmapreduce/',
    'ReleaseLabel': 'emr-6.4.0',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'r3.8xlarge',
                'InstanceCount': 2,
            },
        ],
        'Ec2KeyName': 'my_aws_pem',
        'Ec2SubnetId': 'subnet-XXXX',
        'ServiceAccessSecurityGroup': 'sg-XXX',
        'EmrManagedMasterSecurityGroup': 'sg-XXXX',
        'EmrManagedSlaveSecurityGroup': 'sg-XXXX',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'BootstrapActions':[
        {
            'Name': 'emr-bootstrap-env',
            'ScriptBootstrapAction': {
                'Path': 's3://bucket/{{ params.env }}/my/bootstrap/emr-bootstrap-cluster-{{ params.env }}.sh'
            }
        },
    ],
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'Tags': [
        {
            'Key': 'Cost',
            'Value': 'Unit'
        },
        {
            'Key': 'Name',
            'Value': 'emr-cluster-{}'.format('{{ params.env }}')
        },
        {
            'Key': 'Environment',
            'Value': '{{ params.env }}'
        }
    ],
}

SPARK_STEPS = [
    {
        'Name': 'spark_step',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 's3://ap-northeast-1.elasticmapreduce/libs/script-runner/script-runner.jar',
            'Args': [
                'spark-submit',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '-py-files' ,'s3://bucket/deploy/my_project-lib-1.0.0-py3.8.egg',
                '--files', 's3://bucket/deploy/general_config.yaml,s3://bucket/deploy/custom_config.yaml',
                's3://bucket/deploy/main_spark.py',
                '--last-exec-date', "{{ (prev_execution_date + macros.timedelta(days=1)).strftime('%Y-%m-%d')}}",
                '--region', 'TW',
                '--environment_mode', '{{ params.env }}'
            ],
        },
    }
]
