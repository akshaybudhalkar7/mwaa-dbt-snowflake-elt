from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_mwaa as mwaa
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_deployment as s3deploy
from constructs import Construct

class MwaaStack(Stack):
    """MWAA environment: uploads dags/config to S3, execution role, security group, environment.

    Reusable for blue/green: each environment gets its own name, Airflow version,
    local source folders and S3 prefix inside the shared MWAA bucket.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        mwaa_bucket: s3.IBucket,
        data_lake_bucket: s3.IBucket,
        env_name: str,
        airflow_version: str,
        dags_dir: str,
        config_dir: str,
        s3_prefix: str = "",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- 1. Upload files to the MWAA bucket (must exist before the environment is created) ---
        dags_deploy = s3deploy.BucketDeployment(
            self,
            "DeployDags",
            sources=[s3deploy.Source.asset(dags_dir)],
            destination_bucket=mwaa_bucket,
            destination_key_prefix=f"{s3_prefix}dags",
            prune=True,
        )
        # requirements.txt + startup.sh; prune=False so it never deletes dags/ or other envs' files
        config_deploy_kwargs = {"destination_key_prefix": s3_prefix} if s3_prefix else {}
        config_deploy = s3deploy.BucketDeployment(
            self,
            "DeployConfig",
            sources=[s3deploy.Source.asset(config_dir)],
            destination_bucket=mwaa_bucket,
            prune=False,
            **config_deploy_kwargs,
        )

        # --- 2. Security group: MWAA components talk to each other over a self-referencing rule ---
        sg = ec2.SecurityGroup(self, "MwaaSg", vpc=vpc, description="MWAA environment", allow_all_outbound=True)
        sg.add_ingress_rule(sg, ec2.Port.all_traffic(), "MWAA components")

        # --- 3. Execution role: what Airflow tasks are allowed to do in AWS ---
        role = iam.Role(
            self,
            "ExecutionRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com"),
            ),
        )
        mwaa_bucket.grant_read(role)
        data_lake_bucket.grant_read_write(role)
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["airflow:PublishMetrics"],
                resources=[f"arn:aws:airflow:{self.region}:{self.account}:environment/{env_name}"],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogStream",
                    "logs:CreateLogGroup",
                    "logs:PutLogEvents",
                    "logs:GetLogEvents",
                    "logs:GetLogRecord",
                    "logs:GetLogGroupFields",
                    "logs:GetQueryResults",
                ],
                resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:airflow-{env_name}-*"],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["logs:DescribeLogGroups", "cloudwatch:PutMetricData", "s3:GetAccountPublicAccessBlock"],
                resources=["*"],
            )
        )
        # Celery broker queue owned by the MWAA service account
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "sqs:ChangeMessageVisibility",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                    "sqs:GetQueueUrl",
                    "sqs:ReceiveMessage",
                    "sqs:SendMessage",
                ],
                resources=[f"arn:aws:sqs:{self.region}:*:airflow-celery-*"],
            )
        )
        # AWS-owned KMS key that encrypts the SQS queue (only usable via SQS)
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey*", "kms:Encrypt"],
                not_resources=[f"arn:aws:kms:*:{self.account}:key/*"],
                conditions={"StringLike": {"kms:ViaService": [f"sqs.{self.region}.amazonaws.com"]}},
            )
        )

        # --- 4. The environment (only an L1 construct exists for MWAA) ---
        def log(level: str) -> mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty:
            return mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(enabled=True, log_level=level)

        environment = mwaa.CfnEnvironment(
            self,
            "Environment",
            name=env_name,
            airflow_version=airflow_version,
            environment_class="mw1.small",
            min_workers=1,
            max_workers=2,
            execution_role_arn=role.role_arn,
            source_bucket_arn=mwaa_bucket.bucket_arn,
            dag_s3_path=f"{s3_prefix}dags",
            requirements_s3_path=f"{s3_prefix}requirements.txt",
            startup_script_s3_path=f"{s3_prefix}startup.sh",
            webserver_access_mode="PUBLIC_ONLY",
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=[sg.security_group_id],
                subnet_ids=[subnet.subnet_id for subnet in vpc.private_subnets],
            ),
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=log("WARNING"),
                scheduler_logs=log("WARNING"),
                task_logs=log("INFO"),
                webserver_logs=log("WARNING"),
                worker_logs=log("WARNING"),
            ),
            airflow_configuration_options={"core.load_examples": "False"},
        )
        # Role policies and S3 files must be in place before MWAA validates the environment
        environment.node.add_dependency(role)
        environment.node.add_dependency(dags_deploy)
        environment.node.add_dependency(config_deploy)

        CfnOutput(self, "AirflowUiUrl", value=f"https://{environment.attr_webserver_url}")
        CfnOutput(self, "ExecutionRoleArn", value=role.role_arn)
