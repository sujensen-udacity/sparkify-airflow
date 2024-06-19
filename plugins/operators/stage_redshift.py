from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    staging_table_copy = ("""
    copy {}
    from '{}'
    access_key_id '{}'
    secret_access_key '{}'
    json '{}'
    region '{}'
    """)

    @apply_defaults
    def __init__(self,
                 *args,
                 task_id: str = "foo",
                 table: str = "my_table",
                 s3_key: str = "my_key",
                 s3_bucket: str = "my_bucket",
                 redshift_conn_id: str = "my_redshift",
                 aws_credentials_id: str = "my_creds",
                 json_format: str = "auto",
                 region: str = "us-east-1",

                 **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, task_id=task_id, **kwargs)
        self.task_id = task_id

        self.table = table
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.json_format = json_format
        self.region = region

    def execute(self, context):
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("s3 key to be loaded: " + self.s3_key)
        formatted_copy_statement = self.staging_table_copy.format(
            self.table,
            "s3://{}/{}".format(self.s3_bucket, self.s3_key),
            aws_connection.login,
            aws_connection.password,
            self.json_format,
            self.region
        )
        redshift.run(formatted_copy_statement)
