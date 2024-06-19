from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)

    staging_events_copy = ("""
    copy staging_events (artist, auth, firstname, gender, iteminsession, lastname, length, level, location, method, page, registration, sessionid, song, status, ts, useragent, userid)
    from '{}'
    access_key_id '{}'
    secret_access_key '{}'
    json 's3://{}/log_json_path.json'
    region 'us-east-1';
    """)

    # Only using a small subset of song data in s3, due to long loading time
    staging_songs_copy = ("""
    copy staging_songs (num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)
    from '{}' 
    access_key_id '{}'
    secret_access_key '{}'
    json 'auto'
    region 'us-east-1'
    trimblanks
    truncatecolumns;
    """)

    @apply_defaults
    def __init__(self,
                 *args,
                 task_id: str = "foo",
                 redshift_conn_id: str = "redshift",
                 aws_credentials_id: str = "aws_credentials",
                 s3_bucket: str = "susan-airflow-bucket-3",
                 s3_key: str = "foobar",
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, task_id=task_id, **kwargs)
        self.task_id = task_id

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        # Attributes of the operator, but not args.
        self.table = ""

    def execute(self, context):
        if self.task_id == "Stage_events":
            self.table = "staging_events"
            self.s3_key = "log-data/{execution_date.year}/{execution_date.month:02}/{ds}-events.json"
            final_s3_key = self.s3_key.format(**context)
            copy_statement = self.staging_events_copy
        elif self.task_id == "Stage_songs":
            self.table = "staging_songs"
            self.s3_key = "song-data/A"
            final_s3_key = self.s3_key
            copy_statement = self.staging_songs_copy
        else:
            msg = "Task can be either Stage_events or Stage_songs"
            self.log.exception(msg)
            raise Exception(msg)

        for i in context.keys():
            self.log.info("i =" + i)
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_copy_statement = copy_statement.format(
            "s3://{}/{}".format(self.s3_bucket, final_s3_key),
            aws_connection.login,
            aws_connection.password,
            self.s3_bucket)
        redshift.run(formatted_copy_statement)






