from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 *args,
                 task_id: str = "foo",
                 sql_statement: str = "my_sql",
                 target_db: str = "my_target_db",
                 target_table: str = "my_target_table",
                 redshift_conn_id: str = "my_redshift",
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, task_id=task_id, **kwargs)
        self.task_id = task_id
        self.sql_statement = sql_statement
        self.target_db = target_db
        self.target_table = target_table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Appending data to the fact table')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        insert_statement = "insert into {0}.{1} ({2})".format(self.target_db, self.target_table, self.sql_statement)
        redshift_hook.run(insert_statement)