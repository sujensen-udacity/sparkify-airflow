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
                 **kwargs):

        super(LoadFactOperator, self).__init__(*args, task_id=task_id, **kwargs)
        self.task_id = task_id
        self.sql_statement = sql_statement
        self.target_db = target_db
        self.target_table = target_table

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        insert_statement = "insert into {0}.{1} ({2})".format(self.target_db, self.target_table, self.sql_statement)
        redshift_hook.run(insert_statement)