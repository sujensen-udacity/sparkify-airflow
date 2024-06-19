from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 *args,
                 task_id: str = "foo",
                 sql_statement: str = "my_sql",
                 target_db: str = "my_target_db",
                 target_table: str = "my_target_table",
                 redshift_conn_id: str = "my_redshift",
                 append_only: bool = False,
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, task_id=task_id, **kwargs)
        self.task_id = task_id
        self.sql_statement = sql_statement
        self.target_db = target_db
        self.target_table = target_table
        self.redshift_conn_id = redshift_conn_id
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if not self.append_only:
            self.log.info('Deleting data from the dimension table')
            delete_from_statement = "truncate table {0}.{1}".format(self.target_db, self.target_table)
            redshift_hook.run(delete_from_statement)
        self.log.info('Writing data to the dimension table')
        insert_statement = "insert into {0}.{1} ({2})".format(self.target_db, self.target_table, self.sql_statement)
        redshift_hook.run(insert_statement)
