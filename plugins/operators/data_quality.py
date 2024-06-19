from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import List

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 *args,
                 task_id: str = "foo",
                 sql_statements: List[str] = (),
                 expected_results: List[int] = (),
                 redshift_conn_id: str = "my_redshift",
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, task_id=task_id, **kwargs)
        self.task_id = task_id

        self.sql_statements = sql_statements
        self.expected_results = expected_results
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info('Running data quality checks')
        if len(self.sql_statements) != len(self.expected_results):
            msg = "Number of data quality sql statements must equal number of expected results"
            self.log.exception(msg)
            raise Exception(msg)
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for i in range(len(self.sql_statements)):
            sql_stmt = self.sql_statements[i]
            expected_result = self.expected_results[i]
            actual_result = redshift_hook.get_first(sql_stmt)[0]
            if actual_result != expected_result:
                msg = "Data quality check failed ({0})".format(sql_stmt)
                self.log.exception(msg)
                raise Exception(msg)

