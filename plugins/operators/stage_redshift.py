from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 *args,
                 task_id: str = "foo",
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, task_id=task_id, **kwargs)
        self.task_id = task_id

    def execute(self, context):
        self.log.info("StageToRedshiftOperator not implemented yet, taskId = " + self.task_id)





