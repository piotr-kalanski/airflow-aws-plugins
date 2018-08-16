from airflow.models import BaseOperator


class ExecuteRedshiftQueryOperator(BaseOperator):

    def __init__(self, *args, **kwargs):
        super(ExecuteRedshiftQueryOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        print("Hello World")
        print(context)
