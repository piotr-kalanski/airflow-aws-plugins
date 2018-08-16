from airflow.models import BaseOperator


class ExecuteRedshiftQueryOperator(BaseOperator):

    def __init__(self, *args, **kwargs):
        super(ExecuteRedshiftQueryOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # TODO
        print("ExecuteRedshiftQueryOperator")


class ExecuteCopyToRedshiftOperator(BaseOperator):

    def __init__(self, *args, **kwargs):
        super(ExecuteCopyToRedshiftOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # TODO
        print("ExecuteCopyToRedshiftOperator")
