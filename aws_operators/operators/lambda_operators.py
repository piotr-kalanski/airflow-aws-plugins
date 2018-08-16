from airflow.models import BaseOperator


class ExecuteLambdaOperator(BaseOperator):

    def __init__(self, *args, **kwargs):
        super(ExecuteLambdaOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        # TODO
        print("ExecuteLambdaOperator")

