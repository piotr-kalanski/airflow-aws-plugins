from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging


class ExecuteRedshiftQueryOperator(BaseOperator):

    def __init__(self, redshift_conn_id, query: str, *args, **kwargs):
        """
        :param redshift_conn_id: the destination redshift connection id
        :param query: SQL query to execute
        """
        super(ExecuteRedshiftQueryOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        pg_hook = PostgresHook(self.redshift_conn_id)
        logging.info("Execute Redshift query {}".format(self.query))
        pg_hook.run(self.query)


class ExecuteCopyToRedshiftOperator(BaseOperator):

    def __init__(
            self,
            redshift_conn_id,
            s3_bucket: str,
            s3_key: str,
            redshift_schema: str,
            table: str,
            iam_role: str,
            mode: str,
            copy_params=list(),
            *args,
            **kwargs
    ):
        super(ExecuteCopyToRedshiftOperator, self).__init__(*args, **kwargs)
        # TODO based on: https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift_operator.py

    def execute(self, context):
        # TODO
        print("ExecuteCopyToRedshiftOperator")
