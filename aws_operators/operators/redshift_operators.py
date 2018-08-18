from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
import logging


class ExecuteRedshiftQueryOperator(BaseOperator):

    def __init__(self, redshift_conn_id, query, *args, **kwargs):
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
            s3_bucket,
            s3_key,
            redshift_schema,
            table,
            iam_role,
            mode,
            where_condition_fn=None,
            copy_params=[],
            *args,
            **kwargs
    ):
        """

        Modes:
        - append - just insert new rows to table
        - overwrite - truncate table and insert new rows
        - append_overwrite - remove selected rows using condition where_condition and then insert new rows

        :param redshift_conn_id: the destination redshift connection id
        :param s3_bucket: name of source S3 bucket
        :param s3_key: path to source data in S3 bucket
        :param redshift_schema: name of destination Redshift schema
        :param table: name of destination Redshift table
        :param iam_role: name of IAM role for Redshift COPY command
        :param mode: append, overwrite, append_overwrite
        :param where_condition_fn: obligatory parameter for append_overwrite mode, function returning condition for WHERE statement in delete
        :param copy_params: additional COPY command parameters
        """

        super(ExecuteCopyToRedshiftOperator, self).__init__(*args, **kwargs)
        self.pg_hook = PostgresHook(redshift_conn_id)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_schema = redshift_schema
        self.table = table
        self.full_table_name = self.redshift_schema + "." + self.table
        self.iam_role = iam_role
        self.mode = mode.upper()
        self.where_condition_fn = where_condition_fn
        self.copy_params = copy_params

    def execute(self, context):
        if self.mode == "OVERWRITE":
            self.__truncate_table()
            self.__execute_copy()
        elif self.mode == "APPEND":
            self.__execute_copy()
        elif self.mode == "APPEND_OVERWRITE":
            self.__delete_from_table(context)
            self.__execute_copy()
            self.__vacuum_table()

    def __execute_query(self, query):
        print("Executing query: " + query)
        # TODO - uncomment:
        #self.pg_hook.run(query)

    def __vacuum_table(self):
        query = "VACUUM FULL TABLE {}".format(self.full_table_name)
        self.__execute_query(query)

    def __truncate_table(self):
        query = "TRUNCATE TABLE {}".format(self.full_table_name)
        self.__execute_query(query)

    def __delete_from_table(self, context):
        condition = self.where_condition_fn(context)
        query = "DELETE FROM TABLE {} WHERE {}".format(self.full_table_name, condition)
        self.__execute_query(query)

    def __execute_copy(self):
        copy_query = self.__construct_copy_query()
        self.__execute_query(copy_query)

    def __construct_copy_query(self) -> str:
        additional_params = '\n'.join(self.copy_params)
        return """
        COPY {table}
        FROM 's3://{bucket}/{key}'
        CREDENTIALS 'iam_role={iam_role}'
        {additional_params}
        """.format(
            table=self.full_table_name,
            bucket=self.s3_bucket,
            key=self.s3_key,
            iam_role=self.iam_role,
            additional_params=additional_params
        )
