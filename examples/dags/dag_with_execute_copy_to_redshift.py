from airflow import DAG
from aws_operators.operators.redshift_operators import ExecuteCopyToRedshiftOperator
from datetime import datetime


with DAG('dag_with_execute_redshift_operator', start_date=datetime(2018, 8, 11)) as dag:
    (
        ExecuteCopyToRedshiftOperator(
            redshift_conn_id='',
            s3_bucket='bucket',
            s3_key='key',
            redshift_schema='public',
            table='table',
            iam_role='iam_role',
            mode='append'
        )
        >> ExecuteCopyToRedshiftOperator(
            redshift_conn_id='',
            s3_bucket='bucket',
            s3_key='key',
            redshift_schema='public',
            table='table',
            iam_role='iam_role',
            mode='overwrite',
            copy_params=['CSV']
        )
        >> ExecuteCopyToRedshiftOperator(
            redshift_conn_id='',
            s3_bucket='bucket',
            s3_key='key',
            redshift_schema='public',
            table='table',
            iam_role='iam_role',
            mode='append_overwrite',
            where_condition_fn=lambda c: 'DATE = ' + c['date']
        )
    )
