from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from aws_operators.operators.custom_operators import ExecuteRedshiftQueryOperator
from datetime import datetime


with DAG('dag_with_execute_redshift_operator', start_date=datetime(2018, 8, 11)) as dag:
    (
        BashOperator(
            task_id='bash_hello',
            bash_command='echo "HELLO!"'
        )
        >> ExecuteRedshiftQueryOperator(
            task_id='task_with_execute_redshift_operator'
        )
    )
