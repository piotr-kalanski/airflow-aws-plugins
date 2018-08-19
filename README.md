# airflow-aws-plugin

Airflow plugin with AWS operators

# Table of contents

- [Installation](#installation)
- [Operators](#operators)
    - [AWS Lambda](#aws-lambda)
        - [ExecuteLambdaOperator](#executelambdaoperator)
    - [AWS Redshift](#aws-redshift)    
        - [ExecuteRedshiftQueryOperator](#executeredshiftqueryoperator)
        - [ExecuteCopyToRedshiftOperator](#executecopytoredshiftoperator)            

# Installation

Copy [aws_operators](/aws_operators) directory to *plugins* directory in airflow (default AIRFLOW_HOME/plugins/).

# Operators

List of operators by AWS service:

## AWS Lambda

### ExecuteLambdaOperator

Operator responsible for triggering AWS Lambda function.

*Example:*

```python
ExecuteLambdaOperator(
    task_id='task_with_execute_lambda_operator',
    airflow_context_to_lambda_payload=lambda c: {"date": c["execution_date"].strftime('%Y-%m-%d')   },
    additional_payload={"param1": "value1", "param2": 21},
    lambda_function_name="LambdaFunctionName"
)
```

Above task executes AWS Lambda function `LambdaFunctionName` with payload:

```json
{
  "date": "2018-08-01",
  "param1": "value1",
  "param2": 21
}
```
where `date` is equal to `execution_date` of airflow dag. This is extracted by `airflow_context_to_lambda_payload` function from airflow context dictionary.

## AWS Redshift

### ExecuteRedshiftQueryOperator

Execute Redshift query.

*Example:*

DROP Redshift table:

```python
ExecuteRedshiftQueryOperator(
    task_id='drop_table',
    redshift_conn_id='redshift_dev',
    query='DROP TABLE IF EXISTS TEST_TABLE'
)
```

### ExecuteCopyToRedshiftOperator

Execute Redshift COPY command.

*Example 1 - append data:*

```python
ExecuteCopyToRedshiftOperator(
    task_id='redshift_copy_append',
    redshift_conn_id='redshift_dev',
    s3_bucket='bucket',
    s3_key='key',
    redshift_schema='public',
    table='table',
    iam_role='iam_role',
    mode='append'
)
```

*Example 2 - overwrite table:*

```python
ExecuteCopyToRedshiftOperator(
    task_id='redshift_copy_overwrite',
    redshift_conn_id='redshift_dev',
    s3_bucket='bucket',
    s3_key='key',
    redshift_schema='public',
    table='table',
    iam_role='iam_role',
    mode='overwrite',
    copy_params=['CSV']
)
```

#### S3 key dependent on airflow context

Source S3 key can be constructed using custom Python function based on airflow context.

Example:

```python
ExecuteCopyToRedshiftOperator(
    s3_key=lambda context: "year={y}/month={m}/day={d}/".format(y=context["execution_date"].year, m=context["execution_date"].strftime("%m"), d=context["execution_date"].strftime("%d"))
)
```
