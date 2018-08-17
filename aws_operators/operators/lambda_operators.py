from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException

import boto3
import json
import logging
import base64


class ExecuteLambdaOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            airflow_context_to_lambda_payload,
            additional_payload: dict,
            lambda_function_name: str,
            *args,
            **kwargs
    ):
        """
        :param airflow_context_to_lambda_payload: function extracting fields from Airflow context to Lambda payload
        :param additional_payload: additional parameters for Lambda payload
        :param lambda_function_name: name of Lambda function
        :param args:
        :param kwargs:
        """
        super(ExecuteLambdaOperator, self).__init__(*args, **kwargs)
        self._airflow_context_to_lambda_payload = airflow_context_to_lambda_payload
        self._additional_payload = additional_payload
        self._lambda_function_name = lambda_function_name
        self._lambda_client = boto3.client('lambda')

    def execute(self, context):
        request_payload = self.__create_lambda_payload(context)

        logging.info('Executing AWS Lambda {} with payload {}'.format(self._lambda_function_name, request_payload))

        response = self._lambda_client.invoke(
            FunctionName=self._lambda_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(request_payload),
            LogType='Tail'
        )

        response_log_tail = base64.b64decode(response.get('LogResult'))
        response_payload = json.loads(response.get('Payload').read())
        response_code = response.get('StatusCode')

        log_msg_logs = 'Tail of logs from AWS Lambda:\n{logs}'.format(logs=response_log_tail)
        log_msg_payload = 'Response payload from AWS Lambda:\n{resp}'.format(resp=response_payload)

        if response_code == 200:
            logging.info(log_msg_logs)
            logging.info(log_msg_payload)
            return response_code
        else:
            logging.error(log_msg_logs)
            logging.error(log_msg_payload)
            raise AirflowException('Lambda invoke failed')

    def __create_lambda_payload(self, context):
        payload = self._airflow_context_to_lambda_payload(context)
        payload.update(self._additional_payload)
        return payload