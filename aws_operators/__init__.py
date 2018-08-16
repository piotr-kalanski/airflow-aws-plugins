from airflow.plugins_manager import AirflowPlugin
from aws_operators.operators.custom_operators import ExecuteRedshiftQueryOperator


class AWSOperatorsPlugin(AirflowPlugin):
    name = "AWS operators plugin"
    operators = [ExecuteRedshiftQueryOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
