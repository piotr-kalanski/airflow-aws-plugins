* SSH to EC2 instance with airflow

* Run below commands:


    export AIRFLOW_HOME=/home/airflow
    cd airflow_virtualenv/bin
    source activate
    
* List dags:

    
    airflow list_dags
    
* List tasks in dag with Lambda:

    
    airflow list_tasks dag_with_execute_lambda_operator
    
* Run tasks with lambda:    

    
    airflow test dag_with_execute_lambda_operator task_with_execute_lambda_operator 2018-08-10
    
