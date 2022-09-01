from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
# import logging

from airflow.utils import timezone


default_args = {
    'owner': 'airflow',
}

with DAG (
    'my_1st_dag',
    schedule_interval='*/5 * * * *',
    default_args=default_args,
    start_date=timezone.datetime(2021, 7, 9),
    tags = ['ETL','Hello World'],
    catchup=False,
) as dag :

    t1 = DummyOperator(
        task_id='my_1st_task',
        dag=dag,
    )

    t2 = DummyOperator(
        task_id='my_2nd_task',
        dag=dag,
    )

    def verygood():
        print("Good job")
        return 'End.'

    t3 = PythonOperator(
        task_id='say_verygood',
        python_callable=verygood,
        dag=dag
    )

    t4 = BashOperator(
        task_id='t4-echo',
        bash_command='echo hello',
        dag=dag
    )

    def hello():
        return 'Hello, An and Ant'

    say_hello = PythonOperator(
        task_id='say_hello',
        python_callable=hello,
        dag=dag
    )

    def print_log_messages():
        logging.debug('This is a debug message')
        logging.info('This is an info message')
        logging.warning('This is a warning message')
        logging.error('This is an error message')
        logging.critical('This is a critical message')

        return 'Whatever is returned also gets printed in the logs'

    run_this = PythonOperator(
        task_id='print_log_messages',
        python_callable=print_log_messages,
        dag=dag,
    )

t1 >> [t2 ,t3] >> t4