from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

dag =  DAG(
    'cuongg',
    default_args={
        'email': ['cuong@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='A simple DAG sample by ThangNC',
    schedule_interval="@once",
    start_date=datetime(2022, 6, 10), # Start date
    tags=['cuongdv'],
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date > /home/cuong/date.txt',
    dag = dag
)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag = dag
)
t3 = BashOperator(
    task_id='echo',
    bash_command='echo t3 running',
    dag = dag
)

[t1 , t2] >> t3