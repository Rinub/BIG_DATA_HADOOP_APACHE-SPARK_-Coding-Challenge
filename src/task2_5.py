
from task2_2 import agg_df
from task2_3 import avg_df
from task2_4 import capacity

# air flow dag commands

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'pyspark_airflow_pipeline',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 11),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}


dag = DAG(
    'parallel_task_excution',
    default_args=default_args,
    description='Pyspark running tasks in parallel',
    schedule_interval=timedelta(days=1),
)
# task_1 = DummyOperator(task_id='task_1', dag=dag)
# task_2 = DummyOperator(task_id='task_2', dag=dag)
# task_3 = DummyOperator(task_id='task_3', dag=dag)

task_1 = BashOperator(
    task_id='task_1',
    bash_command='python task2_1.py',
    dag=dag
)

task_2 = BashOperator(
    task_id='task_2',
    bash_command='python task2_2.py',
    dag=dag
)

task_3 = BashOperator(
    task_id='task_3',
    bash_command='python task2_3.py',
    dag=dag
)
task_4 = DummyOperator(task_id='task_2', dag=dag)
task_5 = DummyOperator(task_id='task_3', dag=dag)
task_6 = DummyOperator(task_id='task_3', dag=dag)


group_1 = [task_2, task_3]
group_2 = [task_4, task_5, task_6]

# set up task dependencies

task_1.set_downstream(group_1)

task_1.set_downstream(group_2)

for task in group_1:
    task.set_upstream(task_1)
    
for task in group_2:
    task.set_upstream(task_1)
