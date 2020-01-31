import airflow
import datetime
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Mike',
    'start_date': datetime.datetime(2020, 1, 31),
}

dag = DAG(
    dag_id='exercise2',
    default_args=args,
    schedule_interval="@daily",
)

t1 = DummyOperator(task_id='task1', dag=dag)
t2 = DummyOperator(task_id='task2', dag=dag)
t3 = DummyOperator(task_id='task3', dag=dag)
t4 = DummyOperator(task_id='task4', dag=dag)
t5 = DummyOperator(task_id='task5', dag=dag)

t1 >> t2 >> [t3,t4] >> t5
