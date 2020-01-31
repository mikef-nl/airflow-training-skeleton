import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'Mike',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise3',
    default_args=args,
    schedule_interval=None
)

def _print_exec_date(**context):
    print(context["execution_date"])

t1 = PythonOperator(
    task_id='print_execution_date',
    python_callable=_print_exec_date,
    provide_context=True,
    dag=dag)

t5 = DummyOperator(task_id='the_end', dag=dag)
wait_periods = ['1','5','10']
for wait_period in wait_periods:
    t1 >> BashOperator(task_id='wait_'+wait_period, bash_command="sleep "+wait_period, dag=dag) >> t5
#t2 = BashOperator(task_id='wait_5', bash_command="sleep 5", dag=dag)
#t3 = BashOperator(task_id='wait_1', bash_command="sleep 1", dag=dag)
#t4 = BashOperator(task_id='wait_10', bash_command="sleep 10", dag=dag)


t1 >> [t2,t3,t4] >> t5
