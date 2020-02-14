import airflow
import calendar
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

args = {
    'owner': 'Mike',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercise3',
    default_args=args,
    schedule_interval=None
)

weekday_person_to_email = {
    0: "Bob",
    1: "Joe",
    2: "Alice",
    3: "Joe",
    4: "Alice",
    5: "Alice",
    6: "Alice"
}

def _print_weekday(execution_date, **context):
    return execution_date.strftime("%a")

def _email_contact(execution_date, **context):
    day_number=calendar.weekday(context["ds_nodash"][0:3],context["ds_nodash"][4:5],context["ds_nodash"][6:7])
    return ("email_"+weekday_person_to_email[day_number])

t1 = PythonOperator(
    task_id='print_weekday',
    python_callable=_print_weekday,
    provide_context=True,
    dag=dag)

branching = BranchPythonOperator(
    task_id="branching", 
    python_callable=_email_contact, 
    provide_context=True, 
    dag=dag)

t5 = BashOperator(task_id='the_end', dag=dag)
people = set(weekday_person_to_email.values)
for person in people:
    t1 >> branching >> (task_id='email_'+person, dag=dag) >> t5
