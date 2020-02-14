import airflow
from airflow.models import DAG
from dags.operators.launch_library_operator import LaunchLibraryOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(10)}
dag = DAG(
  dag_id="download_rocket_launches",
  default_args=args,
  description="DAG downloading rocket launches from Launch Library.",
  schedule_interval="0 0 * * *",
)

download_rocket_launches = LaunchLibraryOperator(
  task_id="download",
  conn_id="launchlibrary",
  endpoint="launch",
  params={"startdate": "{{ ds }}", "enddate": "{{ tomorrow_ds }}"},
  result_path=".",
  dag=dag,
)

download_rocket_launches
