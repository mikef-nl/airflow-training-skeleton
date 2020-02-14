import airflow
from airflow.models import DAG
from operators.http_to_gcs_operator import HttpToGcsOperator

args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(90)}
dag = DAG(
  dag_id="exchange_rates",
  default_args=args,
  description="DAG getting Exchange rates.",
  schedule_interval="0 0 * * *",
)

get_rates = HttpToGcsOperator( 
    http_conn_id = "https_exchange_default",
    endpoint = "/data",
    gcs_bucket = "https://console.cloud.google.com/storage/browser/europe-west1-training-airfl-90de010f-bucket",
    gcs_path = "/data/{{ ds }}",
    task_id='get_rates', 
    dag=dag
)

get_rates
