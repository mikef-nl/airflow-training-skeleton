from airflow.contrib.operators.postgres_to_gcs_operator import (PostgresToGoogleCloudStorageOperator)
import airflow
from airflow.models import DAG

args = {
    'owner': 'Mike',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='exercis5',
    default_args=args,
    schedule_interval=None
)

t1 = PostgresToGoogleCloudStorageOperator(
  task_id="postgres_to_gcs",
  sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date between (now() - '1 week'::interval) and (now() - '2 weeks'::interval",
  bucket="postgres_export_mfennemore",
  filename="ex_{{ execution_date }}",
  postgres_conn_id="postgres_default",
  google_cloud_storage_conn_id="google_cloud_storage_default",
  dag=dag,)

