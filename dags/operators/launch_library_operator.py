import airflow
from airflow.models import BaseOperator
from airflow import AirflowException
from hooks.launch_hook import LaunchHook
from airflow.utils.decorators import apply_defaults

class LaunchLibraryOperator(BaseOperator):

    template_fields = ('_params','_result_path')
    
    @apply_defaults
    def __init__(self, endpoint, conn_id, params, result_path, *args, **kwargs):        
        super().__init__(*args, **kwargs)
        self._endpoint = endpoint
        self.conn_id = conn_id
        self.params = params
        self._result_path = result_path
    
    def execute(self, context):
        hook = LaunchHook(self.conn_id)
        hook.download_launches(self.conn_id)
