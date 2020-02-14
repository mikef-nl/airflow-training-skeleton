import airflow
from airflow.operators.base_operator import BaseOperator

class LaunchLibraryOperator(BaseOperator):

    template_fields = ('_start_date','_end_date','_params','_result_path')
    
    @apply_defaults
    def __init__(self, start_date, end_date, endpoint, conn_id, params, result_path, *args, **kwargs):        
        super().__init__(*args, **kwargs)
        self._start_date = start_date
        self._end_date = end_date
        self._endpoint = endpoint
        self.conn_id = conn_id
        self.params = params
        self._result_path = result_path
    
    def execute(self, context):
        hook = LaunchHook()
        hook.download_launches(self.conn_id)
