import requests
from airflow.hooks.base_hook import BaseHook

class LaunchHook(BaseHook):

   BaseHook.get_connection('launch_library_default').host
   def __init__(self, conn_id):
       super().__init__(source=None)
       self._conn_id = conn_id
       self._conn = None

   def get_conn(self):
      if self._conn is None:
         self._conn = object()
      return self._conn
       
   def _download_launches(ds, tomorrow_ds, **_):
      query = f"https://launchlibrary.net/1.4/launch?startdate={ds}&enddate={tomorrow_ds}"
      result_path = f"/tmp/rocket_launches/ds={ds}"
      pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
      response = requests.get(query)
      print(f"response was {response}")
