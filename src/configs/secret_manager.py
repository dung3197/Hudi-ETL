from src.configs.settings import settings
import requests, logging
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import RetryError, RequestException
from functools import lru_cache

class SecretManager():
    def __init__(self, app_id:str, query:str, object_name:str):
        self.app_id = app_id
        self.query = query
        self.object_name = object_name
                
        self.secret_manager_url = settings.sm_url
        self.default_s3_conn = {"s3_url": f"{settings.s3_url}:{settings.s3_port}", "s3_access_key": settings.s3_access_key, "s3_secret_key": settings.s3_secret_key}
        self.error_codes = (201, 204, 400, 401, 403, 404, 409, 429, 500, 501)
        
    def sessions(self):
        session = requests.Session()
        retry = Retry(total=5, 
                     read=5, 
                     connect=5,
                     backoff_factor=0.3,
                     status_forcelist=self.error_codes
                     )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session
    
    def gen_params(self):
        return {"AppID":self.app_id, "Query":f"Safe={self.query};Object={self.object_name}"}
    
    def get_s3_conn(self) -> dict:
        params = self.gen_params()
        session = self.sessions()
        
        try:
            response = (session.get(url=self.secret_manager_url, params=params)).json()
            # print(response.json())
            return {"s3_url": f"{response['Address']}:{settings.s3_port}", "s3_access_key": response["UserName"], "s3_secret_key": response["Content"]}
        except RetryError as err:
            logging.error(f"Retry Error: {err}")
        except RequestException as err:
            logging.error(f"Unexpected request error: {err}")
            
        return self.default_s3_conn
    
@lru_cache(maxsize=128)
def get_s3_conn() -> dict:
    sm = SecretManager(app_id=settings.sm_app_id, 
                       query=settings.sm_query, 
                       object_name=settings.sm_object_name)
    conn = sm.get_s3_conn()
    return conn