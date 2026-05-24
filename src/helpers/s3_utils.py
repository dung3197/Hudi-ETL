import re
import logging
from datetime import datetime, timedelta
from minio import Minio
from src.configs.settings import settings

class S3Utils():
    
    def __init__(self, s3_client: Minio):
        self.s3_client = s3_client
    
    def list_objects(self, bucket_name, prefix=None, recursive=False, regex:str=None):
        try:
            objects = self.s3_client.list_objects(bucket_name, prefix, recursive)
            list_objects = list(object.object_name for object in objects)
            if regex is not None:
                r = re.compile(f"{prefix}_{regex}")
                list_object_filter = list(filter(r.match, list_objects))
                return list_object_filter
            else:
                return list_objects
        except Exception as e:
            logging.error(e)
            
    def list_objects_by_range(self, bucket_name, prefix=None, from_time:datetime=None, to_time:datetime=None):
        """_summary_ List ra tất cả objects trong bucket theo khoảng thời gian

        Args:
            bucket_name (_type_): tên bucket
            prefix (_type_, optional): prefix. Defaults to None.
            from_time (datetime, optional): last_modified từ thời gian. Defaults to None.
            to_time (datetime, optional): last_modified đến thời gian. Defaults to None.

        Returns:
            _type_: list path của objects
        """
        
        try:
            objects = []
            
            if from_time and to_time:
                
                if to_time <= from_time:
                    return objects
                
                from_date = from_time.replace(hour=0,minute=0,second=0,microsecond=0)
                to_date = to_time.replace(hour=0,minute=0,second=0,microsecond=0)
                
                curr_date = from_date
                while curr_date <= to_date:
                    date_objects = list(self.s3_client.list_objects(bucket_name, f"{prefix}/{curr_date.strftime(settings.date_format)}/", True))
                    for object in date_objects:
                        objects.append(object)
                        
                    curr_date += timedelta(days=1)
            else:
                objects = list(self.s3_client.list_objects(bucket_name, f"{prefix}/", True))
                
            #sort by last modified
            objects = sorted(objects, key=lambda obj:obj.last_modified)
            
            list_objects = []
            for object in objects:
                if from_time and object.last_modified.astimezone(settings.timezone) < from_time:
                    continue
                if to_time and object.last_modified.astimezone(settings.timezone) > to_time:
                    continue
                list_objects.append(object.object_name)
            return list_objects
        
        except Exception as e:
            logging.error(e)
            return objects