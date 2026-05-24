gauge:str = "Gauge"

class SingletonMeta(type):
    _instances = {}

    def __call__(self, *args, **kwds):
        if self not in self._instances:
            self._instances[self] = super().__call__(*args, **kwds)
        return self._instances[self]


class MetricUtils(metaclass=SingletonMeta):
    def __init__(self):
        self.metrics = {}
        
    def create_gauge_metrics(self, df_name, df_process_duration, df_total_records, process_time):
        if gauge not in self.metrics:
            self.metrics[gauge] = []
            
        self.metrics[gauge].append({
            "grouping_key": df_name,
            "metrics_data": {
                "name": df_name,
                "duration": df_process_duration,
                "rows": df_total_records,
                "processTime": f"{process_time}"
            }
        })
    
    def get_all_metrics(self):
        return self.metrics
