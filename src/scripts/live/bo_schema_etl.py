from src.helpers.dag_utils import all_processors
from src.scripts.live.bo_table_etl_base import BOETLBase
from src.scripts.schema_etl_base import SchemaETLBase

class BORawProcess(SchemaETLBase):
    def __init__(self, processors=None):
        if processors is None:
            processors = all_processors(BOETLBase)
        super().__init__("ETL_BO_RAW",processors)

