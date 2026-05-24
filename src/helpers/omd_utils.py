from src.configs.settings import settings
from metadata.ingestion.source.pipeline.airflow.lineage_parser import OMEntity

def load_inlets_outlets(input_list):
    
    result_list = []
    
    for inputs in input_list:
      result_list.append(OMEntity(entity=inputs['entity'], fqn=inputs['fqn'], key=inputs['key']))
    
    return result_list

    
