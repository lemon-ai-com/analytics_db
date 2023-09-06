from .prepared_data import PreparedDataConnector
from .adjust import AdjustRawDataConnector
from .appsflyer import AppsflyerRawDataConnector
from .predict import PredictDataConnector


RawDataConnectorType = AppsflyerRawDataConnector | AdjustRawDataConnector

def get_db_connector_for_tracker(tracker: str) -> RawDataConnectorType:
    if tracker.lower() == 'appsflyer':
        return AppsflyerRawDataConnector()
    elif tracker.lower() == 'adjust':
        return AdjustRawDataConnector()
    else:
        raise NotImplemented(f'connector for tracker {tracker} is not implemented yet')
    
def get_prepared_data_db_connector(pipeline_id: str) -> PreparedDataConnector:
    return PreparedDataConnector(pipeline_id=pipeline_id)

def get_predict_db_connector(model_id: str) -> PredictDataConnector:
    return PredictDataConnector(model_id=model_id)