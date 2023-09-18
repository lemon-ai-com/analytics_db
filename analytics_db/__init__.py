from .adjust import AdjustRawDataConnector
from .appsflyer import AppsflyerRawDataConnector
from .predict import PredictDataConnector
from .prepared_data import PreparedDataConnector

RawDataConnectorType = AppsflyerRawDataConnector | AdjustRawDataConnector


def get_db_connector_for_tracker(tracker: str) -> RawDataConnectorType:
    if tracker.lower() == "appsflyer":
        return AppsflyerRawDataConnector()
    elif tracker.lower() == "adjust":
        return AdjustRawDataConnector()
    else:
        raise NotImplementedError(
            f"connector for tracker {tracker} is not implemented yet"
        )


def get_prepared_data_db_connector(pipeline_id: str) -> PreparedDataConnector:
    return PreparedDataConnector(pipeline_id=pipeline_id)


def get_predict_db_connector(is_event_predict=False, is_metric_predict=False) -> PredictDataConnector:
    return PredictDataConnector(is_event_predict, is_metric_predict)
