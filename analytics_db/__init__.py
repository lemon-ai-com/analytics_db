from .adjust import AdjustRawDataConnector
from .appsflyer import AppsflyerRawDataConnector

__version__ = '0.1.0'

RawDataConnectorType = AppsflyerRawDataConnector | AdjustRawDataConnector

def get_db_connector_for_tracker(tracker: str) -> RawDataConnectorType:
    if tracker.lower() == 'appsflyer':
        return AppsflyerRawDataConnector()
    elif tracker.lower() == 'adjust':
        return AdjustRawDataConnector()
    else:
        raise NotImplemented(f'connector for tracker {tracker} is not implemented yet')
