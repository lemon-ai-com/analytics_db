import pandas as pd

from .connection import Client, add_db_client


class PredictDataConnector:
    def __init__(self, is_event_predict: bool = False, is_metric_predict: bool = False):
        if is_event_predict:
            self.table_path = f"predict.event_predict"
        elif is_metric_predict:
            self.table_path = f"predict.metric_predict"
        else:
            raise NotImplementedError("either is_event_predict or is_metric_predict should be True")

    @add_db_client
    def save_predicts(self, predicts: pd.DataFrame, db_client: Client = None):
        db_client.insert_dataframe(
            f"""INSERT INTO {self.table_path} (user_mmp_id, predict_value) VALUES""", 
            predicts[['user_mmp_id', 'predict_value']]
        )
