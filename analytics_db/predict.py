import pandas as pd

from .connection import Client, add_db_client


class PredictDataConnector:
    def __init__(self, model_id: str):
        self.model_id = model_id
        self.table_path = f"predict.{self.model_id}_predict"

    @add_db_client
    def save_predicts(self, predicts: pd.DataFrame, db_client: Client = None):
        db_client.insert_dataframe(
            f"""INSERT INTO {self.table_path} (user_id, predict_value) VALUES""", predicts
        )
