
import pandas as pd
from .connection import add_db_client, Client


class PredictDataConnector:
    def __init__(self, model_id: str):
        self.model_id = model_id
        self.table_path = f"predicts.{self.model_id}_predict"

    @add_db_client
    def save_predicts(self, predicts: pd.DataFrame, db_client: Client = None):
        db_client.insert_dataframe(
            f"""INSERT INTO {self.table_path} (user_mmp_id, predict) VALUES""", 
            predicts
        )

    