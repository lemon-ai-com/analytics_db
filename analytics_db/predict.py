from datetime import date
import datetime
import uuid
import pandas as pd

from .connection import Client, add_db_client
from . import RawDataConnectorType

class PredictDataConnector:
    def __init__(self, is_event_predict: bool = False, is_metric_predict: bool = False, is_sent_event: bool = False):
        if is_event_predict:
            self.table_path = f"predict.event_predict"
        elif is_metric_predict:
            self.table_path = f"predict.metric_predict"
        elif is_sent_event:
            self.table_path = f"predict.sent_event"
        else:
            raise NotImplementedError("either is_event_predict or is_metric_predict or is_sent_event should be True")

    @add_db_client
    def save_predicts(self, predicts: pd.DataFrame, db_client: Client = None):
        columns_str = ", ".join(predicts.columns)

        db_client.insert_dataframe(
            f"""INSERT INTO {self.table_path} ({columns_str}) VALUES""", 
            predicts
        )

    @add_db_client
    def get_event_statictics(
        self, 
        event_id: uuid.UUID, 
        application_id_in_store: str,
        target_type: str,
        target_calculation_period_in_seconds: int,
        convertion_event_names: list[str],
        start_date: date, 
        end_date: date, 
        target_value_from: float,
        target_value_to: float,
        raw_data_db_connector: RawDataConnectorType,
        db_client: Client = None
    ) -> pd.DataFrame:
        
        start_dt = datetime(start_date.year, start_date.month, start_date.day)
        end_dt = datetime(end_date.year, end_date.month, end_date.day)
        
        target_calc_query = raw_data_db_connector.create_query_to_calculate_target(
            application_id=application_id_in_store,
            target_type=target_type,
            target_calculation_period_in_seconds=target_calculation_period_in_seconds,
            convertion_event_names=convertion_event_names,
            start_dt=start_dt,
            end_dt=end_dt,
        )

        target_calc_where_parts = []
        target_calc_where_args = {}

        if target_value_from:
            target_calc_where_parts.append("target >= %(target_value_from)s")
            target_calc_where_args["target_value_from"] = target_value_from

        if target_value_to:
            target_calc_where_parts.append("target <= %(target_value_to)s")
            target_calc_where_args["target_value_to"] = target_value_to
        
        query = f"""
        WITH 
            target AS (
                SELECT
                    toDate(install_time) AS install_date,
                    count(1) as total_users,
                    sum(and({', '.join(target_calc_where_parts) if len(target_calc_where_parts) > 0 else '1'})) as number_of_target_users
                FROM ({target_calc_query})
                GROUP BY install_date
                ),
            sent AS (
                SELECT 
                    toDate(install_time) AS install_date,
                    count(1) AS number_of_events,
                    uniq(user_mmp_id) AS number_of_predict_users
                FROM {self.table_path}
                WHERE event_id = %(event_id)s
                AND install_time >= %(start_date)s
                AND install_time <= %(end_date)s
                GROUP BY install_date
            )
        SELECT target.install_date as install_date, total_users, number_of_target_users, number_of_events, number_of_predict_users
        FROM target LEFT JOIN sent USING (install_date)
        ORDER BY install_date
        """

        return db_client.query_dataframe(query, {
            "event_id": event_id,
            "start_date": start_date,
            "end_date": end_date,
            **target_calc_where_args
        })