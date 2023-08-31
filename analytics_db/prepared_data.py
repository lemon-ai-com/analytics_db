from datetime import datetime
import uuid
import pandas as pd
from .connection import add_db_client, Client


class PreparedDataConnector:
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.table_path_prefix = f"prepared.{self.pipeline_id}"

    @add_db_client
    def insert_prepared_data(self, uservectors: pd.DataFrame, eventvectors: pd.DataFrame, db_client: Client = None):
        uservectors_feature_names = [f'`{x}`' for x in uservectors.columns if x != 'user_mmp_id']
        uservectors_table_name = f"{self.table_path_prefix}_uservectors"
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {uservectors_table_name} (
            user_mmp_id String,
            install_time DateTime,
            {', '.join(uservectors_feature_names)}
        ) ENGINE = ReplacingMergeTree()
        ORDER BY user_mmp_id
        """
        db_client.execute(create_table_query)

        eventvectors_feature_names = [f'`{x}`' for x in eventvectors.columns if x not in ('user_mmp_id', 'event_number', 'install_time')]
        eventvectors_table_name = f"{self.table_path_prefix}_eventvectors"
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {eventvectors_table_name} (
            user_mmp_id String,
            event_number Int64,
            install_time DateTime,
            {', '.join(eventvectors_feature_names)}
        ) ENGINE = ReplacingMergeTree()
        ORDER BY user_mmp_id, event_number
        """
        db_client.execute(create_table_query)

        db_client.insert_dataframe(f"""INSERT INTO {uservectors_table_name} VALUES""", uservectors)
        db_client.insert_dataframe(f"""INSERT INTO {eventvectors_table_name} VALUES""", eventvectors)
    
    @add_db_client
    def get_prepated_data(self, start_dt: datetime = None, end_dt: datetime = None, db_client: Client = None) -> tuple[pd.DataFrame, pd.DataFrame]:
        where_parts = []
        where_args = {}

        if start_dt:
            where_parts.append(f"install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append(f"install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT *
        FROM {self.table_path_prefix}_uservectors
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        uservectors = db_client.query_dataframe(query, where_args)

        query = f"""
        SELECT *
        FROM {self.table_path_prefix}_eventvectors
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        eventvectors = db_client.query_dataframe(query, where_args)

        return uservectors, eventvectors
    
    @add_db_client
    def get_avg_number_of_events_per_user(self, db_client: Client = None) -> float:
        query = f"""
        SELECT count(1) / uniq(user_mmp_id) as result
        FROM {self.table_path_prefix}_eventvectors
        """
        return db_client.query_dataframe(query).iloc[0, 0]
    
    @add_db_client
    def get_avg_number_of_installs_per_date(
        self, start_dt: datetime = None, end_dt: datetime = None, db_client: Client = None
    ) -> float:
        where_parts = []
        where_args = {}

        if start_dt:
            where_parts.append(f"install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append(f"install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT uniq(user_mmp_id) / uniq(toDate(install_time)) as result
        FROM {self.table_path_prefix}_uservectors
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        return db_client.query_dataframe(query, where_args)["result"]
    
