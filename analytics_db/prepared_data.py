import logging
import uuid
from datetime import datetime

import pandas as pd

from .connection import Client, add_db_client
from clickhouse_driver.errors import ServerException



class PreparedDataConnector:
    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.table_uservectors = f"prepared_data.`{self.pipeline_id}_uservectors`"
        self.table_eventvectors = f"prepared_data.`{self.pipeline_id}_eventvectors`"

    @add_db_client
    def init_db(self, db_client: Client = None):
        db_client.execute(
            f"""
            CREATE DATABASE IF NOT EXISTS prepared_data
            """
        )

    def _insert_df_in_chunks_if_needed(self, query: str, df: pd.DataFrame, client: Client, chunk_size: int=None):
        if not chunk_size:
            chunk_size = len(df)

        for i in range(0, len(df), chunk_size):
            try:
                client.insert_dataframe(query, df.iloc[i : min(i + chunk_size, len(df))])
            except ServerException as e:
                err_str = str(e)
                if "Code: 241" in err_str:
                    logging.error(f"Got Clickhouse memory limit exceeded error for chunk_size={chunk_size}, df.shape={df.shape}: {err_str}, will split insert")
                    logging.exception(e)
                self._insert_df_in_chunks_if_needed(query, df.iloc[i:], client, chunk_size=chunk_size // 2)
            except Exception as e:
                logging.error(f"Unknown error while saving data to Clickhouse: {e}")
                logging.exception(e)

    @add_db_client
    def insert_prepared_data(
        self,
        uservectors: pd.DataFrame,
        eventvectors: pd.DataFrame,
        db_client: Client = None,
    ):
        uservectors_create_columns = [
            f"`{x}` Nullable(Float64)" for x in uservectors.columns if x not in ("user_mmp_id", "install_time")
        ]
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {self.table_uservectors} (
            user_mmp_id String,
            install_time DateTime,
            created_at DateTime default now(),
            {', '.join(uservectors_create_columns)}
        ) ENGINE = ReplacingMergeTree()
        ORDER BY user_mmp_id
        """
        db_client.execute(create_table_query)

        eventvectors_create_columns = [
            f"`{x}` Nullable(Float64)"
            for x in eventvectors.columns
            if x not in ("user_mmp_id", "event_number", "install_time")
        ]
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {self.table_eventvectors} (
            user_mmp_id String,
            event_number Int64,
            install_time DateTime,
            created_at DateTime default now(),
            {', '.join(eventvectors_create_columns)}
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (user_mmp_id, event_number)
        """
        db_client.execute(create_table_query)

        uservectors_columns = ', '.join([f'`{x}`' for x in uservectors.columns])
        uservectors_insert_query = f"""INSERT INTO {self.table_uservectors} ({uservectors_columns}) VALUES"""
        self._insert_df_in_chunks_if_needed(uservectors_insert_query, uservectors, db_client)

        eventvectors_columns = ', '.join([f'`{x}`' for x in eventvectors.columns])
        eventvectors_insert_query = f"""INSERT INTO {self.table_eventvectors} ({eventvectors_columns}) VALUES"""
        self._insert_df_in_chunks_if_needed(eventvectors_insert_query, eventvectors, db_client)

    @add_db_client
    def get_prepated_data(
        self,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        where_parts = []
        where_args = {}

        if start_dt:
            where_parts.append("install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT *
        FROM {self.table_uservectors}
        {('WHERE ' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        uservectors = db_client.query_dataframe(query, where_args)

        query = f"""
        SELECT *
        FROM {self.table_eventvectors}
        {('WHERE ' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        eventvectors = db_client.query_dataframe(query, where_args)

        return uservectors, eventvectors

    @add_db_client
    def get_number_of_users(
        self,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ) -> float:
        where_parts = []
        where_args = {}

        if start_dt:
            where_parts.append("install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT uniq(user_mmp_id) as result
        FROM {self.table_eventvectors}
        {('WHERE ' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """
        return db_client.query_dataframe(query, where_args).iloc[0, 0]

    @add_db_client
    def get_number_of_events(
        self,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ) -> float:
        where_parts = []
        where_args = {}

        if start_dt:
            where_parts.append("install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT count(1) as result
        FROM {self.table_eventvectors}
        {('WHERE ' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """
        return db_client.query_dataframe(query, where_args).iloc[0, 0]

    @add_db_client
    def get_number_of_events_per_install_hour_in_prepared_data(
        self,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ):
        where_parts, where_args = [], {}

        if start_dt:
            where_parts.append("install_date >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("install_date <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT date_trunc('hour', install_time) as install_hour, count(1) as number_of_events
        FROM {self.table_eventvectors}
        {('WHERE ' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        GROUP BY install_hour
        """

        df = db_client.query_dataframe(query, where_args)
        return df.set_index("install_hour")["number_of_events"]

    @add_db_client
    def get_number_of_install_dates(
        self,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ) -> float:
        where_parts = []
        where_args = {}

        if start_dt:
            where_parts.append("install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT uniq(toDate(install_time)) as result
        FROM {self.table_uservectors}
        {('WHERE ' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        return db_client.query_dataframe(query, where_args).iloc[0, 0]

    @add_db_client
    def get_earliest_install_date_with_no_prediction(
        self,
        event_id: uuid.UUID | None,
        metric_id: uuid.UUID | None,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ) -> datetime:
        where_parts = []
        where_args = {}

        if start_dt:
            where_parts.append("install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        where_args["event_id"] = str(event_id)
        where_args["metric_id"] = str(metric_id)

        query = f"""
        SELECT min(install_time) as result
        FROM {self.table_uservectors}
        WHERE user_mmp_id NOT IN (
            SELECT DISTINCT user_mmp_id
            FROM predict.event_predict
            WHERE created_at > %(start_date)s AND event_id = %(event_id)s
            UNION ALL
            SELECT DISTINCT user_mmp_id
            FROM predict.metric_predict
            WHERE created_at > %(start_date)s AND metric_id = %(metric_id)s
        )
        {('AND ' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        return db_client.query_dataframe(query, where_args).iloc[0, 0]
    
    @add_db_client
    def get_max_install_time(self, db_client: Client = None) -> datetime:
        query = f"""
        SELECT max(install_time) as result
        FROM {self.table_uservectors}
        """
        return db_client.query_dataframe(query).iloc[0, 0]
