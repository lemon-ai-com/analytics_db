import uuid
from datetime import datetime

import pandas as pd

from .connection import Client, add_db_client


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
            {', '.join(eventvectors_create_columns)}
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (user_mmp_id, event_number)
        """
        db_client.execute(create_table_query)

        uservectors_columns = ', '.join([f'`{x}`' for x in uservectors.columns])
        db_client.insert_dataframe(
            f"""INSERT INTO {self.table_uservectors} ({uservectors_columns}) VALUES""", uservectors
        )
        eventvectors_columns = ', '.join([f'`{x}`' for x in eventvectors.columns])
        db_client.insert_dataframe(
            f"""INSERT INTO {self.table_eventvectors} ({eventvectors_columns}) VALUES""", eventvectors
        )

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
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        uservectors = db_client.query_dataframe(query, where_args)

        query = f"""
        SELECT *
        FROM {self.table_eventvectors}
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
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
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """
        return db_client.query_dataframe(query).iloc[0, 0]

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
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """
        return db_client.query_dataframe(query).iloc[0, 0]

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
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
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
        {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        return db_client.query_dataframe(query, where_args).iloc[0, 0]

    @add_db_client
    def get_earliest_install_date_with_no_prediction_for_model_id(
        self,
        model_id: uuid.UUID,
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

        query = f"""
        SELECT min(install_time) as result
        FROM {self.table_uservectors}
        WHERE user_mmp_id NOT IN (
            SELECT user_mmp_id
            FROM predict.{model_id}_predict
            {('WHERE' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        )
        {('AND' + ' AND '.join(where_parts)) if len(where_parts) > 0 else ''}
        """

        return db_client.query_dataframe(query, where_args).iloc[0, 0]
