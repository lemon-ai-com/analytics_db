from datetime import date, datetime
import logging
import typing
from clickhouse_driver.errors import ServerException

import pandas as pd

from .connection import Client, add_db_client


class AppsflyerRawDataConnector:
    def __init__(self) -> None:
        self.table_name = "raw_data.appsflyer_raw_data"

    def init_db(self, db_client: Client = None):
        query = """CREATE DATABASE IF NOT EXISTS raw_data"""
        db_client.execute(query)

    @add_db_client
    def are_records_present_for_application_id(
        self,
        application_id: str,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ) -> bool:
        where_parts = [
            "app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_dt:
            where_parts.append("event_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("event_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT count(1) as count
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)}
        """

        df = db_client.query_dataframe(query, where_args)
        return df["count"][0] > 0

    @add_db_client
    def get_number_of_installs(
        self, application_id: str, db_client: Client = None
    ) -> int:
        query = f"""SELECT uniq(appsflyer_id) as uniq
        FROM {self.table_name}
        WHERE app_id = %(application_id)s"""

        df = db_client.query_dataframe(query, {"application_id": application_id})
        return df["uniq"][0]

    @add_db_client
    def get_number_of_events_per_date(
        self,
        application_id: str,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ) -> pd.Series:
        where_parts = [
            "app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_dt:
            where_parts.append("event_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("event_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT toDate(event_time) as event_date, count(1) as number_of_events
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)} AND event_time is not null
        GROUP BY event_date
        """

        df = db_client.query_dataframe(query, where_args)
        return df.set_index("event_date")["number_of_events"]

    @add_db_client
    def get_avg_number_of_events_per_user(
        self,
        application_id: str,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ) -> int:
        where_parts = [
            "app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_dt:
            where_parts.append("event_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("event_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT count(1) / uniq(appsflyer_id) as result
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)}
        """

        df = db_client.query_dataframe(query, where_args)
        return df["result"][0]

    @add_db_client
    def load_raw_data(
        self,
        application_id: str,
        install_dt_from: datetime,
        install_dt_to: datetime,
        max_seconds_from_install: int = None,
        db_client: Client = None,
    ) -> pd.DataFrame:
        where_parts = [
            "app_id = %(application_id)s",
            "install_time >= %(install_dt_from)s",
            "install_time <= %(install_dt_to)s",
        ]
        where_args = {
            "application_id": application_id,
            "install_dt_from": install_dt_from,
            "install_dt_to": install_dt_to,
        }

        if max_seconds_from_install:
            where_parts.append(
                "date_diff('second', install_time, event_time) <= %(max_seconds_from_install)s"
            )
            where_args["max_seconds_from_install"] = max_seconds_from_install

        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)}
        """

        df = db_client.query_dataframe(query, where_args)
        df["user_mmp_id"] = df["appsflyer_id"]
        return df

    @add_db_client
    def calculate_metrics_for_outlier_detection_by_user(
        self,
        application_id: str,
        start_date: date = None,
        end_date: date = None,
        db_client: Client = None,
    ) -> pd.DataFrame:
        where_parts = [
            "app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_date:
            where_parts.append("install_time >= %(start_date)s")
            where_args["start_date"] = start_date

        if end_date:
            where_parts.append("install_time <= %(end_date)s")
            where_args["end_date"] = end_date

        query = f"""
        SELECT appsflyer_id as user_mmp_id, count(1) as number_of_events,
            max(date_diff('second', install_time, event_time)) as max_time_from_install
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)}
        GROUP BY user_mmp_id
        """

        df = db_client.query_dataframe(query, where_args)
        return df

    @add_db_client
    def get_number_of_installs_per_date(
        self,
        application_id: str,
        start_dt: datetime = None,
        end_dt: datetime = None,
        censoring_period_seconds: int = None,
        db_client: Client = None,
    ) -> pd.Series:
        where_parts = [
            "app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_dt:
            where_parts.append("install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        if censoring_period_seconds:
            where_parts.append(
                """
                (is_record_source_pull_api
                    AND date_diff('second', install_time, max_event_time_pull_api)
                        > %(censoring_period_seconds)s)
                OR
                ((is_record_source_push_api OR is_record_source_postback)
                    AND date_diff('second', install_time, max_event_time_push_api)
                        > %(censoring_period_seconds)s)"""
            )
            where_args["censoring_period_seconds"] = censoring_period_seconds

        query = f"""
        WITH (
            SELECT max(event_time) FROM {self.table_name}
                WHERE app_id = %(application_id)s
                    AND is_record_source_pull_api
        ) as max_event_time_pull_api,
        (
            SELECT max(event_time) FROM {self.table_name}
                WHERE app_id = %(application_id)s
                    AND (is_record_source_push_api OR is_record_source_postback)
        ) as max_event_time_push_api

        SELECT toDate(install_time) as install_date, uniq(appsflyer_id) as number_of_installs
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)} AND install_time is not null
        GROUP BY install_date
        """

        df = db_client.query_dataframe(query, where_args)
        return df.set_index("install_date")["number_of_installs"]

    @add_db_client
    def get_number_of_events_per_install_hour(
        self,
        application_id: str,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None,
    ):
        where_parts = [
            "app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_dt:
            where_parts.append("install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append("install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT date_trunc('hour', install_time) as install_hour, count(1) as number_of_events
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)} AND install_time is not null
        GROUP BY install_hour
        """

        df = db_client.query_dataframe(query, where_args)
        return df.set_index("install_hour")["number_of_events"]

    @add_db_client
    def get_number_of_installs_by_install_date(self, application_id: str, db_client: Client=None) -> pd.DataFrame:
        query = f"""
            SELECT count(1) as count, toDate(install_time) as install_date
            FROM {self.table_name}
            WHERE app_id = %(application_id)s AND install_time is not null
            GROUP BY install_date
            """

        df = db_client.query_dataframe(query, {"application_id": application_id})
        return df

    @add_db_client
    def get_avg_number_of_events_per_day(self, application_id: str, db_client: Client) -> float:
        query = f"""
            SELECT count(1) / uniq(toDate(event_time)) as result
            FROM {self.table_name}
            WHERE app_id = %(application_id)s
            """

        df = db_client.query_dataframe(query, {"application_id": application_id})
        return df["result"][0]
    
    @add_db_client
    def get_max_number_of_events_per_day(self, application_id: str, db_client: Client=None) -> float:
        query = f"""
            SELECT max(day_count) as result
            FROM (
                SELECT toDate(event_time) as event_date, count(1) as day_count
                FROM {self.table_name}
                WHERE app_id = %(application_id)s AND event_time is not null
                GROUP BY event_date
            )
            """

        df = db_client.query_dataframe(query, {"application_id": application_id})
        return df["result"][0]

    @add_db_client
    def get_number_of_events_in_date_range(
        self, application_id: str, start_date: datetime, end_date: datetime, db_client: Client=None
    ) -> int:
        query = f"""
            SELECT count(1) as count
            FROM {self.table_name}
            WHERE app_id = %(application_id)s
                AND toDate(install_time) >= toDate(%(start_date)s)
                AND toDate(install_time) <= toDate(%(end_date)s)
        """

        df = db_client.query_dataframe(
            query,
            {
                "application_id": application_id,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        return df["count"][0]

    @add_db_client
    def save_loaded_data(self, df: pd.DataFrame, db_client: Client=None, cb_on_failure=None):
        columns = ", ".join(df.columns)

        query = f"""INSERT INTO {self.table_name} ({columns}) VALUES"""

        try:
            db_client.insert_dataframe(query, df)
        except ServerException as e:
            err_str = str(e)
            if "Code: 241" in err_str:
                logging.error(f"Got Clickhouse memory limit exceeded error for df.shape={df.shape}: {err_str}, will split insert")
                logging.exception(e)
            self.save_loaded_data(df.iloc[: len(df) // 2], db_client=db_client, cb_on_failure=cb_on_failure)
            self.save_loaded_data(df.iloc[len(df) // 2 :], db_client=db_client, cb_on_failure=cb_on_failure)
        except Exception as e:
            logging.error(f"Unknown error while saving data to Clickhouse: {e}")
            logging.exception(e)

            if callable(cb_on_failure):
                cb_on_failure(df)


    @add_db_client
    def count_records_in_table(self, db_client: Client=None):
        query = f"""SELECT count(1) as count FROM {self.table_name}"""

        df = db_client.query_dataframe(query)
        return df["count"][0]
    
    @add_db_client
    def calculate_target_for_app_users(
        self,
        application_id: str,
        target_type: typing.Literal['ltv', 'number_of_conversions', 'lt'],
        target_calculation_period_in_seconds: int,
        convertion_event_names: list[str] = None,
        start_dt: datetime = None,
        end_dt: datetime = None,
        db_client: Client = None
        ) -> pd.Series:
        if target_type in ('ltv', 'number_of_conversions') and not convertion_event_names:
            raise ValueError(f'convertion_event_names must be provided for target_type={target_type}')
        
        where_parts = [
            "app_id = %(application_id)s",
            "date_diff('second', install_time, event_time) <= %(target_calculation_period_in_seconds)s"
        ]
        where_args = {
            "application_id": application_id, 
            "target_calculation_period_in_seconds": target_calculation_period_in_seconds
        }

        if start_dt:
            where_parts.append("install_time >= %(start_dt)s")
            where_args["start_dt"] = start_dt

        if end_dt:
            where_parts.append("install_time <= %(end_dt)s")
            where_args["end_dt"] = end_dt

        if target_type == 'ltv':
            query = f"""
            SELECT appsflyer_id as user_mmp_id, sum(toFloat64OrZero(event_revenue)) as target
            FROM {self.table_name}
            WHERE {' AND '.join(where_parts)} AND event_name IN %(convertion_event_names)s
            GROUP BY user_mmp_id"""
            where_args['convertion_event_names'] = convertion_event_names
        elif target_type == 'number_of_conversions':
            query = f"""
            SELECT appsflyer_id as user_mmp_id, count(1) as target
            FROM {self.table_name}
            WHERE {' AND '.join(where_parts)} AND event_name IN %(convertion_event_names)s
            GROUP BY user_mmp_id"""
            where_args['convertion_event_names'] = convertion_event_names
        elif target_type == 'lt':
            query = f"""
            SELECT appsflyer_id as user_mmp_id, max(date_diff('second', install_time, event_time)) as target
            FROM {self.table_name}
            WHERE {' AND '.join(where_parts)}
            GROUP BY user_mmp_id"""
        else:
            raise ValueError(f'Invalid target_type={target_type}')
        
        df = db_client.query_dataframe(query, where_args)
        return df.set_index('user_mmp_id')['target']

