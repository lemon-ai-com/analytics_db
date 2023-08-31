from datetime import date, datetime

import pandas as pd
from .connection import add_db_client, Client


class AppsflyerRawDataConnector:
    def __init__(self) -> None:
        self.table_name = "appsflyer_raw_data"

    @add_db_client
    def get_number_of_installs(self, application_id: str, db_client: Client = None) -> int:
        query = f"""SELECT uniq(appsflyer_id) as uniq FROM {self.table_name} 
        WHERE app_id = %(application_id)s"""

        df = db_client.query_dataframe(query, {"application_id": application_id})
        return df["uniq"][0]
    
    # later will use it in model manager lambda
    # @add_db_client
    # def calculate_target_by_user(self, application: Application, target: TargetBase, db_client: Client = None) -> pd.Series:
    #     where_parts = [
    #         f"app_id = %(application_id)s",
    #     ]
    #     where_args = {"application_id": application.id_in_store}

    #     if target.preset_target == PresetTargetEnum.ltv:
    #         target_sql_calc = """sum(event_revenue)"""
    #         where_parts.append(f"event_name IN %(convertion_event_names)s")
    #         where_args["convertion_event_names"] = application.convertion_event_names
    #     elif target.preset_target == PresetTargetEnum.number_of_conversions:
    #         target_sql_calc = """count(1)"""
    #         where_parts.append(f"event_name IN %(convertion_event_names)s")
    #         where_args["convertion_event_names"] = application.convertion_event_names
    #     elif target.preset_target == PresetTargetEnum.lt:
    #         target_sql_calc = """max(date_diff('second', install_time, event_time))"""
    #     else:
    #         raise NotImplementedError(
    #             f"preset target {target.preset_target} is not implemented"
    #         )
        
    #     if target.preset_target_time_from_install_limit_value and target.preset_target_time_from_install_limit_unit:
    #         preset_target_time_from_install_limit_unit = target.preset_target_time_from_install_limit_unit.value[:-1] # removing 's' from the end to make it compatible with Clickhouse
    #         where_parts.append(f"date_diff('{preset_target_time_from_install_limit_unit}', install_time, event_time) <= %(preset_target_time_from_install_limit_value)s")
    #         where_args["preset_target_time_from_install_limit_value"] = target.preset_target_time_from_install_limit_value
        
    #     query = f"""SELECT appsflyer_id as user_mmp_id, {target_sql_calc} as target
    #     FROM {self.table_name}
    #     WHERE {' AND '.join(where_parts)}"""

    #     df = db_client.query_dataframe(query, where_args)

    #     return df.set_index("user_mmp_id")["target"]

    @add_db_client
    def get_number_of_events_per_date(
        self, application_id: str, start_dt: datetime = None, end_dt: datetime = None, db_client: Client = None
    ) -> pd.Series:
        where_parts = [
            f"app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_dt:
            where_parts.append(f"event_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append(f"event_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        query = f"""
        SELECT toDate(event_time) as event_date, count(1) as number_of_events
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)}
        GROUP BY event_date
        """

        df = db_client.query_dataframe(query, where_args)
        return df.set_index("event_date")["number_of_events"]

    @add_db_client
    def get_number_of_installs_per_date(
        self, application_id: str, start_dt: datetime = None, end_dt: datetime = None, 
        censoring_period_seconds: int = None, db_client: Client = None
    ) -> pd.Series:
        where_parts = [
            f"app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_dt:
            where_parts.append(f"install_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append(f"install_time <= %(end_date)s")
            where_args["end_date"] = end_dt

        if censoring_period_seconds:
            where_parts.append(
                f"""
                (is_record_source_pull_api 
                    AND date_diff('second', install_time, max_event_time_pull_api) > %(censoring_period_seconds)s)
                OR 
                ((is_record_source_push_api OR is_record_source_postback) 
                    AND date_diff('second', install_time, max_event_time_push_api) > %(censoring_period_seconds)s)""")
            where_args["censoring_period_seconds"] = censoring_period_seconds

        query = f"""
        WITH (
            SELECT max(event_time) FROM {self.table_name} WHERE app_id = %(application_id)s AND is_record_source_pull_api
        ) as max_event_time_pull_api,
        (
            SELECT max(event_time) FROM {self.table_name} WHERE app_id = %(application_id)s AND (is_record_source_push_api OR is_record_source_postback)
        ) as max_event_time_push_api

        SELECT toDate(install_time) as install_date, uniq(appsflyer_id) as number_of_installs
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)}
        GROUP BY install_date
        """

        df = db_client.query_dataframe(query, where_args)
        return df.set_index("install_date")["number_of_installs"]

    @add_db_client
    def get_avg_number_of_events_per_user(
        self, application_id: str, start_dt: datetime = None, end_dt: datetime = None, db_client: Client = None
    ) -> int:
        where_parts = [
            f"app_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_dt:
            where_parts.append(f"event_time >= %(start_date)s")
            where_args["start_date"] = start_dt

        if end_dt:
            where_parts.append(f"event_time <= %(end_date)s")
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
        max_seconds_from_install: int = None, db_client: Client = None,
    ) -> pd.DataFrame:
        where_parts = [
            f"application_id = %(application_id)s",
            f"install_time >= %(install_dt_from)s",
            f"install_time <= %(install_dt_to)s",
        ]
        where_args = {
            "application_id": application_id,
            "install_dt_from": install_dt_from,
            "install_dt_to": install_dt_to,
            }

        if max_seconds_from_install:
            where_parts.append(f"date_diff('second', install_time, event_time) <= %(max_seconds_from_install)s")
            where_args["max_seconds_from_install"] = max_seconds_from_install

        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)}
        """

        df = db_client.query_dataframe(query, where_args)
        df['user_mmp_id'] = df['appsflyer_id']
        return df

    @add_db_client
    def calculate_metrics_for_outlier_detection_by_user(
        self, application_id: str, start_date: date = None, end_date: date = None, db_client: Client = None
    ) -> pd.DataFrame:
        where_parts = [
            f"application_id = %(application_id)s",
        ]
        where_args = {"application_id": application_id}

        if start_date:
            where_parts.append(f"install_time >= %(start_date)s")
            where_args["start_date"] = start_date

        if end_date:
            where_parts.append(f"install_time <= %(end_date)s")
            where_args["end_date"] = end_date

        query = f"""
        SELECT appsflyer_id as user_mmp_id, count(1) as number_of_events, max(date_diff('second', install_time, event_time)) as max_time_from_install
        FROM {self.table_name}
        WHERE {' AND '.join(where_parts)}
        GROUP BY user_mmp_id
        """

        df = db_client.query_dataframe(query, where_args)
        return df
    
    @add_db_client
    def insert_prepared_data(self, pipeline_id: str, uservectors: pd.DataFrame, eventvectors: pd.DataFrame, db_client: Client = None):
        uservectors_feature_names = [x for x in uservectors.columns if x != 'user_mmp_id']
        uservectors_table_name = f"{pipeline_id}_uservectors"
        create_table_query = f"""CREATE TABLE IF NOT EXISTS {uservectors_table_name} (
            user_mmp_id String,
            {', '.join(uservectors_feature_names)}
        ) ENGINE = ReplacingMergeTree()
        ORDER BY user_mmp_id
        """
        db_client.execute(create_table_query)

        eventvectors_feature_names = [x for x in eventvectors.columns if x not in ('user_mmp_id', 'event_number', 'install_time')]
        eventvectors_table_name = f"{pipeline_id}_eventvectors"
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

