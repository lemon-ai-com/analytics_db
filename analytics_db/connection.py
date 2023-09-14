from clickhouse_driver import Client

from .config import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def add_db_client(func):
    def wrapper(*args, **kwargs):
        if kwargs.get("db_client") is not None:
            return func(*args, **kwargs)
        else:
            with Client(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                compression='zstd',
                secure=True if DB_PORT == 9440 else False,
                settings={"use_numpy": True},
                connect_timeout=60*5,
            ) as client:
                kwargs["db_client"] = client
                return func(*args, **kwargs)

    return wrapper
