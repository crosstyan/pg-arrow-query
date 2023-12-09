from loguru import logger
import psycopg
from psycopg import Connection
from psycopg.sql import SQL
from typing import Dict, Optional, Generator, List, TypedDict, TypeVar, Iterable, Callable, Any, Sequence
from pydantic import BaseModel
from pathlib import Path
import pyarrow as pa
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
import plotly.subplots as sp
from IPython.display import display
import os


class DatabaseConfig(BaseModel):
    dbname: str
    user: str
    password: Optional[str]


class Config(BaseModel):
    database: DatabaseConfig

def to_kv_str(d: Dict[str, str]) -> str:
    """Convert dictionary to key-value string"""
    return " ".join(f"{k}={v}" for k, v in d.items())


def postgres_env_password() -> Optional[str]:
    """Get password from environment variable"""
    return os.environ.get("PGPASSWORD")


async def get_df_by_sql(conn_info: str, sql: str) -> pl.DataFrame:
    """Get dataframe by SQL"""
    async with await psycopg.AsyncConnection.connect(conninfo=conn_info) as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)    # type: ignore
            rows = await cur.fetchall()
            assert cur.description is not None
            column_names = [desc[0] for desc in cur.description]
            return pl.DataFrame(rows, schema=column_names)

async def get_arrow_by_sql(conn_info: str, sql: str) -> pa.Table:
    """Get dataframe by SQL"""
    # https://stackoverflow.com/questions/76758084/how-to-send-arrow-data-from-fastapi-to-the-js-apache-arrow-package-without-copyi
    async with await psycopg.AsyncConnection.connect(conninfo=conn_info) as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)    # type: ignore
            rows = await cur.fetchall()
            assert cur.description is not None
            column_names = [desc[0] for desc in cur.description]
            return pl.DataFrame(rows, schema=column_names).to_arrow()
