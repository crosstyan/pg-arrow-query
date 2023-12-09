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
from utils.oids import *
from psycopg_pool import AsyncConnectionPool
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


async def get_df_by_sql(pool: AsyncConnectionPool, sql: str) -> pl.DataFrame:
  """Get dataframe by SQL"""
  async with pool.connection() as conn:
    async with conn.cursor() as cur:
      await cur.execute(sql)    # type: ignore
      rows = await cur.fetchall()
      assert cur.description is not None
      column_names = [desc[0] for desc in cur.description]
      return pl.DataFrame(rows, schema=column_names)


def oid_to_arrow(oid: int) -> Optional[pa.DataType]:
  """Convert PostgreSQL OID to pyarrow DataType"""
  if oid == INT2_OID:
    return pa.int16()
  elif oid == INT4_OID:
    return pa.int32()
  elif oid == INT8_OID:
    return pa.int64()
  elif oid == NUMERIC_OID:
    return pa.float64()
  elif oid == FLOAT4_OID:
    return pa.float32()
  elif oid == FLOAT8_OID:
    return pa.float64()
  elif oid == BOOL_OID:
    return pa.bool_()
  elif oid == CHAR_OID:
    return pa.int8()
  elif oid == VARCHAR_OID:
    return pa.string()
  elif oid == TEXT_OID:
    return pa.string()
  elif oid == DATE_OID:
    return pa.date32()
  elif oid == TIME_OID:
    return pa.time64("us")
  elif oid == TIMETZ_OID:
    return pa.time64("us")
  elif oid == TIMESTAMP_OID:
    return pa.timestamp("us")
  elif oid == TIMESTAMPTZ_OID:
    return pa.timestamp("us")
  elif oid == BYTEA_OID:
    return pa.binary()
  else:
    logger.warning(f"Unknown OID: {oid}")
    return None


async def get_arrow_by_sql(pool: AsyncConnectionPool, sql: str) -> pa.Table:
  async with pool.connection() as conn:
    async with conn.cursor() as cur:
      await cur.execute(sql)    # type: ignore
      rows = await cur.fetchall()
      assert cur.description is not None
      # https://www.psycopg.org/psycopg3/docs/api/objects.html#the-description-column-object
      # https://peps.python.org/pep-0249/#type-objects-and-constructors
      # https://peps.python.org/pep-0249/#description
      column_names = [desc[0] for desc in cur.description]
      column_types = [oid_to_arrow(desc[1]) for desc in cur.description]
      assert all(t is not None for t in column_types), f"bad type for {cur.description}"
      # https://stackoverflow.com/questions/57939092/fastest-way-to-construct-pyarrow-table-row-by-row
      schema_dict = zip(column_names, column_types)
      schema = pa.schema(schema_dict)
      pat = pa.Table.from_pydict(dict(zip(column_names, zip(*rows))), schema=schema)
      return pat
