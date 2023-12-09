from datetime import datetime, timedelta
from enum import Enum, auto
from result import Result, Ok, Err
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union, Literal, Generic, TypeVar
import paho.mqtt as mqtt
# import asyncio
import contextlib
import starlette
from starlette.requests import Request
from starlette.applications import Starlette
from starlette.responses import JSONResponse, StreamingResponse, AsyncContentStream, Content, ContentStream, Response
from starlette.websockets import WebSocket
from starlette.routing import Route, WebSocketRoute
from starlette.exceptions import HTTPException
import uvicorn
import logging
from starlette.concurrency import run_until_first_complete, run_in_threadpool
from pathlib import Path
import ujson
import click
import tomli
import sys
import asyncio
import pyarrow as pa
import pyarrow.ipc as ipc
from utils.db import Config, to_kv_str, get_arrow_by_sql
import psycopg
from pydantic import BaseModel
from pydantic import ValidationError
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger('uvicorn')
config_path: Optional[Path] = None
pg_conn_pool: Optional[AsyncConnectionPool] = None


@contextlib.asynccontextmanager
async def lifespan(_app: Starlette):
  logger.info("lifespan starts")
  global pg_conn_pool
  assert config_path is not None
  with open(config_path, 'rb') as f:
    config_dict = tomli.load(f)
  config = Config.model_validate(config_dict)
  pg_conn_info = to_kv_str(config.database.model_dump())
  pg_conn_pool = AsyncConnectionPool(conninfo=pg_conn_info)
  yield
  await pg_conn_pool.close()
  logger.info("lifespan end")


class QueryBody(BaseModel):
  sql: str


async def handle_query(request: Request):
  # https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.file
  # https://stackoverflow.com/questions/76758084/how-to-send-arrow-data-from-fastapi-to-the-js-apache-arrow-package-without-copyi
  try:
    content_type = request.headers.get("Content-Type")
    query: Optional[str] = None
    if content_type is None or "text/plain" in content_type:
      query = bytes.decode(await request.body(), encoding="utf-8")
    elif "json" in content_type:
      body = await request.json()
      query_body = QueryBody.model_validate(body)
      query = query_body.sql

    if query is None:
      raise HTTPException(status_code=400, detail="bad query request")

    if pg_conn_pool is None:
      raise HTTPException(status_code=500, detail="no database connection pool")

    table: pa.Table
    table = await get_arrow_by_sql(pg_conn_pool, query)
    sink = pa.BufferOutputStream()

    async def gen():
      with ipc.new_stream(sink, table.schema) as writer:
        for batch in table.to_batches():
          writer.write_batch(batch)
          yield sink.getvalue()
          sink.clear()

    stream = StreamingResponse(gen(), media_type="application/vnd.apache.arrow.file")
    return stream
  except HTTPException as e:
    return JSONResponse({"error": e.detail}, status_code=e.status_code)
  except ValidationError as e:
    return JSONResponse({"error": str(e)}, status_code=400)
  except psycopg.DataError as e:
    return JSONResponse({"error": str(e)}, status_code=400)
  except Exception as e:
    return JSONResponse({"error": str(e)}, status_code=500)


app = Starlette(debug=True,
                routes=[
                    Route('/', lambda _request: JSONResponse({"test": "ok"}), methods=["GET"]),
                    Route("query", handle_query, methods=["POST"])
                ],
                lifespan=lifespan)    # type: ignore


@click.command()
@click.option('--config',
              '-c',
              type=click.Path(exists=True),
              help='config file path',
              default='config.toml')
def main(config: str):
  global config_path
  if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
  config_path = Path(config)
  uvicorn.run(app, host="0.0.0.0", port=8000)    # type: ignore


if __name__ == "__main__":
  main()
