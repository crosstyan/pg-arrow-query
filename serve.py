from datetime import datetime, timedelta
from enum import Enum, auto
from result import Result, Ok, Err
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union, Literal, Generic, TypeVar
import paho.mqtt as mqtt
# import asyncio
import contextlib
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.websockets import WebSocket
from starlette.routing import Route, WebSocketRoute
import uvicorn
import logging
from starlette.concurrency import run_until_first_complete, run_in_threadpool
from pathlib import Path
import ujson
import click
import tomli
import sys
import asyncio
from utils.db import Config, to_kv_str
import psycopg
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


# https://stackoverflow.com/questions/67599119/fastapi-asynchronous-background-tasks-blocks-other-requests
# https://github.com/python/mypy/issues/4245
app = Starlette(debug=True,
                routes=[
                    Route('/', lambda _request: JSONResponse({"test": "test"})),
                ],
                lifespan=lifespan)    # type: ignore


@click.command()
@click.option('--config', '-c', type=click.Path(exists=True), help='config file path', default='config.toml')
def main(config:str):
  global config_path
  if sys.platform == "win32":
          asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
  config_path = Path(config)
  uvicorn.run(app, host="0.0.0.0", port=8000)    # type: ignore


if __name__ == "__main__":
  main()
