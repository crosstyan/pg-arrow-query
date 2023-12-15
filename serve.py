from datetime import datetime, timedelta
from enum import Enum, auto
from result import Result, Ok, Err
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union, Literal, Generic, TypeVar
import paho.mqtt as mqtt
import contextlib
import starlette
from starlette.requests import Request
from starlette.applications import Starlette
from starlette.responses import JSONResponse, StreamingResponse, AsyncContentStream, Content, ContentStream, Response
from starlette.websockets import WebSocket, WebSocketDisconnect
from starlette.routing import Route, WebSocketRoute
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.cors import CORSMiddleware
import uvicorn
import logging
from starlette.concurrency import run_until_first_complete, run_in_threadpool
from pathlib import Path
import click
import tomli
import sys
import asyncio
import pyarrow as pa
import pyarrow.ipc as ipc
from utils.db import Config, get_by_sql, to_kv_str, get_arrow_by_sql
import psycopg
from pydantic import BaseModel
from pydantic import ValidationError
from psycopg import AsyncConnection
import io
import ujson
from psycopg_pool import AsyncConnectionPool
from broadcaster import Broadcast, Event

logger = logging.getLogger('uvicorn')
config_path: Optional[Path] = None
pg_conn_pool: Optional[AsyncConnectionPool] = None
broadcast: Optional[Broadcast] = None
# see https://github.com/crosstyan/cborpc
# the transport layer should no intervene the message
# just broadcast
channel_name = "rpc"

# default 131_072
MAX_CHUNK_SIZE = 131_072


# MAX_CHUNK_SIZE = 1024
async def ws_handler(ws: WebSocket):
  await ws.accept()
  # https://github.com/encode/starlette/commit/3f6d4f5969d8c153477c534a31fc50925843f7b0
  # https://github.com/encode/starlette/pull/1443
  # https://github.com/florimondmanca/arel/issues/26
  await run_until_first_complete((ws_receiver, {"websocket": ws}), (ws_sender, {"websocket": ws}))


class EventMessage(BaseModel):
  host: str
  port: int
  message: bytes | str


async def ws_receiver(websocket: WebSocket):
  assert broadcast is not None, "broadcast is not initialized"

  async def iter_any():
    try:
      while True:
        data = await websocket.receive()
        if b := data.get("bytes"):
          yield b
        elif s := data.get("text"):
          yield s
        elif t := data.get("type"):
          if t == "websocket.disconnect":
            break
        else:
          logger.error("unknown message %s", data)
          continue
    except WebSocketDisconnect:
      pass
    except RuntimeError:
      pass

  websocket.iter_bytes()
  async for message in iter_any():
    assert websocket.client is not None
    msg = EventMessage(host=websocket.client.host, port=websocket.client.port, message=message)
    await broadcast.publish(channel=channel_name, message=msg)


async def ws_sender(websocket: WebSocket):
  assert broadcast is not None, "broadcast is not initialized"
  async with broadcast.subscribe(channel=channel_name) as subscriber:
    async for event in subscriber:    # type: ignore
      event: Event
      msg: EventMessage = event.message
      assert isinstance(msg, EventMessage)
      assert websocket.client is not None
      is_same_target = (msg.host == websocket.client.host) and (msg.port == websocket.client.port)
      if not is_same_target:
        if isinstance(msg.message, str):
          await websocket.send_text(msg.message)
        else:
          await websocket.send_bytes(msg.message)
      else:
        pass


@contextlib.asynccontextmanager
async def lifespan(_app: Starlette):
  logger.info("lifespan starts")
  logger.info("pyarrow version %s", pa.__version__)
  global pg_conn_pool
  global broadcast
  assert config_path is not None
  with open(config_path, 'rb') as f:
    config_dict = tomli.load(f)
  config = Config.model_validate(config_dict)
  pg_conn_info = to_kv_str(config.database.model_dump())
  pg_conn_pool = AsyncConnectionPool(conninfo=pg_conn_info)
  broadcast = Broadcast("memory://")
  await broadcast.connect()
  yield
  await broadcast.disconnect()
  await pg_conn_pool.close()
  logger.info("lifespan end")


class QueryBody(BaseModel):
  sql: str


async def handle_query(request: Request):
  # https://www.iana.org/assignments/media-types/application/vnd.apache.arrow.file
  # actually you don't need to use the stupid `BufferOutputStream`
  # just use `io.BytesIO`
  # https://stackoverflow.com/questions/76758084/how-to-send-arrow-data-from-fastapi-to-the-js-apache-arrow-package-without-copyi
  try:
    content_type = request.headers.get("Content-Type")
    query: Optional[str] = None

    if content_type is None or "text/plain" in content_type:
      query = bytes.decode(await request.body(), encoding="utf-8")
    elif "json" in content_type:
      body = (await request.json()).get("query_body")
      query_body = QueryBody.model_validate(body)
      query = query_body.sql

    if query is None:
      raise HTTPException(status_code=400, detail="bad query request")

    if pg_conn_pool is None:
      raise HTTPException(status_code=500, detail="no database connection pool")

    accept = request.headers.get("Accept")

    def get_accept_type():
      default = "json"
      if accept is None:
        return default
      if "apache.arrow" in accept:
        return "arrow"
      elif "application/json" in accept:
        return "json"
      else:
        return default

    async def ret_arrow():
      assert pg_conn_pool is not None
      table: pa.Table
      table = await get_arrow_by_sql(pg_conn_pool, query)

      async def gen():
        with io.BytesIO() as sink:
          writer = pa.ipc.new_stream(sink, table.schema)
          for batch in table.to_batches(max_chunksize=MAX_CHUNK_SIZE):
            writer.write_batch(batch)
            yield sink.getvalue()
            # effectively reset the buffer to empty
            sink.truncate(0)
            sink.seek(0)

      stream = StreamingResponse(gen(), media_type="application/vnd.apache.arrow.file")
      return stream

    async def ret_json():
      assert pg_conn_pool is not None
      column_names: list[str]
      rows: list[tuple[Any, ...]]
      column_names, rows = await get_by_sql(pg_conn_pool, query)
      r = {"names": column_names, "rows": rows}
      def row_datetime_to_iso(row: tuple[Any, ...]) -> tuple[Any, ...]:
        return tuple(map(lambda x: x.isoformat() if isinstance(x, datetime) else x, row))
      r["rows"] = list(map(row_datetime_to_iso, r["rows"]))
      b = ujson.dumps(r)
      return Response(b, media_type="application/json")

    if get_accept_type() == "arrow":
      return await ret_arrow()
    else:
      return await ret_json()
  except HTTPException as e:
    return JSONResponse({"error": e.detail}, status_code=e.status_code)
  except ValidationError as e:
    return JSONResponse({"error": str(e)}, status_code=400)
  except psycopg.DataError as e:
    return JSONResponse({"error": str(e)}, status_code=400)
  except Exception as e:
    return JSONResponse({"error": str(e)}, status_code=500)


middleware = [
      Middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    ),
]
app = Starlette(debug=True,
                routes=[
                    Route('/', lambda _request: JSONResponse({"test": "ok"}), methods=["GET"]),
                    Route("/query", handle_query, methods=["POST"]),
                    WebSocketRoute("/ws", ws_handler, name="ws"),
                ],
                middleware=middleware,
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
