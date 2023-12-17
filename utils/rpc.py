from datetime import datetime, timedelta
from pydantic import BaseModel
from result import Result, Ok, Err
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union, Literal, Generic, TypeVar
from starlette.websockets import WebSocket, WebSocketDisconnect
import logging
from starlette.concurrency import run_until_first_complete, run_in_threadpool
from broadcaster import Broadcast, Event
from enum import Enum, auto
from pathlib import Path
from .models import EventMessage
import urllib.parse as urlparse
import os

logger = logging.getLogger("uvicorn")
CBORPC_CHANNEL_NAME = "rpc"


# MAX_CHUNK_SIZE = 1024
async def cbor_ws_handler(ws: WebSocket, broadcast: Optional[Broadcast]):
  await ws.accept()
  # https://github.com/encode/starlette/commit/3f6d4f5969d8c153477c534a31fc50925843f7b0
  # https://github.com/encode/starlette/pull/1443
  # https://github.com/florimondmanca/arel/issues/26
  await ws.accept()
  await run_until_first_complete((cbor_ws_receiver, {
      "websocket": ws,
      "broadcast": broadcast
  }), (cbor_ws_sender, {
      "websocket": ws,
      "broadcast": broadcast
  }))


async def cbor_ws_receiver(websocket: WebSocket, broadcast: Optional[Broadcast]):
  assert broadcast is not None, "broadcast is not initialized"

  async def iter_any():
    try:
      while True:
        data = await websocket.receive()
        if b := data.get("bytes"):
          b: bytes | None
          yield b
        elif s := data.get("text"):
          s: str | None
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
    # handled in file manager


async def cbor_ws_sender(websocket: WebSocket, broadcast: Optional[Broadcast]):
  assert broadcast is not None, "broadcast is not initialized"
  async with broadcast.subscribe(channel=CBORPC_CHANNEL_NAME) as subscriber:
    # handled in file manager
    pass

