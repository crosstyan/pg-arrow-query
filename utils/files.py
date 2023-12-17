from datetime import datetime, timedelta
from pydantic import BaseModel
from result import Result, Ok, Err
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union, Literal, Generic, TypeVar, Sequence
from starlette.websockets import WebSocket, WebSocketDisconnect
import logging
from starlette.concurrency import run_until_first_complete, run_in_threadpool
from broadcaster import Broadcast, Event
from enum import Enum, auto
from pathlib import Path
from .models import EventMessage, FileConfig, FileError, FileErrorCode
import urllib.parse as urlparse
import os
import cbor2
from pathlib import PureWindowsPath

logger = logging.getLogger('uvicorn')
FILE_CHANNEL_NAME = "file"


# TODO: sort by, offset, limit, ascend/descend
# TODO: filter (blacklist/whitelist)
class FileRequest(BaseModel):
  sid: int
  path: str
  # will stream all files in the folder
  implicit_read: bool


def sanitize_path(path: str):
  """
    Sanitize a path against directory traversals
    
    Based on https://stackoverflow.com/questions/13939120/sanitizing-a-file-path-in-python.
    >>> sanitize_path('../test')
    'test'
    >>> sanitize_path('../../test')
    'test'
    >>> sanitize_path('../../abc/../test')
    'test'
    >>> sanitize_path('../../abc/../test/fixtures')
    'test/fixtures'
    >>> sanitize_path('../../abc/../.test/fixtures')
    '.test/fixtures'
    >>> sanitize_path('/test/foo')
    'test/foo'
    >>> sanitize_path('./test/bar')
    'test/bar'
    >>> sanitize_path('.test/baz')
    '.test/baz'
    >>> sanitize_path('qux')
    'qux'
    >>> sanitize_path("")
    "."
    >>> sanitize_path("/")
    "."
    """
  # - pretending to chroot to the current directory
  # - cancelling all redundant paths (/.. = /)
  # - making the path relative
  return os.path.relpath(os.path.join("/", path), "/")


def to_relative_url(root: str, path: str) -> str:
  """
  Convert an absolute URL path to a relative path based on the specified root.

  Parameters:
  - root (str): The root path from which to calculate the relative path.
  - path (str): The absolute URL whose path component is to be converted.

  Returns:
  - str: The relative path from the root to the path's URL.

  Example:
  >>> to_relative_url('/home/user/docs', 'http://example.com/home/user/docs/report.pdf')
  'report.pdf'
  """
  url = urlparse.urlparse(path)
  rpath = url.path
  return os.path.relpath(rpath, root)


class FileManager:
  # like {"/", "C:\Users\cross\Desktop\code\Kohaku-NAI\data"}
  # of course the result will be sanitized to prevent
  # going out of the root
  _pathes: Dict[str, Path] = {}

  @staticmethod
  def from_config(config: Sequence[FileConfig]) -> "FileManager":
    pathes = {}
    for c in config:
      pathes[c.key] = c.path
    return FileManager(pathes)

  def __init__(self, pathes: Dict[str, str]) -> None:
    for k, v in pathes.items():
      self.add_root(k, v)

  def add_root(self, root: str, path: str | Path) -> None:
    p = Path(path)
    if not p.exists():
      raise RuntimeError(f"path {path} not exists for {root}")
    if not p.is_dir():
      raise RuntimeError(f"path {path} is not a directory for {root}")
    sk = sanitize_path(root)
    if sk in self._pathes:
      raise RuntimeError(f"key {root} already exists. (Unsanitized: {root})")
    self._pathes[sk] = p

  def access(self, path: str) -> Result[Path, FileError]:
    try:
      sp = sanitize_path(path)
      p = Path(sp)
      # the parts of Path(".") is empty
      # idk why
      root = p.parts[0].strip() if len(p.parts) > 0 else "."
      target_path = self._pathes.get(root)
      rest:Path
      if target_path is None:
        root_path = self._pathes.get(".")
        if root_path is None:
          return Err(FileError(code=FileErrorCode.FILE_NOT_FOUND, message=f"key {root} not found"))
        rest = Path(root_path).joinpath(*p.parts)
      else:
        rest = Path(target_path).joinpath(*p.parts[1:])
      if not rest.exists():
        return Err(FileError(code=FileErrorCode.FILE_NOT_FOUND, message=f"path {rest} not found"))
      return Ok(rest)
    except RuntimeError as e:
      return Err(FileError(code=FileErrorCode.RUNTIME_ERROR, message=str(e), extra=e))
    except IndexError as e:
      return Err(FileError(code=FileErrorCode.RUNTIME_ERROR, message=str(e), extra=e))


def decode_request(cbor_bytes: bytes):
  # Content-Type: application/cbor
  # schema
  # [int, str ,        bool]
  #  sid, path, read_folder
  #
  # sid is the session id
  # if read_folder is true, then if the path
  # is a folder, it reads every files in the folder
  try:
    buf = cbor2.loads(cbor_bytes)
    if not isinstance(buf, list):
      raise ValueError("request must be a list")
    id = buf[0]
    if not isinstance(id, int):
      raise ValueError("id must be an integer")
    s = buf[1]
    if not isinstance(s, str):
      raise ValueError("path must be a string")
    r = buf[2]
    if not isinstance(r, bool):
      raise ValueError("implicit_read must be a boolean")
    return Ok(FileRequest(sid=id, path=s, implicit_read=r))
  except ValueError as e:
    return Err(FileError(code=FileErrorCode.RUNTIME_ERROR, message=str(e), extra=e))
  except IndexError as e:
    return Err(FileError(code=FileErrorCode.RUNTIME_ERROR, message=str(e), extra=e))


class FileResponse(BaseModel):
  sid: int
  request_path: str
  file_path: str
  error: Optional[FileError] = None
  # filename, is_dir
  filenames: Optional[List[Tuple[str, bool]]] = None
  content: Optional[bytes] = None


def encode_response(resp: FileResponse) -> bytes:
  # TODO: eof
  r = []
  r.append(resp.sid)
  r.append(resp.request_path)
  r.append(resp.file_path)
  if resp.error is not None:
    e = []
    e.append(resp.error.code.value)
    e.append(resp.error.message)
    r.append(e)
    r.append(None)
    r.append(None)
    return cbor2.dumps(r)
  else:
    if resp.filenames is not None:
      r.append(None)
      r.append(resp.filenames)
      r.append(None)
    else:
      r.append(None)
      r.append(None)
      r.append(resp.content)
    return cbor2.dumps(r)


async def file_ws_receiver(websocket: WebSocket, broadcast: Optional[Broadcast],
                           fm: Optional[FileManager]):
  assert broadcast is not None, "broadcast is not initialized"
  assert fm is not None, "file manager is not initialized"

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
    if not isinstance(msg.message, bytes):
      logger.error("message should be bytes; get %s (%s)", msg, type(msg.message))
      continue
    req = decode_request(msg.message)
    if req.is_err():
      await websocket.send_json(req.unwrap_err())
      continue
    req = req.unwrap()
    p = fm.access(req.path)
    if p.is_err():
      resp = FileResponse(sid=req.sid,
                          request_path=req.path,
                          file_path=req.path,
                          error=p.unwrap_err())
      await websocket.send_bytes(encode_response(resp))
      continue
    if p.unwrap().is_dir():
      pd = p.unwrap()
      if req.implicit_read:

        def gen():
          for f in pd.iterdir():
            if f.is_file():
              yield (f.name, f.read_bytes())

        for f, b in gen():
          fp = PureWindowsPath(os.path.normpath(os.path.join(req.path, f))).as_posix()
          resp = FileResponse(sid=req.sid, request_path=req.path, file_path=fp, content=b)
          await websocket.send_bytes(encode_response(resp))
        resp = FileResponse(sid=req.sid,
                            request_path=req.path,
                            file_path=req.path,
                            error=FileError(code=FileErrorCode.EOF))
        await websocket.send_bytes(encode_response(resp))
        continue
      else:
        file_names_ = pd.iterdir()
        file_names = list(map(lambda x: (x.name, x.is_dir()), file_names_))
        resp = FileResponse(sid=req.sid,
                            request_path=req.path,
                            file_path=req.path,
                            filenames=file_names)
        await websocket.send_bytes(encode_response(resp))
        continue
    if p.unwrap().is_file():
      resp = FileResponse(sid=req.sid,
                          request_path=req.path,
                          file_path=req.path,
                          content=p.unwrap().read_bytes())
      await websocket.send_bytes(encode_response(resp))
      continue


async def file_ws_sender(websocket: WebSocket, broadcast: Optional[Broadcast]):
  assert broadcast is not None, "broadcast is not initialized"
  async with broadcast.subscribe(channel=FILE_CHANNEL_NAME) as subscriber:
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


async def file_ws_handler(ws: WebSocket, broadcast: Optional[Broadcast], fm: Optional[FileManager]):
  await ws.accept()
  await run_until_first_complete((file_ws_receiver, {
      "websocket": ws,
      "broadcast": broadcast,
      "fm": fm
  }), (file_ws_sender, {
      "websocket": ws,
      "broadcast": broadcast
  }))
