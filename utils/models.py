from pydantic import BaseModel
from typing import Optional, Any, Sequence
from enum import Enum, auto
from pathlib import Path


class DatabaseConfig(BaseModel):
  dbname: str
  user: str
  password: Optional[str]


class FileConfig(BaseModel):
  key: str
  path: str


class FileErrorCode(Enum):
  RUNTIME_ERROR = auto()
  EOF = auto()
  FILE_NOT_FOUND = 404


class FileError(BaseModel):
  code: FileErrorCode
  message: Optional[str] = None
  extra: Optional[Any] = None


class EventMessage(BaseModel):
  host: str
  port: int
  message: bytes | str


class Config(BaseModel):
  database: DatabaseConfig
  files: Sequence[FileConfig] = []
