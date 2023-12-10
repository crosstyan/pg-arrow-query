import { isLeft, isRight, left, right } from "fp-ts/Either"
import type { Either, Left, Right } from "fp-ts/Either"


export const MagicNumbers = {
  request: 0x00,
  response: 0x01,
  notification: 0x02,
} as const

export type MagicLiteral = typeof MagicNumbers[keyof typeof MagicNumbers]

const RuntimeErrors = 0x10

const BadMagicNumber = 0x20
const BadType = 0x21
const BadLength = 0x22
const BadCBOR = 0x23

const InvalidMethod = 0x40

const DuplicateStringIndex = 0x31
const DuplicateNumberIndex = 0x32

export const E = {
  RuntimeErrors,
  BadMagicNumber,
  BadType,
  BadLength,
  BadCBOR,
  InvalidMethod,
  DuplicateStringIndex,
  DuplicateNumberIndex,
} as const

export type ErrorCodeLiteral = typeof E[keyof typeof E]

export const StringError: Record<ErrorCodeLiteral, string> = {
  [RuntimeErrors]: "runtime errors",
  [BadMagicNumber]: "bad magic number",
  [BadType]: "bad type",
  [BadLength]: "bad length",
  [BadCBOR]: "bad cbor",
  [InvalidMethod]: "invalid method",
  [DuplicateStringIndex]: "duplicate string index",
  [DuplicateNumberIndex]: "duplicate number index",
} as const

export type RpcError = {
  code: ErrorCodeLiteral
  message?: string | null | undefined
  /**
   * an extra field for upstream to check and
   * won't actually be serialized
   */
  extra?: unknown | null | undefined
}

export type Result<T> = Either<RpcError, T>

export const ListAllMethod = 0x69

// a magic map for void return
export const VoidOk = { 0: 1 } as const

export function isErrorCode(code: number): code is ErrorCodeLiteral {
  const codes:number[] = Object.values(E)
  if (codes.includes(code)) {
    return true
  } else {
    return false
  }
}
