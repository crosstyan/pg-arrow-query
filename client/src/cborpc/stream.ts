import { isLeft, isRight, left, right } from 'fp-ts/Either'
import { Observable, Subject, Subscription } from "rxjs"
import * as OP from "rxjs/operators"
import type { Either, Left, Right } from 'fp-ts/Either'
import { CBOR } from "cbor-redux"
import { VoidOk, type Result, StringError } from "./errors"
import { E, MagicNumbers } from "./errors"
import type { RpcError } from "./errors"
import WebSocket from "ws"
import pino from "pino"

export type WsData = WebSocket.Data

type CallMessage = {
  msg_id: number
  method: string | number
  params: any[]
}

function encodeCallMessage(msg: CallMessage): ArrayBuffer {
  const magic = MagicNumbers.request
  const msg_id = msg.msg_id
  const method = msg.method
  const params = msg.params
  const data = CBOR.encode([magic, msg_id, method, params])
  return data
}

function decodeCallMessage(data: ArrayBuffer): Result<CallMessage> {
  const res = CBOR.decode(data)

  if (res.length < 4) {
    const err: RpcError = {
      code: E.BadLength,
    }
    return left(err)
  }

  const [magic, msg_id, method, params] = res
  if (typeof magic !== "number") {
    const err: RpcError = {
      code: E.BadType,
      message: "magic number is not a number",
    }
    return left(err)
  }

  const values: number[] = Object.values(MagicNumbers)
  if (!values.includes(magic)) {
    const err: RpcError = {
      code: E.BadMagicNumber,
      message: `magic number ${magic} is not in ${MagicNumbers}`,
    }
    return left(err)
  }

  if (typeof msg_id !== "number") {
    const err: RpcError = {
      code: E.BadType,
      message: "msg_id is not a number",
    }
    return left(err)
  }


  if (!(typeof method === "string" || typeof method === "number")) {
    const err: RpcError = {
      code: E.BadType,
      message: "method is not a string or number",
    }
    return left(err)
  }

  if (!Array.isArray(params)) {
    const err: RpcError = {
      code: E.BadType,
      message: "params is not an array",
    }
    return left(err)
  }

  return right({
    msg_id,
    method,
    params,
  })
}


type ResultMessage<T> = {
  msg_id: number
  error?: RpcError
  result?: T
}

/**
 * `T` should be something could be serialized by CBOR
 * @param result
 * @param throwBothNull if both error and result are null, throw an error. Otherwise, use an Ok magic number
 */
function encodeResult<T>(result: ResultMessage<T>, throwBothNull: boolean = false): ArrayBuffer {
  const magic = MagicNumbers.response
  const msg_id = result.msg_id
  let error: RpcError | undefined | null = result.error
  let res: T | undefined | null | typeof VoidOk = result.result

  if (error === undefined) {
    error = null
  }

  let cleanError = error !== null ? {
    code: error.code,
    message: error.message ?? null,
  } : null

  if (res === undefined) {
    res = null
  }

  if (cleanError === null && res === null) {
    if (throwBothNull) {
      throw new Error("both error and result are null")
    }
    res = VoidOk
  }

  const data = CBOR.encode([magic, msg_id, cleanError, res])
  return data
}

function decodeResult(data: ArrayBuffer): Result<ResultMessage<any>> {
  const res = CBOR.decode(data)

  if (res.length < 4) {
    const err: RpcError = {
      code: E.BadLength,
    }
    return left(err)
  }

  const [magic, msg_id, error, result] = res
  if (typeof magic !== "number") {
    const err: RpcError = {
      code: E.BadType,
      message: "magic number is not a number",
    }
    return left(err)
  }

  const values: number[] = Object.values(MagicNumbers)
  if (!values.includes(magic)) {
    const err: RpcError = {
      code: E.BadMagicNumber,
      message: `magic number ${magic} is not in ${MagicNumbers}`,
    }
    return left(err)
  }

  if (typeof msg_id !== "number") {
    const err: RpcError = {
      code: E.BadType,
      message: "msg_id is not a number",
    }
    return left(err)
  }

  // don't care about error or result type
  // downstream should check for it
  return right({
    msg_id,
    error,
    result,
  })
}

type Resolver<T> = (value: T | PromiseLike<T>) => void
type AnyResultMessage = ResultMessage<any>

export class CborRpcCaller {
  private ws: WebSocket
  private subject: Subject<WsData>
  private callObs: Observable<AnyResultMessage>
  private sub: Subscription | undefined
  private table: Record<number, Resolver<AnyResultMessage>>
  private logger: ReturnType<typeof pino>

  constructor(url: string) {
    this.ws = new WebSocket(url)
    this.ws.binaryType = "arraybuffer"
    this.subject = new Subject()
    this.logger = pino()
    this.table = {}

    this.ws.onmessage = (event: WebSocket.MessageEvent) => {
      this.subject.next(event.data)
    }

    this.callObs = this.subject.pipe(
      OP.filter((data: WsData): data is ArrayBuffer => data instanceof ArrayBuffer),
      OP.map((data: ArrayBuffer) => decodeResult(data)),
      OP.map((result: Result<ResultMessage<any>>) => {
        if (isLeft(result)) {
          const errorStr = StringError[result.left.code]
          this.logger.error(`decode error`, errorStr)
          return null
        }
        return result.right
      }),
      OP.filter((result: CallMessage | null): result is CallMessage => result !== null),
    )

    this.sub = this.callObs.subscribe((msg: CallMessage) => {
      this.logger.info(`callMsg`, msg)
      // try to resolve the promise
      const resolver = this.table[msg.msg_id]
      if (resolver === undefined) {
        this.logger.error(`promise not found for msg_id ${msg.msg_id}`)
        return
      }
      try {
        resolver(msg)
      } catch (e) {
        this.logger.error(`resolving promise for ${msg.msg_id}`, e, msg)
      } finally {
        delete this.table[msg.msg_id]
      }
    })
  }

  public call(method: string | number, ...params: any[]): Promise<ResultMessage<any>> {
    const UINT32_MAX = 4294967295
    let msg_id = Math.floor(Math.random() * UINT32_MAX)
    while (this.table[msg_id] !== undefined) {
      msg_id = Math.floor(Math.random() * UINT32_MAX)
    }
    const msg: CallMessage = {
      msg_id,
      method,
      params,
    }
    const data = encodeCallMessage(msg)
    const promise = new Promise<ResultMessage<any>>((resolve, reject) => {
      this.table[msg_id] = resolve
    })
    this.ws.send(data)
    return promise
  }

  public wsOpend(): boolean {
    return this.ws.readyState === WebSocket.OPEN
  }


  public close() {
    this.sub?.unsubscribe()
    this.ws.close()
  }

  // https://www.totaltypescript.com/typescript-5-2-new-keyword-using
  public [Symbol.dispose]() {
    this.close()
  }
}

