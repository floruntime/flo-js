/**
 * Flo SDK error types.
 */

import { StatusCode, statusCodeToString, TABLE_HASH, VERSION } from "./types.js";

/**
 * Base error class for Flo SDK errors.
 */
export class FloError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FloError";
  }
}

/**
 * Error thrown when the client is not connected to the server.
 */
export class NotConnectedError extends FloError {
  constructor() {
    super("flo: not connected to server");
    this.name = "NotConnectedError";
  }
}

/**
 * Error thrown when connection to the server fails.
 */
export class ConnectionError extends FloError {
  constructor(endpoint: string, cause?: Error) {
    super(
      `flo: connection failed: ${endpoint}${cause ? ` - ${cause.message}` : ""}`
    );
    this.name = "ConnectionError";
    this.cause = cause;
  }
}

/**
 * Error thrown when the endpoint format is invalid.
 */
export class InvalidEndpointError extends FloError {
  constructor(endpoint: string, reason?: string) {
    super(`flo: invalid endpoint: ${endpoint}${reason ? ` (${reason})` : ""}`);
    this.name = "InvalidEndpointError";
  }
}

/**
 * Error thrown when an unexpected end of stream is encountered.
 */
export class UnexpectedEOFError extends FloError {
  constructor(context?: string) {
    super(`flo: unexpected end of stream${context ? `: ${context}` : ""}`);
    this.name = "UnexpectedEOFError";
  }
}

/**
 * Error thrown when the protocol magic number is invalid.
 */
export class InvalidMagicError extends FloError {
  constructor() {
    super("flo: invalid protocol magic");
    this.name = "InvalidMagicError";
  }
}

/**
 * Thrown when the server was built from another protocol version or op
 * table than this SDK, so neither can read the other. Upgrade the SDK and
 * the server together.
 */
export class TableMismatchError extends FloError {
  constructor(
    public readonly serverVersion: number,
    public readonly serverTable: bigint
  ) {
    super(
      serverVersion !== VERSION
        ? `flo: server protocol ${serverVersion}, client protocol ${VERSION}: upgrade the client`
        : `flo: server table 0x${hex16(serverTable)}, client table 0x${hex16(TABLE_HASH)}: upgrade the client`
    );
    this.name = "TableMismatchError";
  }
}

function hex16(n: bigint): string {
  return n.toString(16).padStart(16, "0");
}

/**
 * Error thrown when CRC32 checksum validation fails.
 */
export class InvalidChecksumError extends FloError {
  constructor(expected: number, actual: number) {
    super(
      `flo: invalid checksum: expected 0x${expected.toString(16).padStart(8, "0")}, got 0x${actual.toString(16).padStart(8, "0")}`
    );
    this.name = "InvalidChecksumError";
  }
}

/**
 * Error thrown when the response is incomplete.
 */
export class IncompleteResponseError extends FloError {
  constructor(context?: string) {
    super(`flo: incomplete response${context ? `: ${context}` : ""}`);
    this.name = "IncompleteResponseError";
  }
}

/**
 * Error thrown when the namespace exceeds maximum size.
 */
export class NamespaceTooLargeError extends FloError {
  constructor() {
    super("flo: namespace too large (max 255 bytes)");
    this.name = "NamespaceTooLargeError";
  }
}

/**
 * Error thrown when the key exceeds maximum size.
 */
export class KeyTooLargeError extends FloError {
  constructor() {
    super("flo: key too large (max 64 KB)");
    this.name = "KeyTooLargeError";
  }
}

/**
 * Error thrown when the value exceeds maximum size.
 */
export class ValueTooLargeError extends FloError {
  constructor() {
    super("flo: value too large (max 16 MB)");
    this.name = "ValueTooLargeError";
  }
}

/**
 * Error thrown when a blocking wait (blockMs) exceeds 300000 ms (5 minutes).
 * The server would refuse it with bad_request, so the SDK refuses it first.
 */
export class BlockTooLongError extends FloError {
  constructor(blockMs: number) {
    super(
      `flo: a blocking wait (blockMs) is at most 300000 ms (5 minutes), got ${blockMs}`
    );
    this.name = "BlockTooLongError";
  }
}

/**
 * Error thrown when the operation times out.
 */
export class TimeoutError extends FloError {
  constructor(timeoutMs: number) {
    super(`flo: operation timed out after ${timeoutMs}ms`);
    this.name = "TimeoutError";
  }
}

/**
 * Error returned by the Flo server.
 */
export class ServerError extends FloError {
  readonly status: StatusCode;
  readonly serverMessage: string;

  constructor(status: StatusCode, message?: string) {
    const statusStr = statusCodeToString(status);
    super(
      message
        ? `flo: server error (${statusStr}): ${message}`
        : `flo: server error: ${statusStr}`
    );
    this.name = "ServerError";
    this.status = status;
    this.serverMessage = message ?? "";
  }

  /**
   * Check if this error matches a specific status code.
   */
  is(status: StatusCode): boolean {
    return this.status === status;
  }
}

/**
 * Error thrown when the requested resource is not found.
 */
export class NotFoundError extends ServerError {
  constructor(message?: string) {
    super(StatusCode.NotFound, message);
    this.name = "NotFoundError";
  }
}

/**
 * Error thrown when the request parameters are invalid.
 */
export class BadRequestError extends ServerError {
  constructor(message?: string) {
    super(StatusCode.BadRequest, message);
    this.name = "BadRequestError";
  }
}

/**
 * Error thrown when there is a conflict (e.g., CAS version mismatch).
 */
export class ConflictError extends ServerError {
  constructor(message?: string) {
    super(StatusCode.Conflict, message);
    this.name = "ConflictError";
  }
}

/**
 * Error thrown when authentication is required or failed.
 */
export class UnauthorizedError extends ServerError {
  constructor(message?: string) {
    super(StatusCode.Unauthorized, message);
    this.name = "UnauthorizedError";
  }
}

/**
 * Error thrown when the server is overloaded.
 */
export class OverloadedError extends ServerError {
  constructor(message?: string) {
    super(StatusCode.Overloaded, message);
    this.name = "OverloadedError";
  }
}

/**
 * Error thrown when a write reached no leader, or the shard stopped taking
 * writes or is offline. Retryable, but an offline shard stays unavailable
 * until an operator acts; the server's message says which case it is.
 */
export class UnavailableError extends ServerError {
  constructor(message?: string) {
    super(StatusCode.Unavailable, message);
    this.name = "UnavailableError";
  }
}

/**
 * Error thrown when there is an internal server error. Not retryable: the
 * server also uses it for a write that committed but was not applied, which
 * must not be resent.
 */
export class InternalError extends ServerError {
  constructor(message?: string) {
    super(StatusCode.InternalError, message);
    this.name = "InternalError";
  }
}

/**
 * Creates an appropriate error from a status code and optional data.
 */
export function createServerError(
  status: StatusCode,
  data?: Uint8Array
): ServerError {
  const message = data && data.length > 0 ? new TextDecoder().decode(data) : "";

  switch (status) {
    case StatusCode.NotFound:
      return new NotFoundError(message);
    case StatusCode.BadRequest:
      return new BadRequestError(message);
    case StatusCode.Conflict:
      return new ConflictError(message);
    case StatusCode.Unauthorized:
      return new UnauthorizedError(message);
    case StatusCode.Overloaded:
      return new OverloadedError(message);
    case StatusCode.Unavailable:
      return new UnavailableError(message);
    case StatusCode.InternalError:
      return new InternalError(message);
    default:
      return new ServerError(status, message);
  }
}

/**
 * Check if an error is a NotFoundError.
 */
export function isNotFound(err: unknown): err is NotFoundError {
  return err instanceof ServerError && err.status === StatusCode.NotFound;
}

/**
 * Check if an error is a ConflictError.
 */
export function isConflict(err: unknown): err is ConflictError {
  return err instanceof ServerError && err.status === StatusCode.Conflict;
}

/**
 * Check if an error is a BadRequestError.
 */
export function isBadRequest(err: unknown): err is BadRequestError {
  return err instanceof ServerError && err.status === StatusCode.BadRequest;
}

/**
 * Check if an error is an UnauthorizedError.
 */
export function isUnauthorized(err: unknown): err is UnauthorizedError {
  return err instanceof ServerError && err.status === StatusCode.Unauthorized;
}

/**
 * Check if an error is an OverloadedError.
 */
export function isOverloaded(err: unknown): err is OverloadedError {
  return err instanceof ServerError && err.status === StatusCode.Overloaded;
}

/**
 * Check if an error is an UnavailableError.
 */
export function isUnavailable(err: unknown): err is UnavailableError {
  return err instanceof ServerError && err.status === StatusCode.Unavailable;
}

/**
 * Check if an error is an InternalError.
 */
export function isInternal(err: unknown): err is InternalError {
  return err instanceof ServerError && err.status === StatusCode.InternalError;
}
