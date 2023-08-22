import {
  Buffer,
  readBigVarUint64LESync,
  readFullSync,
  readVarUint32LESync,
  unexpectedEof,
  writeVarInt32LESync,
} from "./deps/binio.ts";

const encoder = new TextEncoder();

function assertWireType(tag: number, expected: number): undefined {
  const wireType = tag & 7;
  if (wireType !== expected) {
    const fieldNumber = tag >>> 3;
    throw new TypeError(
      `Invalid wire type for field ${fieldNumber}: expected ${expected}, got ${wireType}`,
    );
  }
}

function readPbInt32(r: Buffer): number {
  const raw = readVarUint32LESync(r) ?? unexpectedEof();
  return raw | 0;
}

function readPbInt64(r: Buffer): bigint {
  const raw = readBigVarUint64LESync(r) ?? unexpectedEof();
  return BigInt.asIntN(64, raw);
}

function readPbUint32(r: Buffer): number {
  const raw = readVarUint32LESync(r) ?? unexpectedEof();
  return raw;
}

function* readPbPackedUint32(r: Buffer): IterableIterator<number> {
  const len = readVarUint32LESync(r) ?? unexpectedEof();
  for (let i = 0; i < len; i++) {
    yield readPbUint32(r);
  }
}

function readPbBool(r: Buffer): boolean {
  const raw = readVarUint32LESync(r) ?? unexpectedEof();
  return raw !== 0;
}

function readPbBytes(r: Buffer): Uint8Array {
  const len = readVarUint32LESync(r) ?? unexpectedEof();
  return readFullSync(r, new Uint8Array(len)) ?? unexpectedEof();
}

function writePbInt32(w: Buffer, value: number): undefined {
  writeVarInt32LESync(w, value);
}

function writePbBool(w: Buffer, value: boolean): undefined {
  writeVarInt32LESync(w, value ? 1 : 0);
}

function writePbString(w: Buffer, value: string): undefined {
  const buf = encoder.encode(value);
  writeVarInt32LESync(w, buf.length);
  w.writeSync(buf);
}

function writePbBytes(w: Buffer, value: Uint8Array): undefined {
  writeVarInt32LESync(w, value.length);
  w.writeSync(value);
}

export interface SnapshotRead {
  ranges: ReadRange[];
}

export function defaultSnapshotRead(): SnapshotRead {
  return {
    ranges: [],
  };
}

export function decodeSnapshotRead(buf: Uint8Array): SnapshotRead {
  const msg = defaultSnapshotRead();
  const r = new Buffer(buf);
  for (;;) {
    const tag = readVarUint32LESync(r);
    if (tag === null) {
      break;
    }
    switch (tag >>> 3) {
      case 1:
        assertWireType(tag, 2);
        msg.ranges.push(decodeReadRange(readPbBytes(r)));
        break;
    }
  }
  return msg;
}

export interface SnapshotReadOutput {
  ranges: ReadRangeOutput[];
  read_disabled: boolean;
  regions_if_read_disabled: string[];
  read_is_strongly_consistent: boolean;
  primary_if_not_strongly_consistent: string;
}

export function encodeSnapshotReadOutput(msg: SnapshotReadOutput): Uint8Array {
  const w = new Buffer();
  for (const value of msg.ranges) {
    writeVarInt32LESync(w, 0o12);
    writePbBytes(w, encodeReadRangeOutput(value));
  }
  if (msg.read_disabled) {
    writeVarInt32LESync(w, 0o20);
    writePbBool(w, msg.read_disabled);
  }
  for (const value of msg.regions_if_read_disabled) {
    writeVarInt32LESync(w, 0o32);
    writePbString(w, value);
  }
  if (msg.read_is_strongly_consistent) {
    writeVarInt32LESync(w, 0o40);
    writePbBool(w, msg.read_is_strongly_consistent);
  }
  if (msg.primary_if_not_strongly_consistent) {
    writeVarInt32LESync(w, 0o52);
    writePbString(w, msg.primary_if_not_strongly_consistent);
  }
  return w.bytes({ copy: false });
}

export interface ReadRange {
  start: Uint8Array;
  end: Uint8Array;
  limit: number;
  reverse: boolean;
}

export function defaultReadRange(): ReadRange {
  return {
    start: new Uint8Array(),
    end: new Uint8Array(),
    limit: 0,
    reverse: false,
  };
}

export function decodeReadRange(buf: Uint8Array): ReadRange {
  const msg = defaultReadRange();
  const r = new Buffer(buf);
  for (;;) {
    const tag = readVarUint32LESync(r);
    if (tag === null) {
      break;
    }
    switch (tag >>> 3) {
      case 1:
        assertWireType(tag, 2);
        msg.start = readPbBytes(r);
        break;
      case 2:
        assertWireType(tag, 2);
        msg.end = readPbBytes(r);
        break;
      case 3:
        assertWireType(tag, 0);
        msg.limit = readPbInt32(r);
        break;
      case 4:
        assertWireType(tag, 0);
        msg.reverse = readPbBool(r);
        break;
    }
  }
  return msg;
}

export interface ReadRangeOutput {
  values: KvEntry[];
}

export function encodeReadRangeOutput(msg: ReadRangeOutput): Uint8Array {
  const w = new Buffer();
  for (const value of msg.values) {
    writeVarInt32LESync(w, 0o12);
    writePbBytes(w, encodeKvEntry(value));
  }
  return w.bytes({ copy: false });
}

export interface AtomicWrite {
  kv_checks: KvCheck[];
  kv_mutations: KvMutation[];
  enqueues: Enqueue[];
}

export function defaultAtomicWrite(): AtomicWrite {
  return {
    kv_checks: [],
    kv_mutations: [],
    enqueues: [],
  };
}

export function decodeAtomicWrite(buf: Uint8Array): AtomicWrite {
  const msg = defaultAtomicWrite();
  const r = new Buffer(buf);
  for (;;) {
    const tag = readVarUint32LESync(r);
    if (tag === null) {
      break;
    }
    switch (tag >>> 3) {
      case 1:
        assertWireType(tag, 2);
        msg.kv_checks.push(decodeKvCheck(readPbBytes(r)));
        break;
      case 2:
        assertWireType(tag, 2);
        msg.kv_mutations.push(decodeKvMutation(readPbBytes(r)));
        break;
      case 3:
        assertWireType(tag, 2);
        msg.enqueues.push(decodeEnqueue(readPbBytes(r)));
        break;
    }
  }
  return msg;
}

export interface AtomicWriteOutput {
  status: AtomicWriteStatus;
  versionstamp: Uint8Array;
  primary_if_write_disabled: string;
}

export function encodeAtomicWriteOutput(msg: AtomicWriteOutput): Uint8Array {
  const w = new Buffer();
  if (msg.status) {
    writeVarInt32LESync(w, 0o10);
    writePbInt32(w, msg.status);
  }
  if (msg.versionstamp.length) {
    writeVarInt32LESync(w, 0o22);
    writePbBytes(w, msg.versionstamp);
  }
  if (msg.primary_if_write_disabled) {
    writeVarInt32LESync(w, 0o32);
    writePbString(w, msg.primary_if_write_disabled);
  }
  return w.bytes({ copy: false });
}

export interface KvCheck {
  key: Uint8Array;
  versionstamp: Uint8Array;
}

export function defaultKvCheck(): KvCheck {
  return {
    key: new Uint8Array(),
    versionstamp: new Uint8Array(),
  };
}

export function decodeKvCheck(buf: Uint8Array): KvCheck {
  const msg = defaultKvCheck();
  const r = new Buffer(buf);
  for (;;) {
    const tag = readVarUint32LESync(r);
    if (tag === null) {
      break;
    }
    switch (tag >>> 3) {
      case 1:
        assertWireType(tag, 2);
        msg.key = readPbBytes(r);
        break;
      case 2:
        assertWireType(tag, 2);
        msg.versionstamp = readPbBytes(r);
        break;
    }
  }
  return msg;
}

export interface KvMutation {
  key: Uint8Array;
  value: KvValue;
  mutation_type: KvMutationType;
}

export function defaultKvMutation(): KvMutation {
  return {
    key: new Uint8Array(),
    value: defaultKvValue(),
    mutation_type: 0,
  };
}

export function decodeKvMutation(buf: Uint8Array): KvMutation {
  const msg = defaultKvMutation();
  const r = new Buffer(buf);
  for (;;) {
    const tag = readVarUint32LESync(r);
    if (tag === null) {
      break;
    }
    switch (tag >>> 3) {
      case 1:
        assertWireType(tag, 2);
        msg.key = readPbBytes(r);
        break;
      case 2:
        assertWireType(tag, 2);
        msg.value = decodeKvValue(readPbBytes(r));
        break;
      case 3:
        assertWireType(tag, 0);
        msg.mutation_type = readPbInt32(r);
        break;
    }
  }
  return msg;
}

export interface KvValue {
  data: Uint8Array;
  encoding: KvValueEncoding;
}

export function defaultKvValue(): KvValue {
  return {
    data: new Uint8Array(),
    encoding: 0,
  };
}

export function decodeKvValue(buf: Uint8Array): KvValue {
  const msg = defaultKvValue();
  const r = new Buffer(buf);
  for (;;) {
    const tag = readVarUint32LESync(r);
    if (tag === null) {
      break;
    }
    switch (tag >>> 3) {
      case 1:
        assertWireType(tag, 2);
        msg.data = readPbBytes(r);
        break;
      case 2:
        assertWireType(tag, 0);
        msg.encoding = readPbInt32(r);
        break;
    }
  }
  return msg;
}

export interface KvEntry {
  key: Uint8Array;
  value: Uint8Array;
  encoding: KvValueEncoding;
  versionstamp: Uint8Array;
}

export function encodeKvEntry(msg: KvEntry): Uint8Array {
  const w = new Buffer();
  if (msg.key.length) {
    writeVarInt32LESync(w, 0o12);
    writePbBytes(w, msg.key);
  }
  if (msg.value.length) {
    writeVarInt32LESync(w, 0o22);
    writePbBytes(w, msg.value);
  }
  if (msg.encoding) {
    writeVarInt32LESync(w, 0o30);
    writePbInt32(w, msg.encoding);
  }
  if (msg.versionstamp.length) {
    writeVarInt32LESync(w, 0o42);
    writePbBytes(w, msg.versionstamp);
  }
  return w.bytes({ copy: false });
}

export const KvMutationType = Object.freeze({
  M_UNSPECIFIED: 0,
  M_SET: 1,
  M_CLEAR: 2,
  M_SUM: 3,
  M_MAX: 4,
  M_MIN: 5,
});
export type KvMutationType = number;
export const KvValueEncoding = Object.freeze({
  VE_UNSPECIFIED: 0,
  VE_V8: 1,
  VE_LE64: 2,
  VE_BYTES: 3,
});
export type KvValueEncoding = number;
export const AtomicWriteStatus = Object.freeze({
  AW_UNSPECIFIED: 0,
  AW_SUCCESS: 1,
  AW_CHECK_FAILURE: 2,
  AW_UNSUPPORTED_WRITE: 3,
  AW_USAGE_LIMIT_EXCEEDED: 4,
  AW_WRITE_DISABLED: 5,
  AW_QUEUE_BACKLOG_LIMIT_EXCEEDED: 6,
});
export type AtomicWriteStatus = number;

export interface Enqueue {
  payload: Uint8Array;
  deadline_ms: bigint;
  kv_keys_if_undelivered: Uint8Array[];
  backoff_schedule: number[];
}

export function defaultEnqueue(): Enqueue {
  return {
    payload: new Uint8Array(),
    deadline_ms: 0n,
    kv_keys_if_undelivered: [],
    backoff_schedule: [],
  };
}

export function decodeEnqueue(buf: Uint8Array): Enqueue {
  const msg = defaultEnqueue();
  const r = new Buffer(buf);
  for (;;) {
    const tag = readVarUint32LESync(r);
    if (tag === null) {
      break;
    }
    switch (tag >>> 3) {
      case 1:
        assertWireType(tag, 2);
        msg.payload = readPbBytes(r);
        break;
      case 2:
        assertWireType(tag, 0);
        msg.deadline_ms = readPbInt64(r);
        break;
      case 3:
        assertWireType(tag, 2);
        msg.kv_keys_if_undelivered.push(readPbBytes(r));
        break;
      case 4:
        switch (tag & 7) {
          case 0:
            msg.backoff_schedule.push(readPbUint32(r));
            break;
          default:
            assertWireType(tag, 2);
            for (const value of readPbPackedUint32(r)) {
              msg.backoff_schedule.push(value);
            }
            break;
        }
        break;
    }
  }
  return msg;
}
