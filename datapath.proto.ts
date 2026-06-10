import { Uint8ArrayReader, Uint8ArrayWriter } from "./deps/binio.ts";

import {
  assertWireType,
  LEN,
  readBool,
  readBytes,
  readInt32,
  readInt64,
  readTag,
  readUint32,
  skipPayload,
  VARINT,
  writeBool,
  writeBytes,
  writeInt32,
  writeTag,
  writeUint32,
} from "./protobuf.ts";

export interface SnapshotRead {
  ranges: ReadRange[];
}

export function defaultSnapshotRead(): SnapshotRead {
  return { ranges: [] };
}

export function decodeSnapshotRead(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultSnapshotRead(),
): SnapshotRead {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.ranges.push(decodeReadRange(readBytes(r)));
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface SnapshotReadOutput {
  ranges: ReadRangeOutput[];
  readDisabled: boolean;
  readIsStronglyConsistent: boolean;
  status: SnapshotReadStatus;
}

export function encodeSnapshotReadOutput(
  msg: SnapshotReadOutput,
): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  for (const value of msg.ranges) {
    writeTag(w, { fieldNumber: 1, wireType: LEN });
    writeBytes(w, encodeReadRangeOutput(value));
  }
  if (msg.readDisabled) {
    writeTag(w, { fieldNumber: 2, wireType: VARINT });
    writeBool(w, msg.readDisabled);
  }
  if (msg.readIsStronglyConsistent) {
    writeTag(w, { fieldNumber: 4, wireType: VARINT });
    writeBool(w, msg.readIsStronglyConsistent);
  }
  if (msg.status) {
    writeTag(w, { fieldNumber: 8, wireType: VARINT });
    writeInt32(w, msg.status);
  }
  return w.bytes;
}

export const SnapshotReadStatus = Object.freeze({
  SR_UNSPECIFIED: 0,
  SR_SUCCESS: 1,
  SR_READ_DISABLED: 2,
});
export type SnapshotReadStatus = number;

export interface ReadRange {
  start: Uint8Array<ArrayBuffer>;
  end: Uint8Array<ArrayBuffer>;
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

export function decodeReadRange(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultReadRange(),
): ReadRange {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.start = readBytes(r);
        break;
      case 2:
        assertWireType(fieldNumber, wireType, LEN);
        msg.end = readBytes(r);
        break;
      case 3:
        assertWireType(fieldNumber, wireType, VARINT);
        msg.limit = readInt32(r);
        break;
      case 4:
        assertWireType(fieldNumber, wireType, VARINT);
        msg.reverse = readBool(r);
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface ReadRangeOutput {
  values: KvEntry[];
}

export function encodeReadRangeOutput(
  msg: ReadRangeOutput,
): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  for (const value of msg.values) {
    writeTag(w, { fieldNumber: 1, wireType: LEN });
    writeBytes(w, encodeKvEntry(value));
  }
  return w.bytes;
}

export interface AtomicWrite {
  checks: Check[];
  mutations: Mutation[];
  enqueues: Enqueue[];
}

export function defaultAtomicWrite(): AtomicWrite {
  return { checks: [], mutations: [], enqueues: [] };
}

export function decodeAtomicWrite(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultAtomicWrite(),
): AtomicWrite {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.checks.push(decodeCheck(readBytes(r)));
        break;
      case 2:
        assertWireType(fieldNumber, wireType, LEN);
        msg.mutations.push(decodeMutation(readBytes(r)));
        break;
      case 3:
        assertWireType(fieldNumber, wireType, LEN);
        msg.enqueues.push(decodeEnqueue(readBytes(r)));
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface AtomicWriteOutput {
  status: AtomicWriteStatus;
  versionstamp: Uint8Array<ArrayBuffer>;
  failedChecks: number[];
}

export function encodeAtomicWriteOutput(
  msg: AtomicWriteOutput,
): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  if (msg.status) {
    writeTag(w, { fieldNumber: 1, wireType: VARINT });
    writeInt32(w, msg.status);
  }
  if (msg.versionstamp.length) {
    writeTag(w, { fieldNumber: 2, wireType: LEN });
    writeBytes(w, msg.versionstamp);
  }
  if (msg.failedChecks.length) {
    writeTag(w, { fieldNumber: 4, wireType: LEN });
    const p = new Uint8ArrayWriter();
    for (const value of msg.failedChecks) {
      writeUint32(p, value);
    }
    writeBytes(w, p.bytes);
  }
  return w.bytes;
}

export interface Check {
  key: Uint8Array<ArrayBuffer>;
  versionstamp: Uint8Array<ArrayBuffer>;
}

export function defaultCheck(): Check {
  return { key: new Uint8Array(), versionstamp: new Uint8Array() };
}

export function decodeCheck(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultCheck(),
): Check {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.key = readBytes(r);
        break;
      case 2:
        assertWireType(fieldNumber, wireType, LEN);
        msg.versionstamp = readBytes(r);
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface Mutation {
  key: Uint8Array<ArrayBuffer>;
  value: KvValue | null;
  mutationType: MutationType;
  expireAtMs: bigint;
  sumMin: Uint8Array<ArrayBuffer>;
  sumMax: Uint8Array<ArrayBuffer>;
  sumClamp: boolean;
}

export function defaultMutation(): Mutation {
  return {
    key: new Uint8Array(),
    value: null,
    mutationType: 0,
    expireAtMs: 0n,
    sumMin: new Uint8Array(),
    sumMax: new Uint8Array(),
    sumClamp: false,
  };
}

export function decodeMutation(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultMutation(),
): Mutation {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.key = readBytes(r);
        break;
      case 2:
        assertWireType(fieldNumber, wireType, LEN);
        decodeKvValue(readBytes(r), msg.value ??= defaultKvValue());
        break;
      case 3:
        assertWireType(fieldNumber, wireType, VARINT);
        msg.mutationType = readInt32(r);
        break;
      case 4:
        assertWireType(fieldNumber, wireType, VARINT);
        msg.expireAtMs = readInt64(r);
        break;
      case 5:
        assertWireType(fieldNumber, wireType, LEN);
        msg.sumMin = readBytes(r);
        break;
      case 6:
        assertWireType(fieldNumber, wireType, LEN);
        msg.sumMax = readBytes(r);
        break;
      case 7:
        assertWireType(fieldNumber, wireType, VARINT);
        msg.sumClamp = readBool(r);
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface KvValue {
  data: Uint8Array<ArrayBuffer>;
  encoding: ValueEncoding;
}

export function defaultKvValue(): KvValue {
  return { data: new Uint8Array(), encoding: 0 };
}

export function decodeKvValue(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultKvValue(),
): KvValue {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.data = readBytes(r);
        break;
      case 2:
        assertWireType(fieldNumber, wireType, VARINT);
        msg.encoding = readInt32(r);
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface KvEntry {
  key: Uint8Array<ArrayBuffer>;
  value: Uint8Array<ArrayBuffer>;
  encoding: ValueEncoding;
  versionstamp: Uint8Array<ArrayBuffer>;
}

export function encodeKvEntry(msg: KvEntry): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  if (msg.key.length) {
    writeTag(w, { fieldNumber: 1, wireType: LEN });
    writeBytes(w, msg.key);
  }
  if (msg.value.length) {
    writeTag(w, { fieldNumber: 2, wireType: LEN });
    writeBytes(w, msg.value);
  }
  if (msg.encoding) {
    writeTag(w, { fieldNumber: 3, wireType: VARINT });
    writeInt32(w, msg.encoding);
  }
  if (msg.versionstamp.length) {
    writeTag(w, { fieldNumber: 4, wireType: LEN });
    writeBytes(w, msg.versionstamp);
  }
  return w.bytes;
}

export const MutationType = Object.freeze({
  M_UNSPECIFIED: 0,
  M_SET: 1,
  M_DELETE: 2,
  M_SUM: 3,
  M_MAX: 4,
  M_MIN: 5,
  M_SET_SUFFIX_VERSIONSTAMPED_KEY: 9,
});
export type MutationType = number;
export const ValueEncoding = Object.freeze({
  VE_UNSPECIFIED: 0,
  VE_V8: 1,
  VE_LE64: 2,
  VE_BYTES: 3,
});
export type ValueEncoding = number;
export const AtomicWriteStatus = Object.freeze({
  AW_UNSPECIFIED: 0,
  AW_SUCCESS: 1,
  AW_CHECK_FAILURE: 2,
  AW_WRITE_DISABLED: 5,
});
export type AtomicWriteStatus = number;

export interface Enqueue {
  payload: Uint8Array<ArrayBuffer>;
  deadlineMs: bigint;
  keysIfUndelivered: Uint8Array<ArrayBuffer>[];
  backoffSchedule: number[];
}

export function defaultEnqueue(): Enqueue {
  return {
    payload: new Uint8Array(),
    deadlineMs: 0n,
    keysIfUndelivered: [],
    backoffSchedule: [],
  };
}

export function decodeEnqueue(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultEnqueue(),
): Enqueue {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.payload = readBytes(r);
        break;
      case 2:
        assertWireType(fieldNumber, wireType, VARINT);
        msg.deadlineMs = readInt64(r);
        break;
      case 3:
        assertWireType(fieldNumber, wireType, LEN);
        msg.keysIfUndelivered.push(readBytes(r));
        break;
      case 4:
        if (wireType === VARINT) {
          msg.backoffSchedule.push(readUint32(r));
        } else {
          assertWireType(fieldNumber, wireType, LEN);
          const p = new Uint8ArrayReader(readBytes(r));
          while (p.remaining.length !== 0) {
            msg.backoffSchedule.push(readUint32(p));
          }
        }
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface Watch {
  keys: WatchKey[];
}

export function defaultWatch(): Watch {
  return { keys: [] };
}

export function decodeWatch(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultWatch(),
): Watch {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.keys.push(decodeWatchKey(readBytes(r)));
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface WatchOutput {
  status: SnapshotReadStatus;
  keys: WatchKeyOutput[];
}

export function encodeWatchOutput(msg: WatchOutput): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  if (msg.status) {
    writeTag(w, { fieldNumber: 1, wireType: VARINT });
    writeInt32(w, msg.status);
  }
  for (const value of msg.keys) {
    writeTag(w, { fieldNumber: 2, wireType: LEN });
    writeBytes(w, encodeWatchKeyOutput(value));
  }
  return w.bytes;
}

export interface WatchKey {
  key: Uint8Array<ArrayBuffer>;
}

export function defaultWatchKey(): WatchKey {
  return { key: new Uint8Array() };
}

export function decodeWatchKey(
  buf: Uint8Array<ArrayBuffer>,
  msg = defaultWatchKey(),
): WatchKey {
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const tag = readTag(r);
    if (!tag) {
      break;
    }
    const { fieldNumber, wireType } = tag;
    switch (fieldNumber) {
      case 1:
        assertWireType(fieldNumber, wireType, LEN);
        msg.key = readBytes(r);
        break;
      default:
        skipPayload(r, fieldNumber, wireType);
        break;
    }
  }
  return msg;
}

export interface WatchKeyOutput {
  changed: boolean;
  entryIfChanged: KvEntry | null;
}

export function encodeWatchKeyOutput(
  msg: WatchKeyOutput,
): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  if (msg.changed) {
    writeTag(w, { fieldNumber: 1, wireType: VARINT });
    writeBool(w, msg.changed);
  }
  if (msg.entryIfChanged) {
    writeTag(w, { fieldNumber: 2, wireType: LEN });
    writeBytes(w, encodeKvEntry(msg.entryIfChanged));
  }
  return w.bytes;
}
