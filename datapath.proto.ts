import { Uint8ArrayReader, Uint8ArrayWriter } from "./deps/binio.ts";

import {
  assertWireType,
  decodePbBool,
  decodePbBytes,
  decodePbInt32,
  decodePbInt64,
  decodePbPackedUint32,
  decodePbUint32,
  encodePbBool,
  encodePbBytes,
  encodePbInt32,
  encodePbPackedUint32,
  PbWireType,
  readPbRecord,
  writePbRecord,
} from "./protobuf.ts";

export interface SnapshotRead {
  ranges: ReadRange[];
}

export function defaultSnapshotRead(): SnapshotRead {
  return {
    ranges: [],
  };
}

export function decodeSnapshotRead(buf: Uint8Array<ArrayBuffer>): SnapshotRead {
  const msg = defaultSnapshotRead();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.ranges.push(decodeReadRange(record.value));
        break;
    }
  }
  return msg;
}

export interface SnapshotReadOutput {
  ranges: ReadRangeOutput[];
  read_disabled: boolean;
  read_is_strongly_consistent: boolean;
  status: SnapshotReadStatus;
}

export function encodeSnapshotReadOutput(
  msg: SnapshotReadOutput,
): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  for (const value of msg.ranges) {
    writePbRecord(w, {
      fieldNumber: 1,
      wireType: PbWireType.LEN,
      value: encodeReadRangeOutput(value),
    });
  }
  if (msg.read_disabled) {
    writePbRecord(w, {
      fieldNumber: 2,
      wireType: PbWireType.VARINT,
      value: encodePbBool(msg.read_disabled),
    });
  }
  if (msg.read_is_strongly_consistent) {
    writePbRecord(w, {
      fieldNumber: 4,
      wireType: PbWireType.VARINT,
      value: encodePbBool(msg.read_is_strongly_consistent),
    });
  }
  if (msg.status) {
    writePbRecord(w, {
      fieldNumber: 8,
      wireType: PbWireType.VARINT,
      value: encodePbInt32(msg.status),
    });
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

export function decodeReadRange(buf: Uint8Array<ArrayBuffer>): ReadRange {
  const msg = defaultReadRange();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.start = decodePbBytes(record.value);
        break;
      case 2:
        assertWireType(record, PbWireType.LEN);
        msg.end = decodePbBytes(record.value);
        break;
      case 3:
        assertWireType(record, PbWireType.VARINT);
        msg.limit = decodePbInt32(record.value);
        break;
      case 4:
        assertWireType(record, PbWireType.VARINT);
        msg.reverse = decodePbBool(record.value);
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
    writePbRecord(w, {
      fieldNumber: 1,
      wireType: PbWireType.LEN,
      value: encodeKvEntry(value),
    });
  }
  return w.bytes;
}

export interface AtomicWrite {
  checks: Check[];
  mutations: Mutation[];
  enqueues: Enqueue[];
}

export function defaultAtomicWrite(): AtomicWrite {
  return {
    checks: [],
    mutations: [],
    enqueues: [],
  };
}

export function decodeAtomicWrite(buf: Uint8Array<ArrayBuffer>): AtomicWrite {
  const msg = defaultAtomicWrite();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.checks.push(decodeCheck(record.value));
        break;
      case 2:
        assertWireType(record, PbWireType.LEN);
        msg.mutations.push(decodeMutation(record.value));
        break;
      case 3:
        assertWireType(record, PbWireType.LEN);
        msg.enqueues.push(decodeEnqueue(record.value));
        break;
    }
  }
  return msg;
}

export interface AtomicWriteOutput {
  status: AtomicWriteStatus;
  versionstamp: Uint8Array<ArrayBuffer>;
  failed_checks: number[];
}

export function encodeAtomicWriteOutput(
  msg: AtomicWriteOutput,
): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  if (msg.status) {
    writePbRecord(w, {
      fieldNumber: 1,
      wireType: PbWireType.VARINT,
      value: encodePbInt32(msg.status),
    });
  }
  if (msg.versionstamp.length) {
    writePbRecord(w, {
      fieldNumber: 2,
      wireType: PbWireType.LEN,
      value: encodePbBytes(msg.versionstamp),
    });
  }
  if (msg.failed_checks.length) {
    writePbRecord(w, {
      fieldNumber: 4,
      wireType: PbWireType.LEN,
      value: encodePbPackedUint32(msg.failed_checks),
    });
  }
  return w.bytes;
}

export interface Check {
  key: Uint8Array<ArrayBuffer>;
  versionstamp: Uint8Array<ArrayBuffer>;
}

export function defaultCheck(): Check {
  return {
    key: new Uint8Array(),
    versionstamp: new Uint8Array(),
  };
}

export function decodeCheck(buf: Uint8Array<ArrayBuffer>): Check {
  const msg = defaultCheck();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.key = decodePbBytes(record.value);
        break;
      case 2:
        assertWireType(record, PbWireType.LEN);
        msg.versionstamp = decodePbBytes(record.value);
        break;
    }
  }
  return msg;
}

export interface Mutation {
  key: Uint8Array<ArrayBuffer>;
  value: KvValue | null;
  mutation_type: MutationType;
  expire_at_ms: bigint;
  sum_min: Uint8Array<ArrayBuffer>;
  sum_max: Uint8Array<ArrayBuffer>;
  sum_clamp: boolean;
}

export function defaultMutation(): Mutation {
  return {
    key: new Uint8Array(),
    value: null,
    mutation_type: 0,
    expire_at_ms: 0n,
    sum_min: new Uint8Array(),
    sum_max: new Uint8Array(),
    sum_clamp: false,
  };
}

export function decodeMutation(buf: Uint8Array<ArrayBuffer>): Mutation {
  const msg = defaultMutation();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.key = decodePbBytes(record.value);
        break;
      case 2:
        assertWireType(record, PbWireType.LEN);
        msg.value = decodeKvValue(record.value);
        break;
      case 3:
        assertWireType(record, PbWireType.VARINT);
        msg.mutation_type = decodePbInt32(record.value);
        break;
      case 4:
        assertWireType(record, PbWireType.VARINT);
        msg.expire_at_ms = decodePbInt64(record.value);
        break;
      case 5:
        assertWireType(record, PbWireType.LEN);
        msg.sum_min = decodePbBytes(record.value);
        break;
      case 6:
        assertWireType(record, PbWireType.LEN);
        msg.sum_max = decodePbBytes(record.value);
        break;
      case 7:
        assertWireType(record, PbWireType.VARINT);
        msg.sum_clamp = decodePbBool(record.value);
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
  return {
    data: new Uint8Array(),
    encoding: 0,
  };
}

export function decodeKvValue(buf: Uint8Array<ArrayBuffer>): KvValue {
  const msg = defaultKvValue();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.data = decodePbBytes(record.value);
        break;
      case 2:
        assertWireType(record, PbWireType.VARINT);
        msg.encoding = decodePbInt32(record.value);
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
    writePbRecord(w, {
      fieldNumber: 1,
      wireType: PbWireType.LEN,
      value: encodePbBytes(msg.key),
    });
  }
  if (msg.value.length) {
    writePbRecord(w, {
      fieldNumber: 2,
      wireType: PbWireType.LEN,
      value: encodePbBytes(msg.value),
    });
  }
  if (msg.encoding) {
    writePbRecord(w, {
      fieldNumber: 3,
      wireType: PbWireType.VARINT,
      value: encodePbInt32(msg.encoding),
    });
  }
  if (msg.versionstamp.length) {
    writePbRecord(w, {
      fieldNumber: 4,
      wireType: PbWireType.LEN,
      value: encodePbBytes(msg.versionstamp),
    });
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
  deadline_ms: bigint;
  keys_if_undelivered: Uint8Array<ArrayBuffer>[];
  backoff_schedule: number[];
}

export function defaultEnqueue(): Enqueue {
  return {
    payload: new Uint8Array(),
    deadline_ms: 0n,
    keys_if_undelivered: [],
    backoff_schedule: [],
  };
}

export function decodeEnqueue(buf: Uint8Array<ArrayBuffer>): Enqueue {
  const msg = defaultEnqueue();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.payload = decodePbBytes(record.value);
        break;
      case 2:
        assertWireType(record, PbWireType.VARINT);
        msg.deadline_ms = decodePbInt64(record.value);
        break;
      case 3:
        assertWireType(record, PbWireType.LEN);
        msg.keys_if_undelivered.push(decodePbBytes(record.value));
        break;
      case 4:
        switch (record.wireType) {
          case 0:
            msg.backoff_schedule.push(decodePbUint32(record.value));
            break;
          default:
            assertWireType(record, PbWireType.LEN);
            decodePbPackedUint32(record.value, msg.backoff_schedule);
            break;
        }
        break;
    }
  }
  return msg;
}

export interface Watch {
  keys: WatchKey[];
}

export function defaultWatch(): Watch {
  return {
    keys: [],
  };
}

export function decodeWatch(buf: Uint8Array<ArrayBuffer>): Watch {
  const msg = defaultWatch();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.keys.push(decodeWatchKey(record.value));
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
    writePbRecord(w, {
      fieldNumber: 1,
      wireType: PbWireType.VARINT,
      value: encodePbInt32(msg.status),
    });
  }
  for (const value of msg.keys) {
    writePbRecord(w, {
      fieldNumber: 2,
      wireType: PbWireType.LEN,
      value: encodeWatchKeyOutput(value),
    });
  }
  return w.bytes;
}

export interface WatchKey {
  key: Uint8Array<ArrayBuffer>;
}

export function defaultWatchKey(): WatchKey {
  return {
    key: new Uint8Array(),
  };
}

export function decodeWatchKey(buf: Uint8Array<ArrayBuffer>): WatchKey {
  const msg = defaultWatchKey();
  const r = new Uint8ArrayReader(buf);
  for (;;) {
    const record = readPbRecord(r);
    if (!record) {
      break;
    }
    switch (record.fieldNumber) {
      case 1:
        assertWireType(record, PbWireType.LEN);
        msg.key = decodePbBytes(record.value);
        break;
    }
  }
  return msg;
}

export interface WatchKeyOutput {
  changed: boolean;
  entry_if_changed: KvEntry | null;
}

export function encodeWatchKeyOutput(
  msg: WatchKeyOutput,
): Uint8Array<ArrayBuffer> {
  const w = new Uint8ArrayWriter();
  if (msg.changed) {
    writePbRecord(w, {
      fieldNumber: 1,
      wireType: PbWireType.VARINT,
      value: encodePbBool(msg.changed),
    });
  }
  if (msg.entry_if_changed) {
    writePbRecord(w, {
      fieldNumber: 2,
      wireType: PbWireType.LEN,
      value: encodeKvEntry(msg.entry_if_changed),
    });
  }
  return w.bytes;
}
