import {
  assertWireType,
  Buffer,
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

export function decodeSnapshotRead(buf: Uint8Array): SnapshotRead {
  const msg = defaultSnapshotRead();
  const r = new Buffer(buf);
  while (!r.empty()) {
    const record = readPbRecord(r);
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
}

export function encodeSnapshotReadOutput(msg: SnapshotReadOutput): Uint8Array {
  const w = new Buffer();
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
  while (!r.empty()) {
    const record = readPbRecord(r);
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

export function encodeReadRangeOutput(msg: ReadRangeOutput): Uint8Array {
  const w = new Buffer();
  for (const value of msg.values) {
    writePbRecord(w, {
      fieldNumber: 1,
      wireType: PbWireType.LEN,
      value: encodeKvEntry(value),
    });
  }
  return w.bytes({ copy: false });
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

export function decodeAtomicWrite(buf: Uint8Array): AtomicWrite {
  const msg = defaultAtomicWrite();
  const r = new Buffer(buf);
  while (!r.empty()) {
    const record = readPbRecord(r);
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
  versionstamp: Uint8Array;
  failed_checks: number[];
}

export function encodeAtomicWriteOutput(msg: AtomicWriteOutput): Uint8Array {
  const w = new Buffer();
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
  return w.bytes({ copy: false });
}

export interface Check {
  key: Uint8Array;
  versionstamp: Uint8Array;
}

export function defaultCheck(): Check {
  return {
    key: new Uint8Array(),
    versionstamp: new Uint8Array(),
  };
}

export function decodeCheck(buf: Uint8Array): Check {
  const msg = defaultCheck();
  const r = new Buffer(buf);
  while (!r.empty()) {
    const record = readPbRecord(r);
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
  key: Uint8Array;
  value: KvValue;
  mutation_type: MutationType;
  expire_at_ms: bigint;
}

export function defaultMutation(): Mutation {
  return {
    key: new Uint8Array(),
    value: defaultKvValue(),
    mutation_type: 0,
    expire_at_ms: 0n,
  };
}

export function decodeMutation(buf: Uint8Array): Mutation {
  const msg = defaultMutation();
  const r = new Buffer(buf);
  while (!r.empty()) {
    const record = readPbRecord(r);
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
    }
  }
  return msg;
}

export interface KvValue {
  data: Uint8Array;
  encoding: ValueEncoding;
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
  while (!r.empty()) {
    const record = readPbRecord(r);
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
  key: Uint8Array;
  value: Uint8Array;
  encoding: ValueEncoding;
  versionstamp: Uint8Array;
}

export function encodeKvEntry(msg: KvEntry): Uint8Array {
  const w = new Buffer();
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
  return w.bytes({ copy: false });
}

export const MutationType = Object.freeze({
  M_UNSPECIFIED: 0,
  M_SET: 1,
  M_DELETE: 2,
  M_SUM: 3,
  M_MAX: 4,
  M_MIN: 5,
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
  while (!r.empty()) {
    const record = readPbRecord(r);
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
        msg.kv_keys_if_undelivered.push(decodePbBytes(record.value));
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
