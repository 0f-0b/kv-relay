import {
  BufferReader,
  BufferWriter,
  readBigUint64LESync,
  readBigVarUint64LESync,
  readFullSync,
  readUint32LESync,
  unexpectedEof,
  writeBigInt64LESync,
  writeBigVarInt64LESync,
  writeInt32LESync,
} from "./deps/binio.ts";

type ValueOf<T> = T[keyof T];
export const PbWireType = Object.freeze({
  VARINT: 0,
  I64: 1,
  LEN: 2,
  SGROUP: 3,
  EGROUP: 4,
  I32: 5,
});
export type PbWireType = ValueOf<typeof PbWireType>;
export type PbRecord =
  | { fieldNumber: number; wireType: typeof PbWireType.VARINT; value: bigint }
  | { fieldNumber: number; wireType: typeof PbWireType.I64; value: bigint }
  | { fieldNumber: number; wireType: typeof PbWireType.LEN; value: Uint8Array }
  | { fieldNumber: number; wireType: typeof PbWireType.SGROUP }
  | { fieldNumber: number; wireType: typeof PbWireType.EGROUP }
  | { fieldNumber: number; wireType: typeof PbWireType.I32; value: number };

export function assertWireType<T extends PbWireType>(
  record: PbRecord,
  expected: T,
): asserts record is PbRecord & { readonly wireType: T } {
  const { fieldNumber, wireType } = record;
  if (wireType !== expected) {
    throw new TypeError(
      `Invalid wire type for field ${fieldNumber}: expected ${expected}, got ${wireType}`,
    );
  }
}

const readPbVarint = readBigVarUint64LESync;
const readPbI32 = readUint32LESync;
const readPbI64 = readBigUint64LESync;

function readPbLenPrefix(r: BufferReader): Uint8Array | null {
  const rawLen = readPbVarint(r);
  if (rawLen === null) {
    return null;
  }
  const len = decodePbInt32(rawLen);
  if (len < 0) {
    throw new RangeError("Length prefixed payload is too long");
  }
  if (len === 0) {
    return new Uint8Array();
  }
  return readFullSync(r, new Uint8Array(len)) ?? unexpectedEof();
}

export function readPbRecord(r: BufferReader): PbRecord | null {
  const rawTag = readPbVarint(r);
  if (rawTag === null) {
    return null;
  }
  const tag = decodePbUint32(rawTag);
  const fieldNumber = tag >>> 3;
  const wireType = tag & 7;
  switch (wireType) {
    case PbWireType.VARINT: {
      const value = readPbVarint(r) ?? unexpectedEof();
      return { fieldNumber, wireType, value };
    }
    case PbWireType.I64: {
      const value = readPbI64(r) ?? unexpectedEof();
      return { fieldNumber, wireType, value };
    }
    case PbWireType.LEN: {
      const value = readPbLenPrefix(r) ?? unexpectedEof();
      return { fieldNumber, wireType, value };
    }
    case PbWireType.I32: {
      const value = readPbI32(r) ?? unexpectedEof();
      return { fieldNumber, wireType, value };
    }
    case PbWireType.SGROUP:
    case PbWireType.EGROUP:
      return { fieldNumber, wireType };
    default:
      throw new TypeError(`Invalid wire type ${wireType}`);
  }
}

const writePbVarint = writeBigVarInt64LESync;
const writePbI32 = writeInt32LESync;
const writePbI64 = writeBigInt64LESync;

function writePbLenPrefix(w: BufferWriter, value: Uint8Array): undefined {
  if (value.length > 0x7fffffff) {
    throw new RangeError("Length prefixed payload too long");
  }
  writePbVarint(w, encodePbInt32(value.length));
  w.write(value);
}

export function writePbRecord(w: BufferWriter, record: PbRecord): undefined {
  const { fieldNumber, wireType } = record;
  writePbVarint(w, encodePbUint32((fieldNumber << 3) | wireType));
  switch (wireType) {
    case PbWireType.VARINT:
      writePbVarint(w, record.value);
      break;
    case PbWireType.I64:
      writePbI64(w, record.value);
      break;
    case PbWireType.LEN:
      writePbLenPrefix(w, record.value);
      break;
    case PbWireType.SGROUP:
    case PbWireType.EGROUP:
      break;
    case PbWireType.I32:
      writePbI32(w, record.value);
      break;
    default:
      throw new TypeError(`Invalid wire type ${wireType}`);
  }
}

export function encodePbInt32(value: number): bigint {
  return BigInt(value >>> 0);
}

export function encodePbInt64(value: bigint): bigint {
  return BigInt.asUintN(64, value);
}

export function encodePbUint32(value: number): bigint {
  return BigInt(value >>> 0);
}

export function encodePbBool(value: boolean): bigint {
  return encodePbInt32(value ? 1 : 0);
}

export function encodePbBytes(value: Uint8Array): Uint8Array {
  return value;
}

export function encodePbPackedUint32(from: readonly number[]): Uint8Array {
  const p = new BufferWriter();
  for (const value of from) {
    writePbVarint(p, encodePbUint32(value));
  }
  return p.bytes;
}

export function decodePbInt32(raw: bigint): number {
  return Number(BigInt.asIntN(32, raw));
}

export function decodePbInt64(raw: bigint): bigint {
  return BigInt.asIntN(64, raw);
}

export function decodePbUint32(raw: bigint): number {
  return Number(BigInt.asUintN(32, raw));
}

export function decodePbBool(raw: bigint): boolean {
  return decodePbInt32(raw) !== 0;
}

export function decodePbBytes(raw: Uint8Array): Uint8Array {
  return raw;
}

export function decodePbPackedUint32(
  raw: Uint8Array,
  into: number[],
): undefined {
  const p = new BufferReader(raw);
  for (;;) {
    const rawValue = readPbVarint(p);
    if (rawValue === null) {
      break;
    }
    into.push(decodePbUint32(rawValue));
  }
}
