import {
  Buffer,
  readBigUint64LESync,
  readBigVarUint64LESync,
  readFullSync,
  readUint32LESync,
  unexpectedEof,
  writeBigInt64LESync,
  writeBigVarInt64LESync,
  writeInt32LESync,
} from "./deps/binio.ts";

export { Buffer };
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

function readPbVarint(r: Buffer): bigint {
  return readBigVarUint64LESync(r) ?? unexpectedEof();
}

function readPbI32(r: Buffer): number {
  return readUint32LESync(r) ?? unexpectedEof();
}

function readPbI64(r: Buffer): bigint {
  return readBigUint64LESync(r) ?? unexpectedEof();
}

function readPbLenPrefix(r: Buffer): Uint8Array {
  const len = decodePbInt32(readPbVarint(r));
  if (len < 0) {
    throw new RangeError("Length prefixed payload too long");
  }
  return readFullSync(r, new Uint8Array(len)) ?? unexpectedEof();
}

export function readPbRecord(r: Buffer): PbRecord {
  const tag = decodePbUint32(readPbVarint(r));
  const fieldNumber = tag >>> 3;
  const wireType = tag & 7;
  switch (wireType) {
    case PbWireType.VARINT:
      return { fieldNumber, wireType, value: readPbVarint(r) };
    case PbWireType.I64:
      return { fieldNumber, wireType, value: readPbI64(r) };
    case PbWireType.LEN:
      return { fieldNumber, wireType, value: readPbLenPrefix(r) };
    case PbWireType.SGROUP:
      return { fieldNumber, wireType };
    case PbWireType.EGROUP:
      return { fieldNumber, wireType };
    case PbWireType.I32:
      return { fieldNumber, wireType, value: readPbI32(r) };
    default:
      throw new TypeError(`Invalid wire type ${wireType}`);
  }
}

function writePbVarint(w: Buffer, value: bigint): undefined {
  writeBigVarInt64LESync(w, value);
}

function writePbI32(w: Buffer, value: number): undefined {
  writeInt32LESync(w, value);
}

function writePbI64(w: Buffer, value: bigint): undefined {
  writeBigInt64LESync(w, value);
}

function writePbLenPrefix(w: Buffer, value: Uint8Array): undefined {
  if (value.length > 0x7fffffff) {
    throw new RangeError("Length prefixed payload too long");
  }
  writePbVarint(w, encodePbInt32(value.length));
  w.write(value);
}

export function writePbRecord(w: Buffer, record: PbRecord): undefined {
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
  const p = new Buffer();
  for (const value of from) {
    writePbVarint(p, encodePbUint32(value));
  }
  return p.bytes({ copy: false });
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
  const p = new Buffer(raw);
  while (!p.empty()) {
    into.push(decodePbUint32(readPbVarint(p)));
  }
}
