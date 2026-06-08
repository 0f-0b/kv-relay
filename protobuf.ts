import {
  readBigInt64LESync,
  readBigUint64LESync,
  readBigVarUint64LESync,
  readFloat32LESync,
  readFloat64LESync,
  readInt32LESync,
  readUint32LESync,
  readVarUint32LESync,
  type Uint8ArrayReader,
  unexpectedEof,
  writeBigInt64LESync,
  writeBigVarUint64LESync,
  writeFloat32LESync,
  writeFloat64LESync,
  writeInt32LESync,
  writeInt8Sync,
  type WriterSync,
  writeVarUint32LESync,
} from "./deps/binio.ts";

const { asIntN, asUintN } = BigInt;
const encoder = new TextEncoder();
const decoder = new TextDecoder(undefined, { fatal: true, ignoreBOM: true });

function skipExact(r: Uint8ArrayReader, n: number): undefined {
  if (r.skip(n) !== n) {
    unexpectedEof();
  }
}

export const VARINT = 0;
export const I64 = 1;
export const LEN = 2;
export const SGROUP = 3;
export const EGROUP = 4;
export const I32 = 5;
export type WireType =
  | typeof VARINT
  | typeof I64
  | typeof LEN
  | typeof SGROUP
  | typeof EGROUP
  | typeof I32;

export interface Tag {
  fieldNumber: number;
  wireType: WireType;
}

export function assertWireType(
  fieldNumber: number,
  actual: WireType,
  expected: WireType,
): undefined {
  if (actual !== expected) {
    throw new TypeError(
      `Invalid wire type for field ${fieldNumber}: expected ${expected}, got ${actual}`,
    );
  }
}

export function skipPayload(
  r: Uint8ArrayReader,
  fieldNumber: number,
  wireType: WireType,
): undefined {
  switch (wireType) {
    case VARINT:
      readUint64(r);
      break;
    case I64:
      skipExact(r, 8);
      break;
    case LEN:
      readBytes(r);
      break;
    case I32:
      skipExact(r, 4);
      break;
    case SGROUP:
      for (;;) {
        const tag = readTag(r) ?? unexpectedEof();
        if (tag.wireType === EGROUP) {
          if (tag.fieldNumber !== fieldNumber) {
            throw new TypeError("Malformed group");
          }
          break;
        }
        skipPayload(r, tag.fieldNumber, tag.wireType);
      }
      break;
    default:
      throw new TypeError(`Cannot skip wire type ${wireType}`);
  }
}

export function readTag(r: Uint8ArrayReader): Tag | null {
  const tag = readVarUint32LESync(r);
  if (tag === null) {
    return null;
  }
  const fieldNumber = tag >>> 3;
  if (fieldNumber === 0) {
    throw new TypeError("Field number cannot be 0");
  }
  const wireType = tag & 7;
  if (wireType > 5) {
    throw new TypeError(`Invalid wire type ${wireType}`);
  }
  return { fieldNumber, wireType: wireType as WireType };
}

export function readDouble(r: Uint8ArrayReader): number {
  return readFloat64LESync(r) ?? unexpectedEof();
}

export function readFloat(r: Uint8ArrayReader): number {
  return readFloat32LESync(r) ?? unexpectedEof();
}

export function readInt32(r: Uint8ArrayReader): number {
  return Number(asIntN(32, readUint64(r)));
}

export function readInt64(r: Uint8ArrayReader): bigint {
  return asIntN(64, readUint64(r));
}

export function readUint32(r: Uint8ArrayReader): number {
  return Number(asUintN(32, readUint64(r)));
}

export function readUint64(r: Uint8ArrayReader): bigint {
  return readBigVarUint64LESync(r) ?? unexpectedEof();
}

export function readSint32(r: Uint8ArrayReader): number {
  const value = readUint32(r);
  return (value >>> 1) ^ -(value & 1);
}

export function readSint64(r: Uint8ArrayReader): bigint {
  const value = readUint64(r);
  return (value >> 1n) ^ asIntN(1, value);
}

export function readFixed32(r: Uint8ArrayReader): number {
  return readUint32LESync(r) ?? unexpectedEof();
}

export function readFixed64(r: Uint8ArrayReader): bigint {
  return readBigUint64LESync(r) ?? unexpectedEof();
}

export function readSfixed32(r: Uint8ArrayReader): number {
  return readInt32LESync(r) ?? unexpectedEof();
}

export function readSfixed64(r: Uint8ArrayReader): bigint {
  return readBigInt64LESync(r) ?? unexpectedEof();
}

export function readBool(r: Uint8ArrayReader): boolean {
  return !!readUint64(r);
}

export function readString(r: Uint8ArrayReader): string {
  return decoder.decode(readBytes(r));
}

export function readBytes(r: Uint8ArrayReader): Uint8Array<ArrayBuffer> {
  const len = readUint32(r);
  const bytes = r.remaining;
  if (len > bytes.length) {
    unexpectedEof();
  }
  r.skip(len);
  return bytes.subarray(0, len);
}

export function writeTag(w: WriterSync, tag: Tag): undefined {
  const { fieldNumber, wireType } = tag;
  writeUint32(w, (fieldNumber << 3) | wireType);
}

export const writeDouble = writeFloat64LESync;
export const writeFloat = writeFloat32LESync;
export const writeInt32 = writeVarUint32LESync;
export const writeInt64 = writeBigVarUint64LESync;
export const writeUint32 = writeVarUint32LESync;
export const writeUint64 = writeBigVarUint64LESync;

export function writeSint32(w: WriterSync, value: number): undefined {
  value |= 0;
  return writeUint32(w, (value << 1) ^ (value >> 31));
}

export function writeSint64(w: WriterSync, value: bigint): undefined {
  value = asIntN(64, value);
  return writeUint64(w, (value << 1n) ^ (value >> 63n));
}

export const writeFixed32 = writeInt32LESync;
export const writeFixed64 = writeBigInt64LESync;
export const writeSfixed32 = writeInt32LESync;
export const writeSfixed64 = writeBigInt64LESync;

export function writeBool(w: WriterSync, value: boolean): undefined {
  return writeInt8Sync(w, value ? 1 : 0);
}

export function writeString(w: WriterSync, value: string): undefined {
  return writeBytes(w, encoder.encode(value));
}

export function writeBytes(
  w: WriterSync,
  value: Uint8Array<ArrayBuffer>,
): undefined {
  if (value.length > 0xffffffff) {
    throw new RangeError("Length prefixed payload is too long");
  }
  writeUint32(w, value.length);
  w.write(value);
}
