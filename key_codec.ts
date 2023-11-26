import {
  BufferReader,
  BufferWriter,
  readBigUint64BESync,
  readFullSync,
  readUint8Sync,
  unexpectedEof,
  writeBigInt64BESync,
  writeInt16BESync,
  writeInt8Sync,
} from "./deps/binio.ts";
import { concat } from "./deps/std/bytes/concat.ts";

const littleEndian = new Uint8Array(Uint16Array.of(1).buffer)[0] === 1;
const encoder = new TextEncoder();
const decoder = new TextDecoder(undefined, { fatal: true, ignoreBOM: true });
const f64Buf = new Float64Array(1);
const i64Buf = new BigInt64Array(f64Buf.buffer);
const canonicalNaN = 0x7ff8000000000000n;
const signBit = -0x8000000000000000n;

function padStart(array: Uint8Array, len: number): Uint8Array {
  const result = new Uint8Array(len);
  result.set(array, result.length - array.length);
  return result;
}

function trimStart(array: Uint8Array): Uint8Array {
  const len = array.length;
  if (len === 0 || array[0] !== 0) {
    return array;
  }
  for (let i = 1; i < len; i++) {
    if (array[i] !== 0) {
      return array.subarray(i);
    }
  }
  return array.subarray(len);
}

function encodeBigIntBE(value: bigint): {
  sign: boolean;
  bytes: Uint8Array;
} {
  let words = new BigUint64Array(1);
  let len = 0;
  let sign = false;
  if (value < 0n) {
    sign = true;
    value = -value;
  }
  while (value) {
    if (words.length === len) {
      const realloc = new BigUint64Array(len * 2);
      realloc.set(words);
      words = realloc;
    }
    words[len++] = value;
    value >>= 64n;
  }
  words = words.subarray(0, len);
  let bytes = new Uint8Array(words.buffer, 0, words.byteLength);
  if (littleEndian) {
    bytes.reverse();
  } else {
    words.reverse();
  }
  bytes = trimStart(bytes);
  return { sign, bytes };
}

function decodeBigIntBE(sign: boolean, bytes: Uint8Array): bigint {
  bytes = padStart(bytes, Math.ceil(bytes.length / 8) * 8);
  const words = new BigUint64Array(bytes.buffer);
  if (littleEndian) {
    bytes.reverse();
    words.reverse();
  }
  let value = 0n;
  for (const word of words) {
    value = (value << 64n) | word;
  }
  if (sign) {
    value = -value;
  }
  return value;
}

function readKvBytes(r: BufferReader): {
  value: Uint8Array;
  remaining: Uint8Array;
} {
  const bytes = r.readAll();
  const chunks: Uint8Array[] = [];
  let len = 0;
  for (;;) {
    const nulPos = bytes.indexOf(0, len);
    if (nulPos === -1) {
      unexpectedEof();
    }
    if (bytes[nulPos + 1] !== 0xff) {
      chunks.push(bytes.subarray(len, nulPos));
      len = nulPos + 1;
      break;
    }
    chunks.push(bytes.subarray(len, nulPos + 1));
    len = nulPos + 2;
  }
  return { value: concat(chunks), remaining: bytes.subarray(len) };
}

function readKvString(r: BufferReader): {
  value: string;
  remaining: Uint8Array;
} {
  const { value, remaining } = readKvBytes(r);
  return { value: decoder.decode(value), remaining };
}

function readKvInt(r: BufferReader, tag: number): bigint {
  if (tag < 0x0b || tag > 0x1d) {
    throw new TypeError(`Invalid key part tag ${tag}`);
  }
  let sign = false;
  let len = 0;
  if (tag < 0x14) {
    sign = true;
    len = 0x14 - tag;
    if (len > 8) {
      len = ~(readUint8Sync(r) ?? unexpectedEof()) & 0xff;
    }
  } else {
    len = tag - 0x14;
    if (len > 8) {
      len = readUint8Sync(r) ?? unexpectedEof();
    }
  }
  if (len === 0) {
    return 0n;
  }
  const bytes = readFullSync(r, new Uint8Array(len)) ?? unexpectedEof();
  if (sign) {
    for (let i = 0; i < len; i++) {
      bytes[i] ^= 0xff;
    }
  }
  return decodeBigIntBE(sign, bytes);
}

function readKvFloat(r: BufferReader): number {
  const bits = BigInt.asIntN(64, readBigUint64BESync(r) ?? unexpectedEof());
  i64Buf[0] = bits ^ (~(bits >> 63n) | signBit);
  const value = f64Buf[0];
  f64Buf[0] = 0;
  return value;
}

function writeKvBytes(w: BufferWriter, value: Uint8Array): undefined {
  for (;;) {
    const nulPos = value.indexOf(0);
    if (nulPos === -1) {
      w.write(value);
      writeInt8Sync(w, 0);
      break;
    }
    w.write(value.subarray(0, nulPos + 1));
    writeInt8Sync(w, 0xff);
    value = value.subarray(nulPos + 1);
  }
}

function writeKvString(w: BufferWriter, value: string): undefined {
  writeKvBytes(w, encoder.encode(value));
}

function writeKvInt(w: BufferWriter, value: bigint): undefined {
  const { sign, bytes } = encodeBigIntBE(value);
  const len = bytes.length;
  if (len > 255) {
    throw new RangeError(
      `Size of BigInt key part must not exceed 255 bytes (got ${len} bytes)`,
    );
  }
  if (sign) {
    if (len <= 8) {
      writeInt8Sync(w, 0x14 - len);
    } else {
      writeInt16BESync(w, 0x0bff & ~len);
    }
    for (let i = 0; i < len; i++) {
      bytes[i] ^= 0xff;
    }
  } else {
    if (len <= 8) {
      writeInt8Sync(w, 0x14 + len);
    } else {
      writeInt16BESync(w, 0x1d00 | len);
    }
  }
  w.write(bytes);
}

function writeKvFloat(w: BufferWriter, value: number): undefined {
  f64Buf[0] = value;
  let bits = i64Buf[0];
  f64Buf[0] = 0;
  if (Number.isNaN(value)) {
    bits = (bits & signBit) | canonicalNaN;
  }
  writeBigInt64BESync(w, bits ^ ((bits >> 63n) | signBit));
}

export function encodeKey(key: Deno.KvKey): Uint8Array {
  const w = new BufferWriter();
  for (const part of key) {
    switch (typeof part) {
      case "string":
        writeInt8Sync(w, 0x02);
        writeKvString(w, part);
        break;
      case "number":
        writeInt8Sync(w, 0x21);
        writeKvFloat(w, part);
        break;
      case "bigint":
        writeKvInt(w, part);
        break;
      case "boolean":
        writeInt8Sync(w, part ? 0x27 : 0x26);
        break;
      default:
        writeInt8Sync(w, 0x01);
        writeKvBytes(w, part);
        break;
    }
  }
  return w.bytes;
}

export interface RangeKey {
  key: Deno.KvKey;
  mode: 1 | 0 | -1;
}

export function decodeKeyImpl(
  bytes: Uint8Array,
  allowRange: boolean,
): RangeKey {
  const key: Deno.KvKeyPart[] = [];
  let mode: 1 | 0 | -1 = 0;
  let r = new BufferReader(bytes);
  for (;;) {
    const tag = readUint8Sync(r);
    if (tag === null) {
      break;
    }
    if (allowRange) {
      switch (tag) {
        case 0x00:
          mode = 1;
          break;
        case 0xff:
          mode = -1;
          break;
      }
      if (mode) {
        const bytes = r.readAll();
        if (bytes.length === 0) {
          break;
        }
        r = new BufferReader(bytes);
      }
    }
    switch (tag) {
      case 0x01: {
        const { value, remaining } = readKvBytes(r);
        key.push(value);
        r = new BufferReader(remaining);
        break;
      }
      case 0x02: {
        const { value, remaining } = readKvString(r);
        key.push(value);
        r = new BufferReader(remaining);
        break;
      }
      case 0x21:
        key.push(readKvFloat(r));
        break;
      case 0x26:
        key.push(false);
        break;
      case 0x27:
        key.push(true);
        break;
      default:
        key.push(readKvInt(r, tag));
        break;
    }
  }
  return { key, mode };
}

export function decodeRangeKey(bytes: Uint8Array): RangeKey {
  return decodeKeyImpl(bytes, true);
}

export function decodeKey(bytes: Uint8Array): Deno.KvKey {
  return decodeKeyImpl(bytes, false).key;
}
