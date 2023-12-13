import { decodeHex, encodeHex } from "./deps/std/encoding/hex.ts";

import {
  type AtomicWriteOutput,
  AtomicWriteStatus,
  decodeAtomicWrite,
  decodeSnapshotRead,
  decodeWatch,
  encodeAtomicWriteOutput,
  encodeSnapshotReadOutput,
  encodeWatchOutput,
  type KvEntry,
  type KvValue,
  MutationType,
  type SnapshotReadOutput,
  SnapshotReadStatus,
  ValueEncoding,
} from "./datapath.proto.ts";
import { decodeKey, decodeRangeKey, encodeKey } from "./key_codec.ts";
import { deserialize, serialize } from "./v8_serializer.ts";

function serializeValue(value: unknown): KvValue {
  if (value instanceof Uint8Array) {
    return { data: value, encoding: ValueEncoding.VE_BYTES };
  }
  if (value instanceof Deno.KvU64) {
    const bytes = new Uint8Array(8);
    new DataView(bytes.buffer).setBigUint64(0, value.value, true);
    return { data: bytes, encoding: ValueEncoding.VE_LE64 };
  }
  return {
    data: serialize(value, { forStorage: true }),
    encoding: ValueEncoding.VE_V8,
  };
}

function serializeEntry(entry: Deno.KvEntry<unknown>): KvEntry {
  const key = encodeKey(entry.key);
  const { data: value, encoding } = serializeValue(entry.value);
  const versionstamp = decodeHex(entry.versionstamp ?? "");
  return { key, value, encoding, versionstamp };
}

function deserializeValue(value: KvValue | null): unknown {
  if (!value) {
    throw new TypeError("A value is required");
  }
  const { data, encoding } = value;
  switch (encoding) {
    case ValueEncoding.VE_V8:
      return deserialize(data, { forStorage: true });
    case ValueEncoding.VE_LE64:
      if (data.length !== 8) {
        throw new TypeError("Size of LE64 encoded value must be 8 bytes");
      }
      return new Deno.KvU64(
        new DataView(data.buffer, data.byteOffset, data.byteLength)
          .getBigUint64(0, true),
      );
    case ValueEncoding.VE_BYTES:
      return data;
    default:
      throw new TypeError(`Unknown value encoding ${encoding}`);
  }
}

function deserializeKvU64(value: KvValue | null): bigint {
  const u64 = deserializeValue(value);
  if (!(u64 instanceof Deno.KvU64)) {
    throw new TypeError("Value is not a Deno.KvU64");
  }
  return u64.value;
}

type KvEnqueueOptions = Deno.AtomicOperation["enqueue"] extends
  (value: unknown, options?: infer T) => Deno.AtomicOperation ? T
  : KvEnqueueOptions;

export class KvRelay {
  readonly #kv: Deno.Kv;

  constructor(kv: Deno.Kv) {
    this.#kv = kv;
  }

  async snapshotRead(buf: Uint8Array): Promise<Uint8Array> {
    const kv = this.#kv;
    const req = decodeSnapshotRead(buf);
    const ranges = await Promise.all(req.ranges.map(async (range) => {
      const values: KvEntry[] = [];
      const start = decodeRangeKey(range.start);
      const end = decodeRangeKey(range.end);
      let selector: Deno.KvListSelector;
      if (start.mode === -1) {
        throw new TypeError("Unsupported selector");
      }
      if (start.mode === 1) {
        start.key = [...start.key, new Uint8Array()];
        start.mode = 0;
      }
      if (end.mode === 1) {
        end.key = [...end.key, new Uint8Array()];
        end.mode = 0;
      }
      if (end.mode === 0) {
        selector = { start: start.key, end: end.key };
      } else {
        selector = { start: start.key, prefix: end.key };
      }
      const options: Deno.KvListOptions = {};
      if (range.limit) {
        options.limit = range.limit;
      }
      if (range.reverse) {
        options.reverse = range.reverse;
      }
      console.log("kv.list(%o, %o);", selector, options);
      for await (const entry of kv.list(selector, options)) {
        values.push(serializeEntry(entry));
      }
      return { values };
    }));
    const res: SnapshotReadOutput = {
      ranges,
      read_disabled: false,
      read_is_strongly_consistent: true,
      status: SnapshotReadStatus.SR_SUCCESS,
    };
    return encodeSnapshotReadOutput(res);
  }

  async atomicWrite(buf: Uint8Array): Promise<Uint8Array> {
    const kv = this.#kv;
    const req = decodeAtomicWrite(buf);
    const op = (() => {
      console.group("kv.atomic()");
      try {
        const op = kv.atomic();
        const now = Date.now();
        for (const check of req.checks) {
          const key = decodeKey(check.key);
          const versionstamp = check.versionstamp.some(Boolean)
            ? encodeHex(check.versionstamp)
            : null;
          const checkArg: Deno.AtomicCheck = { key, versionstamp };
          console.log(".check(%o)", checkArg);
          op.check(checkArg);
        }
        for (const mutation of req.mutations) {
          const key = decodeKey(mutation.key);
          switch (mutation.mutation_type) {
            case MutationType.M_SET: {
              const value = deserializeValue(mutation.value);
              const options: { expireIn?: number } = {};
              if (mutation.expire_at_ms) {
                const expireAt = Number(mutation.expire_at_ms);
                options.expireIn = expireAt - now;
              }
              console.log(".set(%o, %o, %o)", key, value, options);
              op.set(key, value, options);
              break;
            }
            case MutationType.M_DELETE:
              console.log(".delete(%o)", key);
              op.delete(key);
              break;
            case MutationType.M_SUM: {
              const value = deserializeKvU64(mutation.value);
              console.log(".sum(%o, %o)", key, value);
              op.sum(key, value);
              break;
            }
            case MutationType.M_MAX: {
              const value = deserializeKvU64(mutation.value);
              console.log(".max(%o, %o)", key, value);
              op.max(key, value);
              break;
            }
            case MutationType.M_MIN: {
              const value = deserializeKvU64(mutation.value);
              console.log(".min(%o, %o)", key, value);
              op.min(key, value);
              break;
            }
            case MutationType.M_SET_SUFFIX_VERSIONSTAMPED_KEY: {
              const value = deserializeValue(mutation.value);
              const options: { expireIn?: number } = {};
              if (mutation.expire_at_ms) {
                const expireAt = Number(mutation.expire_at_ms);
                options.expireIn = expireAt - now;
              }
              const suffixedKey = [...key, kv.commitVersionstamp()];
              console.log(".set(%o, %o, %o)", suffixedKey, value, options);
              op.set(suffixedKey, value, options);
              break;
            }
            default:
              throw new TypeError(
                `Unknown mutation type ${mutation.mutation_type}`,
              );
          }
        }
        for (const enqueue of req.enqueues) {
          const value = deserializeValue({
            data: enqueue.payload,
            encoding: ValueEncoding.VE_V8,
          });
          const options: KvEnqueueOptions = {};
          if (enqueue.deadline_ms > now) {
            const deadline = Number(enqueue.deadline_ms);
            options.delay = deadline - now;
          }
          if (enqueue.keys_if_undelivered.length) {
            options.keysIfUndelivered = enqueue.keys_if_undelivered
              .map(decodeKey);
          }
          if (enqueue.backoff_schedule.length) {
            options.backoffSchedule = enqueue.backoff_schedule;
          }
          console.log(".enqueue(%o, %o)", value, options);
          op.enqueue(value, options);
        }
        console.log(".commit();");
        return op;
      } finally {
        console.groupEnd();
      }
    })();
    const res: AtomicWriteOutput = {
      status: AtomicWriteStatus.AW_SUCCESS,
      versionstamp: new Uint8Array(),
      failed_checks: [],
    };
    try {
      const result = await op.commit();
      if (result.ok) {
        res.versionstamp = decodeHex(result.versionstamp);
      } else {
        res.status = AtomicWriteStatus.AW_CHECK_FAILURE;
      }
    } catch {
      res.status = AtomicWriteStatus.AW_UNSPECIFIED;
    }
    return encodeAtomicWriteOutput(res);
  }

  watch(buf: Uint8Array): ReadableStream<Uint8Array> {
    const kv = this.#kv;
    const req = decodeWatch(buf);
    const keys = req.keys.map(({ key }) => decodeKey(key));
    const options = { raw: true };
    console.log("kv.watch(%o, %o);", keys, options);
    return kv.watch(keys, options).pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          const message = encodeWatchOutput({
            status: SnapshotReadStatus.SR_SUCCESS,
            keys: chunk.map((entry) => ({
              changed: true,
              entry_if_changed: entry.versionstamp === null
                ? null
                : serializeEntry(entry),
            })),
          });
          const header = new Uint8Array(4);
          new DataView(header.buffer).setUint32(0, message.length, true);
          controller.enqueue(header);
          controller.enqueue(message);
        },
      }),
    );
  }
}
