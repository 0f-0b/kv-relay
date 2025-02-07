#!/usr/bin/env -S deno run --unstable-kv --allow-read --allow-write --allow-net

import { Command } from "./deps/cliffy/command.ts";

import { signal } from "./interrupt_signal.ts";
import { KvRelay } from "./mod.ts";

const {
  options: { port, host, databaseId, accessToken, ephemeralTokenTtl },
  args: [path],
} = await new Command()
  .name("kv-relay")
  .usage("[options] [path]")
  .option(
    "--host <host:string>",
    "Server hostname.",
    { default: "0.0.0.0" },
  )
  .option(
    "-p, --port <port:integer>",
    "Server port.",
    { default: 10159 },
  )
  .option(
    "--database-id <uuid:string>",
    "UUID of the database.",
    { required: true },
  )
  .option(
    "--access-token <token:string>",
    "Access token.",
    { required: true },
  )
  .option(
    "--ephemeral-token-ttl <ms:integer>",
    "Milliseconds an ephemeral token is valid for.",
    { default: 3600000 },
  )
  .arguments("[path:file]")
  .error((error, cmd) => {
    cmd.showHelp();
    console.error(
      "%cerror%c:",
      "color: red; font-weight: bold",
      "",
      error.message,
    );
    Deno.exit(2);
  })
  .parse();

function getToken(headers: Headers): string | null {
  const authorization = headers.get("authorization");
  if (authorization === null || !authorization.startsWith("Bearer ")) {
    return null;
  }
  return authorization.substring("Bearer ".length);
}

function methodNotAllowed(...allowed: string[]): Response {
  return new Response(null, {
    status: 405,
    headers: [
      ["allow", allowed.join(", ")],
    ],
  });
}

function badRequest(): Response {
  return new Response(null, { status: 400 });
}

function unauthorized(): Response {
  return new Response(null, {
    status: 401,
    headers: [
      ["www-authenticate", "Bearer"],
    ],
  });
}

using kv = await Deno.openKv(path);
const relay = new KvRelay(kv);
const ephemeralTokens = new Set<string>();
const isEphemeralTokenValid = (token: string | null) =>
  token !== null && ephemeralTokens.has(token);
const server = Deno.serve({ hostname: host, port }, async (req) => {
  const url = new URL(req.url);
  switch (url.pathname) {
    case "/": {
      if (req.method !== "POST") {
        return methodNotAllowed("POST");
      }
      if (getToken(req.headers) !== accessToken) {
        return unauthorized();
      }
      let ephemeralToken: string;
      do {
        ephemeralToken = crypto.randomUUID();
      } while (ephemeralTokens.has(ephemeralToken));
      ephemeralTokens.add(ephemeralToken);
      const ephemeralTokenExpireTime = Date.now() + ephemeralTokenTtl;
      Deno.unrefTimer(setTimeout(
        () => ephemeralTokens.delete(ephemeralToken),
        ephemeralTokenTtl,
      ));
      return Response.json({
        version: 1,
        databaseId,
        endpoints: [
          { url: new URL("/kv", url), consistency: "strong" },
        ],
        token: ephemeralToken,
        expiresAt: new Date(ephemeralTokenExpireTime),
      });
    }
    case "/kv/snapshot_read":
      if (req.method !== "POST") {
        return methodNotAllowed("POST");
      }
      if (!isEphemeralTokenValid(getToken(req.headers))) {
        return unauthorized();
      }
      try {
        return new Response(
          await relay.snapshotRead(
            await req.bytes() as Uint8Array<ArrayBuffer>,
          ),
        );
      } catch (e) {
        console.error(e);
        return badRequest();
      }
    case "/kv/atomic_write":
      if (req.method !== "POST") {
        return methodNotAllowed("POST");
      }
      if (!isEphemeralTokenValid(getToken(req.headers))) {
        return unauthorized();
      }
      try {
        return new Response(
          await relay.atomicWrite(await req.bytes() as Uint8Array<ArrayBuffer>),
        );
      } catch (e) {
        console.error(e);
        return badRequest();
      }
    case "/kv/watch":
      if (req.method !== "POST") {
        return methodNotAllowed("POST");
      }
      if (!isEphemeralTokenValid(getToken(req.headers))) {
        return unauthorized();
      }
      try {
        return new Response(
          relay.watch(await req.bytes() as Uint8Array<ArrayBuffer>),
        );
      } catch (e) {
        console.error(e);
        return badRequest();
      }
    default:
      return new Response(null, { status: 404 });
  }
});
const onAbort = () => server.shutdown();
signal.addEventListener("abort", onAbort, { once: true });
try {
  await server.finished;
} finally {
  signal.removeEventListener("abort", onAbort);
}
