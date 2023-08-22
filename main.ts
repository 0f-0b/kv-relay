#!/usr/bin/env -S deno run --unstable --allow-read --allow-write --allow-net

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

const kv = await Deno.openKv(path);
try {
  const relay = new KvRelay(kv);
  const ephemeralTokens = new Set<string>();
  const isEphemeralTokenValid = (token: string | null) =>
    token !== null && ephemeralTokens.has(token);
  const server = Deno.serve({ hostname: host, port, signal }, async (req) => {
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
            { url: url.origin, consistency: "strong" },
          ],
          token: ephemeralToken,
          expiresAt: new Date(ephemeralTokenExpireTime),
        });
      }
      case "/snapshot_read":
        if (req.method !== "POST") {
          return methodNotAllowed("POST");
        }
        if (!isEphemeralTokenValid(getToken(req.headers))) {
          return unauthorized();
        }
        try {
          return new Response(
            await relay.snapshotRead(new Uint8Array(await req.arrayBuffer())),
          );
        } catch (e: unknown) {
          console.error(e);
          return badRequest();
        }
      case "/atomic_write":
        if (req.method !== "POST") {
          return methodNotAllowed("POST");
        }
        if (!isEphemeralTokenValid(getToken(req.headers))) {
          return unauthorized();
        }
        try {
          return new Response(
            await relay.atomicWrite(new Uint8Array(await req.arrayBuffer())),
          );
        } catch (e: unknown) {
          console.error(e);
          return badRequest();
        }
      default:
        return new Response(null, { status: 404 });
    }
  });
  await server.finished;
} finally {
  kv.close();
}
