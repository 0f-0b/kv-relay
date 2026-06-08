#!/usr/bin/env -S deno run --unstable-kv --allow-import=jsr.io:443 --allow-read --allow-write --allow-net

import { Command } from "./deps/cliffy/command.ts";
import { generateSecret, jwtVerify, SignJWT } from "./deps/jose.ts";

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

function getBearerToken(headers: Headers): string | null {
  const authorization = headers.get("authorization");
  if (authorization === null || !authorization.startsWith("Bearer ")) {
    return null;
  }
  return authorization.substring("Bearer ".length);
}

function validateAccessToken(headers: Headers): boolean {
  return getBearerToken(headers) === accessToken;
}

const alg = "HS256";
const secret = await generateSecret(alg);

async function validateEphemeralToken(headers: Headers): Promise<boolean> {
  const token = getBearerToken(headers);
  if (token === null) {
    return false;
  }
  try {
    await jwtVerify(token, secret, { requiredClaims: ["exp"] });
  } catch {
    return false;
  }
  return true;
}

function badRequest(): Response {
  return new Response(null, { status: 400 });
}

function unauthorized(): Response {
  return new Response(null, {
    status: 401,
    headers: { "www-authenticate": "Bearer" },
  });
}

using kv = await Deno.openKv(path);
const relay = new KvRelay(kv);
const server = Deno.serve({ hostname: host, port }, async (req) => {
  if (req.method !== "POST") {
    return new Response(null, {
      status: 501,
      headers: { "connection": "close" },
    });
  }
  const url = new URL(req.url);
  switch (url.pathname) {
    case "/": {
      if (!validateAccessToken(req.headers)) {
        return unauthorized();
      }
      const expiresAt = new Date(Date.now() + ephemeralTokenTtl);
      const token = await new SignJWT()
        .setProtectedHeader({ alg })
        .setExpirationTime(expiresAt)
        .sign(secret);
      return Response.json({
        version: 1,
        databaseId,
        endpoints: [{ url: new URL("/kv", url), consistency: "strong" }],
        token,
        expiresAt,
      });
    }
    case "/kv/snapshot_read":
      if (!await validateEphemeralToken(req.headers)) {
        return unauthorized();
      }
      try {
        return new Response(await relay.snapshotRead(await req.bytes()));
      } catch (e) {
        console.error(e);
        return badRequest();
      }
    case "/kv/atomic_write":
      if (!await validateEphemeralToken(req.headers)) {
        return unauthorized();
      }
      try {
        return new Response(await relay.atomicWrite(await req.bytes()));
      } catch (e) {
        console.error(e);
        return badRequest();
      }
    case "/kv/watch":
      if (!await validateEphemeralToken(req.headers)) {
        return unauthorized();
      }
      try {
        return new Response(relay.watch(await req.bytes()));
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
