import { describe, expect, it } from "vitest";
import { cloudflare } from "./cloudflare.js";

/** Build an SSE body from `[eventName, data]` pairs. */
const sse = (events: [string, string][]): ReadableStream<Uint8Array> => {
  const text = events
    .map(([name, data]) => `event: ${name}\ndata: ${data}\n\n`)
    .join("");
  return new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(text));
      controller.close();
    },
  });
};

const b64 = (s: string): string => Buffer.from(s, "utf8").toString("base64");

interface Call {
  url: string;
  method: string;
  body?: string;
  headers: Record<string, string>;
}

/**
 * A fake bridge that records calls and replies with scripted exec streams.
 *
 * `create()` itself runs an exec (the worktree `mkdir`), so the script is
 * applied only once a test calls `setExec` — otherwise every scripted failure
 * would blow up during creation instead of on the call under test.
 */
const fakeBridge = () => {
  const calls: Call[] = [];
  let script: [string, string][] = [["exit", '{"exit_code":0}']];

  const fetchImpl = (async (input: any, init: any = {}) => {
    const url = String(input);
    calls.push({
      url,
      method: init.method ?? "GET",
      body: typeof init.body === "string" ? init.body : undefined,
      headers: (init.headers ?? {}) as Record<string, string>,
    });

    if (url.endsWith("/v1/sandbox") && init.method === "POST") {
      return new Response(JSON.stringify({ id: "sb-123" }), { status: 200 });
    }
    if (url.endsWith("/session") && init.method === "POST") {
      return new Response(JSON.stringify({ id: "sess-abc" }), { status: 200 });
    }
    if (url.endsWith("/exec")) {
      return new Response(sse(script), { status: 200 });
    }
    if (url.includes("/file/")) {
      return new Response("file-contents", { status: 200 });
    }
    return new Response("", { status: 200 });
  }) as unknown as typeof globalThis.fetch;

  return {
    calls,
    fetchImpl,
    setExec: (events: [string, string][]): void => {
      script = events;
    },
  };
};

const makeProvider = (bridge: ReturnType<typeof fakeBridge>, overrides = {}) =>
  cloudflare({
    apiUrl: "https://bridge.example.com/",
    apiKey: "test-key",
    fetch: bridge.fetchImpl,
    ...overrides,
  });

describe("cloudflare provider", () => {
  it("is an isolated provider named cloudflare", () => {
    const provider = makeProvider(fakeBridge());
    expect(provider.tag).toBe("isolated");
    expect(provider.name).toBe("cloudflare");
  });

  it("creates a sandbox, opens a session carrying env, and authenticates", async () => {
    const bridge = fakeBridge();
    await makeProvider(bridge).create({ env: { FOO: "bar" } });

    const create = bridge.calls.find((c) => c.url.endsWith("/v1/sandbox"));
    expect(create?.method).toBe("POST");
    expect(create?.headers.Authorization).toBe("Bearer test-key");

    // env has nowhere to live on /exec, so it must ride on the session.
    const session = bridge.calls.find((c) => c.url.endsWith("/session"));
    expect(JSON.parse(session?.body ?? "{}")).toMatchObject({
      env: { FOO: "bar" },
    });
  });

  it("trims a trailing slash off the bridge url", async () => {
    const bridge = fakeBridge();
    await makeProvider(bridge).create({ env: {} });
    expect(bridge.calls[0]?.url).toBe("https://bridge.example.com/v1/sandbox");
  });

  it("rejects a missing bridge url instead of building a broken one", async () => {
    const previous = process.env.SANDBOX_API_URL;
    delete process.env.SANDBOX_API_URL;
    try {
      await expect(
        cloudflare({ fetch: fakeBridge().fetchImpl }).create({ env: {} }),
      ).rejects.toThrow(/requires a bridge URL/);
    } finally {
      if (previous !== undefined) process.env.SANDBOX_API_URL = previous;
    }
  });

  it("streams stdout line by line and returns the exit code", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.setExec([
      ["stdout", b64("first\nsecond\n")],
      ["stdout", b64("third\n")],
      ["exit", '{"exit_code":0}'],
    ]);

    const lines: string[] = [];
    const result = await handle.exec("echo hi", {
      onLine: (l) => lines.push(l),
    });

    expect(lines).toEqual(["first", "second", "third"]);
    expect(result.exitCode).toBe(0);
    expect(result.stdout).toBe("first\nsecond\nthird");
  });

  it("emits a trailing unterminated line rather than dropping it", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.setExec([
      ["stdout", b64("no trailing newline")],
      ["exit", '{"exit_code":0}'],
    ]);

    const lines: string[] = [];
    await handle.exec("echo -n hi", { onLine: (l) => lines.push(l) });
    expect(lines).toEqual(["no trailing newline"]);
  });

  it("reports a non-zero exit code", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.setExec([
      ["stderr", b64("boom")],
      ["exit", '{"exit_code":17}'],
    ]);
    const result = await handle.exec("false");
    expect(result.exitCode).toBe(17);
    expect(result.stderr).toBe("boom");
  });

  it("throws on an error event rather than reporting success", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.setExec([
      ["error", '{"error":"container died","code":"exec_error"}'],
    ]);
    await expect(handle.exec("whatever")).rejects.toThrow(/container died/);
  });

  it("throws when the stream ends with no exit event", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.setExec([["stdout", b64("partial\n")]]);
    await expect(handle.exec("hang")).rejects.toThrow(/without an exit event/);
  });

  it("sends argv as an sh -lc script with the worktree as cwd", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.calls.length = 0;

    await handle.exec("git status");
    const exec = bridge.calls.find((c) => c.url.endsWith("/exec"));
    expect(JSON.parse(exec?.body ?? "{}")).toMatchObject({
      argv: ["sh", "-lc", "git status"],
      cwd: "/workspace/repo",
    });
    expect(exec?.headers["Session-Id"]).toBe("sess-abc");
  });

  it("prefixes sudo when asked", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.calls.length = 0;

    await handle.exec("apt-get install -y git", { sudo: true });
    const exec = bridge.calls.find((c) => c.url.endsWith("/exec"));
    expect(JSON.parse(exec?.body ?? "{}").argv[2]).toBe(
      "sudo apt-get install -y git",
    );
  });

  it("stages stdin as a file and redirects it, then cleans up", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.calls.length = 0;

    await handle.exec("pi -p", { stdin: "a very long prompt" });

    // The prompt is staged outside the worktree so an agent's `git add -A`
    // cannot sweep it into a commit.
    const put = bridge.calls.find((c) => c.method === "PUT");
    expect(put?.url).toMatch(
      /\/v1\/sandbox\/sb-123\/file\/workspace\/\.sandcastle-stage\/[0-9a-f-]+$/,
    );

    const scripts = bridge.calls
      .filter((c) => c.url.endsWith("/exec"))
      .map((c) => JSON.parse(c.body ?? "{}").argv[2] as string);
    expect(scripts[0]).toBe("mkdir -p '/workspace/.sandcastle-stage'");
    expect(scripts[1]).toMatch(
      /^pi -p < '\/workspace\/\.sandcastle-stage\/[0-9a-f-]+'$/,
    );

    // The staged file must not outlive the command.
    expect(scripts[2]).toMatch(
      /^rm -f '\/workspace\/\.sandcastle-stage\/[0-9a-f-]+'$/,
    );
  });

  it("copies a single file in and a file out", async () => {
    const { mkdtemp, writeFile, readFile } = await import("node:fs/promises");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");

    const dir = await mkdtemp(join(tmpdir(), "cf-sandbox-test-"));
    const src = join(dir, "in.txt");
    await writeFile(src, "hello");

    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.calls.length = 0;

    await handle.copyIn(src, "/workspace/repo/in.txt");
    expect(bridge.calls.find((c) => c.method === "PUT")?.url).toBe(
      "https://bridge.example.com/v1/sandbox/sb-123/file/workspace/repo/in.txt",
    );

    const dest = join(dir, "nested", "out.txt");
    await handle.copyFileOut("/workspace/repo/out.txt", dest);
    expect(await readFile(dest, "utf8")).toBe("file-contents");
  });

  it("stages a copy-in bound for outside /workspace and moves it into place", async () => {
    const { mkdtemp, writeFile } = await import("node:fs/promises");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");

    const dir = await mkdtemp(join(tmpdir(), "cf-sandbox-test-"));
    const src = join(dir, "repo.bundle");
    await writeFile(src, "bundle-bytes");

    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.calls.length = 0;

    // Sandcastle's sync-in lands the bundle under `mktemp -d`, i.e. /tmp,
    // which the bridge's file routes refuse.
    await handle.copyIn(src, "/tmp/sandcastle-abc123/repo.bundle");

    const put = bridge.calls.find((c) => c.method === "PUT");
    expect(put?.url).toMatch(
      /\/v1\/sandbox\/sb-123\/file\/workspace\/\.sandcastle-stage\/[0-9a-f-]+$/,
    );

    const scripts = bridge.calls
      .filter((c) => c.url.endsWith("/exec"))
      .map((c) => JSON.parse(c.body ?? "{}").argv[2] as string);
    expect(scripts[0]).toBe("mkdir -p '/workspace/.sandcastle-stage'");
    expect(scripts[1]).toMatch(
      /^mkdir -p '\/tmp\/sandcastle-abc123' && mv '\/workspace\/\.sandcastle-stage\/[0-9a-f-]+' '\/tmp\/sandcastle-abc123\/repo\.bundle'$/,
    );
  });

  it("reads a file outside /workspace by staging it inside first", async () => {
    const { mkdtemp, readFile } = await import("node:fs/promises");
    const { tmpdir } = await import("node:os");
    const { join } = await import("node:path");

    const dir = await mkdtemp(join(tmpdir(), "cf-sandbox-test-"));
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.calls.length = 0;

    const dest = join(dir, "0001.patch");
    await handle.copyFileOut("/tmp/sandcastle-patches-xyz/0001.patch", dest);
    expect(await readFile(dest, "utf8")).toBe("file-contents");

    const scripts = bridge.calls
      .filter((c) => c.url.endsWith("/exec"))
      .map((c) => JSON.parse(c.body ?? "{}").argv[2] as string);
    expect(scripts[0]).toMatch(
      /^mkdir -p '\/workspace\/\.sandcastle-stage' && cp '\/tmp\/sandcastle-patches-xyz\/0001\.patch' '\/workspace\/\.sandcastle-stage\/[0-9a-f-]+'$/,
    );
    const get = bridge.calls.find((c) => c.method === "GET");
    expect(get?.url).toMatch(
      /\/v1\/sandbox\/sb-123\/file\/workspace\/\.sandcastle-stage\/[0-9a-f-]+$/,
    );
    // The staged copy must not outlive the read.
    expect(scripts[1]).toMatch(
      /^rm -f '\/workspace\/\.sandcastle-stage\/[0-9a-f-]+'$/,
    );
  });

  it("destroys the sandbox on close", async () => {
    const bridge = fakeBridge();
    const handle = await makeProvider(bridge).create({ env: {} });
    bridge.calls.length = 0;

    await handle.close();
    expect(bridge.calls[0]).toMatchObject({
      url: "https://bridge.example.com/v1/sandbox/sb-123",
      method: "DELETE",
    });
  });
});
