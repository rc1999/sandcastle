/**
 * Cloudflare Sandbox isolated sandbox provider.
 *
 * Runs agents in Cloudflare Sandboxes (Durable-Object-backed Linux containers)
 * via the [sandbox bridge](https://developers.cloudflare.com/sandbox/bridge/) —
 * a Worker you deploy to your own account that exposes the Sandbox SDK over
 * HTTP. The bridge is required: `@cloudflare/sandbox` only works *inside* a
 * Worker, so a Node process like Sandcastle cannot drive it directly.
 *
 * Unlike the Vercel and Daytona providers this has no peer dependency — the
 * bridge is a plain HTTP API, so `fetch` is the whole client.
 */

import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import { dirname, join, relative, posix } from "node:path";
import {
  createIsolatedSandboxProvider,
  type ExecResult,
  type IsolatedSandboxHandle,
  type IsolatedSandboxProvider,
} from "../SandboxProvider.js";
import { BoundedTail, MAX_TAIL_CHARS } from "../boundedTail.js";

/** Default worktree path. The bridge confines every path to `/workspace`. */
const CLOUDFLARE_REPO_PATH = "/workspace/repo";

/** Options for the Cloudflare Sandbox provider. */
export interface CloudflareOptions {
  /**
   * Base URL of your deployed sandbox bridge Worker, e.g.
   * `https://cloudflare-sandbox-bridge.your-subdomain.workers.dev`.
   * Falls back to the `SANDBOX_API_URL` environment variable.
   */
  readonly apiUrl?: string;

  /**
   * Bearer token for the bridge (`SANDBOX_API_KEY` secret on the Worker).
   * Falls back to the `SANDBOX_API_KEY` environment variable.
   *
   * The bridge skips auth entirely when its secret is unset, so this is
   * optional for local `wrangler dev` bridges.
   */
  readonly apiKey?: string;

  /**
   * Absolute path to the worktree inside the sandbox (default
   * `/workspace/repo`). Must sit under `/workspace` — the bridge rejects
   * anything else with a 403.
   */
  readonly worktreePath?: string;

  /** Environment variables injected by this provider. Merged at launch time with env resolver and agent provider env. */
  readonly env?: Record<string, string>;

  /** Per-command timeout in milliseconds, passed to the bridge as `timeout_ms`. */
  readonly timeoutMs?: number;

  /**
   * Maximum number of characters of streamed `exec` output retained per stream
   * (stdout and stderr) when an `onLine` callback is supplied (default: 64KiB).
   */
  readonly maxOutputTailChars?: number;

  /** Injectable `fetch`, for tests. Defaults to the global. */
  readonly fetch?: typeof globalThis.fetch;
}

/** Shell-quote a token for embedding in an `sh -lc` script. */
const shellEscape = (value: string): string =>
  `'${value.replace(/'/g, "'\\''")}'`;

/** Map an absolute sandbox path onto the bridge's `/file/*` route. */
const filePathToUrlSuffix = (sandboxPath: string): string =>
  sandboxPath.replace(/^\/+/, "");

/**
 * The bridge's `/file/*` routes refuse any path outside this tree with a 403.
 * Sandcastle's sync steps stage bundles and patches under `mktemp -d`, which
 * lands in `/tmp`, so files bound for anywhere else are landed in
 * {@link STAGE_DIR} and moved into place with a shell command.
 */
const WORKSPACE_ROOT = "/workspace";
const STAGE_DIR = `${WORKSPACE_ROOT}/.sandcastle-stage`;

const isUnderWorkspace = (sandboxPath: string): boolean =>
  sandboxPath === WORKSPACE_ROOT ||
  sandboxPath.startsWith(`${WORKSPACE_ROOT}/`);

interface SseHandlers {
  onStdout?: (text: string) => void;
  onStderr?: (text: string) => void;
}

/**
 * Consume the bridge's `text/event-stream` exec response.
 *
 * Events are `stdout` / `stderr` (base64-encoded chunks), terminated by either
 * `exit` (`{"exit_code":N}`) or `error` (`{"error":...,"code":...}`).
 */
const consumeExecStream = async (
  body: ReadableStream<Uint8Array>,
  handlers: SseHandlers,
): Promise<number> => {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let exitCode: number | undefined;
  let streamError: string | undefined;

  const handleEvent = (raw: string): void => {
    let eventName = "message";
    const dataLines: string[] = [];
    for (const line of raw.split("\n")) {
      if (line.startsWith("event:")) eventName = line.slice(6).trim();
      else if (line.startsWith("data:")) dataLines.push(line.slice(5).trim());
    }
    const data = dataLines.join("\n");
    if (!data) return;

    switch (eventName) {
      case "stdout":
        handlers.onStdout?.(Buffer.from(data, "base64").toString("utf8"));
        break;
      case "stderr":
        handlers.onStderr?.(Buffer.from(data, "base64").toString("utf8"));
        break;
      case "exit":
        try {
          exitCode =
            (JSON.parse(data) as { exit_code?: number }).exit_code ?? 0;
        } catch {
          exitCode = 0;
        }
        break;
      case "error":
        try {
          const parsed = JSON.parse(data) as { error?: string; code?: string };
          streamError = parsed.error ?? "unknown sandbox error";
        } catch {
          streamError = data;
        }
        break;
    }
  };

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let boundary = buffer.indexOf("\n\n");
      while (boundary !== -1) {
        handleEvent(buffer.slice(0, boundary));
        buffer = buffer.slice(boundary + 2);
        boundary = buffer.indexOf("\n\n");
      }
    }
  } finally {
    reader.releaseLock();
  }
  if (buffer.trim()) handleEvent(buffer);

  if (streamError !== undefined) {
    throw new Error(`Cloudflare sandbox exec failed: ${streamError}`);
  }
  // A stream that ends without `exit` means the connection dropped mid-command;
  // reporting 0 there would silently mark a killed agent run as successful.
  if (exitCode === undefined) {
    throw new Error(
      "Cloudflare sandbox exec stream ended without an exit event",
    );
  }
  return exitCode;
};

/**
 * Create a Cloudflare Sandbox isolated sandbox provider.
 *
 * Sandboxes are ephemeral — each `create()` allocates one and `close()`
 * destroys it.
 *
 * Requires a deployed [sandbox bridge](https://developers.cloudflare.com/sandbox/bridge/)
 * Worker; set `SANDBOX_API_URL` and `SANDBOX_API_KEY`, or pass them here.
 *
 * @example
 * ```ts
 * import { cloudflare } from "@ai-hero/sandcastle/sandboxes/cloudflare";
 *
 * const provider = cloudflare({
 *   apiUrl: "https://cloudflare-sandbox-bridge.my-subdomain.workers.dev",
 * });
 * ```
 */
export const cloudflare = (
  options?: CloudflareOptions,
): IsolatedSandboxProvider =>
  createIsolatedSandboxProvider({
    name: "cloudflare",
    env: options?.env,
    create: async (createOptions): Promise<IsolatedSandboxHandle> => {
      const maxOutputTailChars = options?.maxOutputTailChars ?? MAX_TAIL_CHARS;
      const doFetch = options?.fetch ?? globalThis.fetch;
      const worktreePath = options?.worktreePath ?? CLOUDFLARE_REPO_PATH;

      const apiUrl = (
        options?.apiUrl ??
        process.env.SANDBOX_API_URL ??
        ""
      ).replace(/\/+$/, "");
      if (!apiUrl) {
        throw new Error(
          "Cloudflare sandbox provider requires a bridge URL. Pass `apiUrl` or set SANDBOX_API_URL. " +
            "Deploy the bridge: https://developers.cloudflare.com/sandbox/bridge/",
        );
      }
      const apiKey = options?.apiKey ?? process.env.SANDBOX_API_KEY;

      const authHeaders: Record<string, string> = apiKey
        ? { Authorization: `Bearer ${apiKey}` }
        : {};

      const request = async (
        path: string,
        init: RequestInit & { headers?: Record<string, string> } = {},
      ): Promise<Response> => {
        const response = await doFetch(`${apiUrl}${path}`, {
          ...init,
          headers: { ...authHeaders, ...(init.headers ?? {}) },
        });
        if (!response.ok) {
          const detail = await response.text().catch(() => "");
          throw new Error(
            `Cloudflare sandbox bridge ${init.method ?? "GET"} ${path} failed: ${response.status} ${detail}`.trim(),
          );
        }
        return response;
      };

      const created = (await (
        await request("/v1/sandbox", { method: "POST" })
      ).json()) as { id?: string };
      if (!created.id) {
        throw new Error("Cloudflare sandbox bridge returned no sandbox id");
      }
      const sandboxId = created.id;

      // A session carries the environment for every later exec: the bridge's
      // /exec route accepts only argv, cwd and timeout_ms, so env has nowhere
      // else to go.
      const session = (await (
        await request(`/v1/sandbox/${sandboxId}/session`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ env: createOptions.env, cwd: worktreePath }),
        })
      ).json()) as { id?: string };
      const sessionHeaders: Record<string, string> = session.id
        ? { "Session-Id": session.id }
        : {};

      const execRaw = async (
        script: string,
        cwd: string,
        onLine?: (line: string) => void,
      ): Promise<ExecResult> => {
        const response = await request(`/v1/sandbox/${sandboxId}/exec`, {
          method: "POST",
          headers: { "Content-Type": "application/json", ...sessionHeaders },
          body: JSON.stringify({
            argv: ["sh", "-lc", script],
            cwd,
            ...(options?.timeoutMs !== undefined
              ? { timeout_ms: options.timeoutMs }
              : {}),
          }),
        });
        if (!response.body) {
          throw new Error("Cloudflare sandbox bridge returned no exec stream");
        }

        const stdoutTail = new BoundedTail(maxOutputTailChars, "\n");
        const stderrTail = new BoundedTail(maxOutputTailChars, "");
        let partial = "";

        const exitCode = await consumeExecStream(response.body, {
          onStdout: (text) => {
            if (!onLine) {
              stdoutTail.push(text);
              return;
            }
            const combined = partial + text;
            const lines = combined.split("\n");
            partial = lines.pop() ?? "";
            for (const line of lines) {
              stdoutTail.push(line);
              onLine(line);
            }
          },
          onStderr: (text) => stderrTail.push(text),
        });

        if (onLine && partial) {
          stdoutTail.push(partial);
          onLine(partial);
          partial = "";
        }

        return {
          stdout: stdoutTail.toString(),
          stderr: stderrTail.toString(),
          exitCode,
        };
      };

      const putFile = async (
        sandboxPath: string,
        contents: string | Uint8Array,
      ): Promise<void> => {
        await request(
          `/v1/sandbox/${sandboxId}/file/${filePathToUrlSuffix(sandboxPath)}`,
          {
            method: "PUT",
            headers: {
              "Content-Type": "application/octet-stream",
              ...sessionHeaders,
            },
            body: contents as RequestInit["body"],
          },
        );
      };

      const getFile = async (sandboxPath: string): Promise<ArrayBuffer> => {
        const response = await request(
          `/v1/sandbox/${sandboxId}/file/${filePathToUrlSuffix(sandboxPath)}`,
          { headers: sessionHeaders },
        );
        return response.arrayBuffer();
      };

      const stagePath = (): string => `${STAGE_DIR}/${crypto.randomUUID()}`;

      const execOrThrow = async (
        script: string,
        what: string,
      ): Promise<void> => {
        const result = await execRaw(script, WORKSPACE_ROOT);
        if (result.exitCode !== 0) {
          throw new Error(
            `Cloudflare sandbox bridge: ${what} failed (exit ${result.exitCode}): ${result.stderr}`.trim(),
          );
        }
      };

      const writeSandboxFile = async (
        sandboxPath: string,
        contents: string | Uint8Array,
      ): Promise<void> => {
        if (isUnderWorkspace(sandboxPath)) {
          await putFile(sandboxPath, contents);
          return;
        }
        const stage = stagePath();
        await execOrThrow(
          `mkdir -p ${shellEscape(STAGE_DIR)}`,
          "creating the staging directory",
        );
        await putFile(stage, contents);
        await execOrThrow(
          `mkdir -p ${shellEscape(posix.dirname(sandboxPath))} && mv ${shellEscape(stage)} ${shellEscape(sandboxPath)}`,
          `moving a staged file to ${sandboxPath}`,
        );
      };

      const readSandboxFile = async (
        sandboxPath: string,
      ): Promise<ArrayBuffer> => {
        if (isUnderWorkspace(sandboxPath)) {
          return getFile(sandboxPath);
        }
        const stage = stagePath();
        await execOrThrow(
          `mkdir -p ${shellEscape(STAGE_DIR)} && cp ${shellEscape(sandboxPath)} ${shellEscape(stage)}`,
          `staging ${sandboxPath} for reading`,
        );
        try {
          return await getFile(stage);
        } finally {
          await execRaw(`rm -f ${shellEscape(stage)}`, WORKSPACE_ROOT).catch(
            () => {},
          );
        }
      };

      await execRaw(`mkdir -p ${shellEscape(worktreePath)}`, "/workspace");

      return {
        worktreePath,

        exec: async (
          command: string,
          opts?: {
            onLine?: (line: string) => void;
            cwd?: string;
            sudo?: boolean;
            stdin?: string;
          },
        ): Promise<ExecResult> => {
          const cwd = opts?.cwd ?? worktreePath;
          const base = opts?.sudo ? `sudo ${command}` : command;

          // The bridge has no stdin channel, so a prompt is staged as a file
          // and redirected in. Sandcastle uses stdin precisely to dodge the
          // 128KB argv limit, so inlining it into the command is not an option.
          if (opts?.stdin === undefined) {
            return execRaw(base, cwd, opts?.onLine);
          }

          // The file lives in the staging directory, not the worktree: an
          // agent that runs `git add -A` before the command exits would
          // otherwise sweep it into its commit.
          const stdinPath = stagePath();
          await execOrThrow(
            `mkdir -p ${shellEscape(STAGE_DIR)}`,
            "creating the staging directory",
          );
          await putFile(stdinPath, opts.stdin);
          try {
            return await execRaw(
              `${base} < ${shellEscape(stdinPath)}`,
              cwd,
              opts.onLine,
            );
          } finally {
            await execRaw(
              `rm -f ${shellEscape(stdinPath)}`,
              WORKSPACE_ROOT,
            ).catch(() => {});
          }
        },

        copyIn: async (
          hostPath: string,
          sandboxPath: string,
        ): Promise<void> => {
          const info = await stat(hostPath);
          if (!info.isDirectory()) {
            await writeSandboxFile(sandboxPath, await readFile(hostPath));
            return;
          }
          const walk = async (dir: string): Promise<string[]> => {
            const entries = await readdir(dir, { withFileTypes: true });
            const files: string[] = [];
            for (const entry of entries) {
              const full = join(dir, entry.name);
              if (entry.isDirectory()) files.push(...(await walk(full)));
              else files.push(full);
            }
            return files;
          };
          for (const file of await walk(hostPath)) {
            const rel = relative(hostPath, file).split(/[\\/]/).join("/");
            await writeSandboxFile(
              posix.join(sandboxPath, rel),
              await readFile(file),
            );
          }
        },

        copyFileOut: async (
          sandboxPath: string,
          hostPath: string,
        ): Promise<void> => {
          const bytes = await readSandboxFile(sandboxPath);
          await mkdir(dirname(hostPath), { recursive: true });
          await writeFile(hostPath, Buffer.from(bytes));
        },

        close: async (): Promise<void> => {
          await request(`/v1/sandbox/${sandboxId}`, { method: "DELETE" });
        },
      };
    },
  });
