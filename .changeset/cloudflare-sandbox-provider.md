---
"@ai-hero/sandcastle": minor
---

Add a Cloudflare Sandbox provider (`@ai-hero/sandcastle/sandboxes/cloudflare`).

Runs agents in Cloudflare Sandboxes via a self-deployed [sandbox bridge](https://developers.cloudflare.com/sandbox/bridge/) Worker, which exposes the Sandbox SDK over HTTP. Unlike the Vercel and Daytona providers it needs no peer dependency — the bridge is a plain HTTP API.

```ts
import { run, pi } from "@ai-hero/sandcastle";
import { cloudflare } from "@ai-hero/sandcastle/sandboxes/cloudflare";

await run({
  agent: pi("claude-sonnet-4-6"),
  sandbox: cloudflare({ apiUrl: process.env.SANDBOX_API_URL }),
});
```
