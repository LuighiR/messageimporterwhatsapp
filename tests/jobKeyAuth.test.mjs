import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import test from "node:test";

const PORT = 4012;
const JOB_KEY = "test-job-key";

async function waitForServer(url, timeoutMs = 15_000) {
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        return;
      }
    } catch {
      // server still booting
    }

    await new Promise((resolve) => setTimeout(resolve, 200));
  }

  throw new Error(`Timed out waiting for server at ${url}`);
}

test("server protects operational routes with x-job-key while keeping health public", async () => {
  const child = spawn(process.execPath, ["src/server.mjs"], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      PORT: `${PORT}`,
      JOB_KEY,
      POSTGRES_URL: "",
      POSTGRES_SCHEMA: "core"
    },
    stdio: ["ignore", "pipe", "pipe"]
  });

  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });

  try {
    await waitForServer(`http://127.0.0.1:${PORT}/health`);

    const healthResponse = await fetch(`http://127.0.0.1:${PORT}/health`);
    assert.equal(healthResponse.status, 200);

    const noKeyResponse = await fetch(`http://127.0.0.1:${PORT}/db/tickets?limit=1`);
    assert.equal(noKeyResponse.status, 401);

    const wrongKeyResponse = await fetch(`http://127.0.0.1:${PORT}/db/tickets?limit=1`, {
      headers: {
        "x-job-key": "wrong-key"
      }
    });
    assert.equal(wrongKeyResponse.status, 401);

    const okResponse = await fetch(`http://127.0.0.1:${PORT}/db/tickets?limit=1`, {
      headers: {
        "x-job-key": JOB_KEY
      }
    });
    assert.equal(okResponse.status, 200);
  } finally {
    child.kill("SIGTERM");
    await new Promise((resolve) => child.once("exit", resolve));
    if (stderr.trim()) {
      process.stderr.write(stderr);
    }
  }
});
