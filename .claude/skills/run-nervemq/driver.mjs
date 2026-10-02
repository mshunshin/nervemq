#!/usr/bin/env node
// Drives a real NerveMQ server for agents: launches it on a throwaway data
// directory, then runs a line-based script that mixes admin-UI (browser),
// admin-API and SQS steps in one session.
//
//   node .claude/skills/run-nervemq/driver.mjs up     # start + seed
//   node .claude/skills/run-nervemq/driver.mjs run < script.txt
//   node .claude/skills/run-nervemq/driver.mjs down
//
// See SKILL.md next to this file for the script commands.

import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(HERE, "../../..");
const RUN_DIR = process.env.NERVEMQ_RUN_DIR ?? path.join(os.tmpdir(), "nervemq-run");
const STATE = path.join(RUN_DIR, "state.json");
const SHOTS = path.join(RUN_DIR, "shots");
const PORT = Number(process.env.PORT ?? 8080);
const BIN = process.env.NERVEMQ_BIN ?? path.join(ROOT, "target/debug/nervemq");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function responds(base) {
  try {
    await fetch(`${base}/`, { signal: AbortSignal.timeout(1000) });
    return true;
  } catch {
    return false;
  }
}

function readState() {
  if (!fs.existsSync(STATE)) {
    throw new Error(`no server state at ${STATE}: run \`driver.mjs up\` first`);
  }
  return JSON.parse(fs.readFileSync(STATE, "utf8"));
}

async function up() {
  const base = `http://localhost:${PORT}`;
  if (await responds(base)) {
    throw new Error(`something already answers on ${base}; run \`driver.mjs down\` or set PORT`);
  }
  if (!fs.existsSync(BIN)) {
    throw new Error(`no server binary at ${BIN}: run \`just build\` first`);
  }

  const data = path.join(RUN_DIR, "data");
  fs.rmSync(RUN_DIR, { recursive: true, force: true });
  fs.mkdirSync(data, { recursive: true });

  const st = {
    base,
    dir: RUN_DIR,
    email: "admin@example.com",
    password: "driver-password",
    ns: "demo",
    queue: "jobs",
    accessKey: "DRIVERKEY",
    secretKey: "driver-secret",
  };
  // The root password is overwritten from this on every start; the CLI and
  // the server must share it and the data directory.
  const env = {
    ...process.env,
    NERVEMQ_ROOT_EMAIL: st.email,
    NERVEMQ_ROOT_PASSWORD: st.password,
    NERVEMQ_BIND_ADDRESS: `127.0.0.1:${PORT}`,
    NERVEMQ_HOST: base,
  };
  const cli = (...args) =>
    execFileSync(BIN, ["--data-dir", data, ...args], { env, stdio: "pipe" });
  cli("namespace", "add", st.ns);
  cli(
    "apikey", "add", "--name", "driver", "--namespace", st.ns,
    "--access-key", st.accessKey, "--secret-key", st.secretKey,
  );

  const log = fs.openSync(path.join(RUN_DIR, "server.log"), "a");
  const child = spawn(BIN, ["--data-dir", data], {
    env,
    detached: true,
    stdio: ["ignore", log, log],
  });
  child.unref();
  st.pid = child.pid;

  let exited = false;
  child.on("exit", () => (exited = true));
  for (let i = 0; !(await responds(base)); i++) {
    if (exited || i > 120) {
      throw new Error(`server did not start; see ${path.join(RUN_DIR, "server.log")}`);
    }
    await sleep(500);
  }

  const login = await fetch(`${base}/api/admin/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ email: st.email, password: st.password }),
  });
  if (!login.ok) throw new Error(`login failed: ${login.status}`);
  const cookie = login.headers.getSetCookie().map((c) => c.split(";")[0]).join("; ");
  const created = await fetch(`${base}/api/admin/queue/${st.ns}/${st.queue}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Cookie: cookie },
    body: JSON.stringify({ attributes: {}, tags: {} }),
  });
  if (!created.ok) throw new Error(`queue create failed: ${created.status}`);

  fs.writeFileSync(STATE, JSON.stringify(st, null, 2));
  console.log(`NerveMQ up at ${base} (pid ${st.pid})
  admin UI   ${base}/login   ${st.email} / ${st.password}
  SQS        ${base}/api/sqs/${st.ns}/${st.queue}   key ${st.accessKey} / ${st.secretKey}
  run dir    ${RUN_DIR}   (server.log, data/, shots/)`);
}

async function down() {
  const st = readState();
  try {
    process.kill(st.pid, "SIGTERM");
  } catch {
    // Already gone.
  }
  for (let i = 0; i < 40 && (await responds(st.base)); i++) await sleep(250);
  fs.rmSync(STATE);
  console.log(`stopped pid ${st.pid}; logs and data kept in ${st.dir}`);
}

// Playwright wants the browser build matching its own version. When only
// another build is cached, fall back to the newest cached headless shell.
async function launchBrowser(chromium) {
  try {
    return await chromium.launch();
  } catch (e) {
    if (!String(e).includes("Executable doesn't exist")) throw e;
    const cache =
      process.env.PLAYWRIGHT_BROWSERS_PATH ??
      (process.platform === "darwin"
        ? path.join(os.homedir(), "Library/Caches/ms-playwright")
        : path.join(os.homedir(), ".cache/ms-playwright"));
    const builds = fs.existsSync(cache)
      ? fs.readdirSync(cache)
          .filter((d) => d.startsWith("chromium_headless_shell-"))
          .sort((a, b) => Number(b.split("-")[1]) - Number(a.split("-")[1]))
      : [];
    for (const build of builds) {
      const dir = path.join(cache, build);
      for (const sub of fs.readdirSync(dir)) {
        const exe = path.join(dir, sub, "chrome-headless-shell");
        if (fs.existsSync(exe)) {
          console.log(`  (using cached ${build}; \`npx playwright install chromium-headless-shell\` gets the matching one)`);
          return chromium.launch({ executablePath: exe });
        }
      }
    }
    throw e;
  }
}

async function run() {
  const st = readState();
  const { chromium } = await import("playwright");
  const sqsLib = await import("@aws-sdk/client-sqs");
  fs.mkdirSync(SHOTS, { recursive: true });

  const sqs = new sqsLib.SQSClient({
    endpoint: `${st.base}/api/sqs`,
    region: "us-east-1",
    credentials: { accessKeyId: st.accessKey, secretAccessKey: st.secretKey },
  });
  let queueUrl = `${st.base}/api/sqs/${st.ns}/${st.queue}`;
  let held = [];
  let lastReceived = 0;

  const [w, h] = (process.env.VIEWPORT ?? "1280x900").split("x").map(Number);
  const browser = await launchBrowser(chromium);
  const page = await browser.newPage({ viewport: { width: w, height: h } });
  // React reports hydration failures as uncaught page errors, not console
  // messages: listen to both.
  let errors = [];
  page.on("console", (m) => m.type() === "error" && errors.push(`console: ${m.text()}`));
  page.on("pageerror", (e) => errors.push(`pageerror: ${String(e).split("\n")[0]}`));

  const button = async (name) => {
    const byRole = page.getByRole("button", { name, exact: true });
    return (await byRole.count()) > 0 ? byRole.first() : page.getByText(name, { exact: true }).first();
  };

  const commands = {
    login: async () => {
      await page.goto(`${st.base}/login`);
      await page.fill('input[name="email"]', st.email);
      await page.fill('input[name="password"]', st.password);
      await page.click('button[type="submit"]');
      await page.waitForURL((u) => !u.pathname.startsWith("/login"));
      return `logged in, at ${new URL(page.url()).pathname}`;
    },
    nav: async (p) => {
      await page.goto(`${st.base}${p}`);
      await page.waitForLoadState("networkidle").catch(() => {});
      return new URL(page.url()).pathname;
    },
    wait: async (...text) => {
      await page.getByText(text.join(" ")).first().waitFor({ timeout: 15000 });
    },
    gone: async (...text) => {
      await page.getByText(text.join(" ")).first().waitFor({ state: "hidden", timeout: 15000 });
    },
    click: async (...name) => (await button(name.join(" "))).click(),
    fill: async (selector, ...value) => page.fill(selector, value.join(" ")),
    press: async (key) => page.keyboard.press(key),
    shot: async (name = "screenshot") => {
      // Let dialogs finish fading in, and undo the sideways scroll a click
      // on an off-screen button leaves behind.
      await sleep(400);
      await page.evaluate(() => window.scrollTo(0, window.scrollY));
      const file = path.join(SHOTS, `${name}.png`);
      await page.screenshot({ path: file });
      return file;
    },
    eval: async (...js) => JSON.stringify(await page.evaluate(js.join(" "))),
    api: async (method, p, ...json) => {
      const res = await page.request.fetch(`${st.base}/api/admin${p}`, {
        method,
        data: json.length ? JSON.parse(json.join(" ")) : undefined,
      });
      return `${res.status()} ${(await res.text()).slice(0, 300)}`;
    },
    queue: async (ns, name) => {
      queueUrl = `${st.base}/api/sqs/${ns}/${name}`;
      return queueUrl;
    },
    send: async (...body) => {
      const res = await sqs.send(
        new sqsLib.SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body.join(" ") }),
      );
      return `MessageId ${res.MessageId}`;
    },
    receive: async (max = "10", visibility = "600", wait = "0") => {
      const res = await sqs.send(
        new sqsLib.ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: Number(max),
          VisibilityTimeout: Number(visibility),
          WaitTimeSeconds: Number(wait),
        }),
      );
      const msgs = res.Messages ?? [];
      held.push(...msgs);
      lastReceived = msgs.length;
      return `${msgs.length} received ${JSON.stringify(msgs.map((m) => m.Body))}; holding ${held.length}`;
    },
    "assert-received": async (n) => {
      if (lastReceived !== Number(n)) {
        throw new Error(`last receive returned ${lastReceived}, expected ${n}`);
      }
    },
    "delete-held": async () => {
      for (const m of held) {
        await sqs.send(new sqsLib.DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle }));
      }
      const n = held.length;
      held = [];
      return `deleted ${n}`;
    },
    "release-held": async () => {
      for (const m of held) {
        await sqs.send(
          new sqsLib.ChangeMessageVisibilityCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: m.ReceiptHandle,
            VisibilityTimeout: 0,
          }),
        );
      }
      const n = held.length;
      held = [];
      return `released ${n}`;
    },
    sleep: async (ms) => sleep(Number(ms)),
    errors: async () => (errors.length ? `${errors.length}:\n    ${errors.join("\n    ")}` : "none"),
    "clear-errors": async () => {
      errors = [];
    },
  };

  const rl = readline.createInterface({ input: process.stdin });
  for await (const raw of rl) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const [cmd, ...args] = line.split(/\s+/);
    console.log(`> ${line}`);
    const fn = commands[cmd];
    try {
      if (!fn) throw new Error(`unknown command ${cmd}; known: ${Object.keys(commands).join(", ")}`);
      const out = await fn(...args);
      if (out !== undefined) console.log(`  ${out}`);
    } catch (e) {
      console.log(`  FAILED: ${String(e).split("\n")[0]}`);
      await page.screenshot({ path: path.join(SHOTS, "failure.png") }).catch(() => {});
      console.log(`  screenshot: ${path.join(SHOTS, "failure.png")}; page errors: ${errors.length}`);
      await browser.close();
      process.exit(1);
    }
  }
  await browser.close();
}

const action = { up, down, run }[process.argv[2]];
if (!action) {
  console.error("usage: driver.mjs up | run < script | down");
  process.exit(2);
}
action().catch((e) => {
  console.error(`error: ${e.message}`);
  process.exit(1);
});
