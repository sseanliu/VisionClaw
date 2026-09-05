import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import path from "node:path";
import express from "express";
import { WebSocketServer, type WebSocket } from "ws";
import { config } from "./config.js";
import { initStore, saveStore, userResources } from "./store.js";
import { ensureUser } from "./provision.js";
import { runTurn, runTurnStreaming, queueContext, drainContext, type TurnStats } from "./turn.js";
import { registerSocket, notifyUser, queuePending, drainPending } from "./notify.js";
import { appendTrace, readTrace } from "./trace.js";
import { startBrowse, awaitBrowse, browseEnabled, type BrowseDetail } from "./browse.js";

/** Flatten the computer-use agent's run detail into trace fields. */
function browseDetailFields(d?: BrowseDetail): Record<string, unknown> {
  if (!d) return {};
  return {
    status: d.status,
    model: d.model,
    result: d.result?.slice(0, 1000),
    error: d.error ?? undefined,
    steps: d.steps,
    step_count: d.stepCount,
    input_tokens: d.inputTokens,
    output_tokens: d.outputTokens,
    duration_s: d.durationS,
  };
}
import { registerConnectRoutes } from "./connect.js";
import { approvedAccountIds, initAuth, lookupToken, registerAuthRoutes, touchLastSeen } from "./auth.js";

initStore(config.storePath);
// Sign-in accounts resolve tokens synchronously from an in-memory index, so
// it must exist before the first request.
await initAuth();

const app = express();
// 5mb: task requests may carry a base64 camera frame (~200-400KB typical).
app.use(express.json({ limit: "5mb" }));

/**
 * What the voice model receives the instant a task is spawned.
 *
 * Deliberately an instruction, not a sentence to speak: a fixed string ("I'm
 * still working on that") gets read out verbatim and sounds like a status
 * message, whereas telling the model to acknowledge in its own words produces
 * cover that fits the conversation it is already having. The "do not answer
 * from memory" clause matters -- without it the model happily invents a
 * calendar rather than waiting for the real one.
 */
const SPAWN_ACK =
  "[task started] The request is running in the background. Tell the user briefly and naturally, " +
  "in your own words, that you're on it -- one short sentence, no promises about timing. " +
  "Do NOT answer the question yourself or guess at the content; the real result will arrive " +
  "shortly as a follow-up message for you to relay.";

// ---------- task ledger (continuity without replayed history) ----------

/** Mark the user's current session as having run a turn; only used sessions
 * rotate at the next call. Persisted immediately so failed turns count too. */
async function markSessionUsed(userId: string): Promise<void> {
  const u = await userResources(userId);
  if (!u.sessionUsed) {
    u.sessionUsed = true;
    await saveStore();
  }
}

/** Append a finished task to the user's rolling ledger. Sessions are rotated
 * per call, so this ledger -- not session history -- is what carries "did you
 * find it?" across calls, and it feeds the app's Recent Tasks view. */
async function recordTask(userId: string, prompt: string, result: string): Promise<void> {
  const u = await userResources(userId);
  u.recentTasks ??= [];
  u.recentTasks.push({
    ts: new Date().toISOString(),
    prompt: prompt.slice(0, 300),
    result: result.slice(0, 500),
  });
  if (u.recentTasks.length > 20) u.recentTasks = u.recentTasks.slice(-20);
  await saveStore();
}

/** A few hundred tokens of curated context for a fresh session. */
function buildBriefing(tasks: { ts: string; prompt: string; result: string }[]): string {
  const lines = tasks
    .slice(-5)
    .map((t) => `- [${t.ts}] asked: ${t.prompt}\n  outcome: ${t.result}`)
    .join("\n");
  return (
    "Background from the user's recent sessions (use only if they refer back to it):\n" + lines
  );
}

// ---------- auth ----------

function userFromRequest(req: express.Request, explicitToken?: string): string | null {
  let token = explicitToken?.trim();
  if (!token) {
    const header = req.header("authorization");
    if (!header?.startsWith("Bearer ")) return null;
    token = header.slice("Bearer ".length).trim();
  }
  // The agent worker authenticates users upstream (LiveKit room identity), so
  // it holds one service credential and names the user per request.
  if (config.serviceToken && token === config.serviceToken) {
    const impersonated = req.header("x-user-id")?.trim();
    return impersonated || null;
  }
  const staticUser = config.tokens.get(token);
  if (staticUser) return staticUser;
  // Self-registered accounts: only approved ones resolve here; pending and
  // revoked look exactly like a bad token everywhere except /me.
  const dyn = lookupToken(token);
  if (dyn && dyn.status === "approved") {
    touchLastSeen(dyn.userId);
    return dyn.userId;
  }
  return null;
}

// ---------- app connections (OAuth -> vault) ----------

registerConnectRoutes(app, userFromRequest);
registerAuthRoutes(app);

// ---------- HTTP: the app's existing protocol ----------

// Reachability probe: the app GETs this path and accepts any 2xx-4xx.
app.get("/v1/chat/completions", (_req, res) => {
  res.status(200).json({ ok: true, service: "visionclaw-gateway" });
});

app.get("/health", (_req, res) => {
  res.status(200).json({ ok: true });
});

// The app's delegateTask() posts OpenAI-style chat completions here.
app.post("/v1/chat/completions", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }

  const messages = (req.body?.messages ?? []) as Array<{ role: string; content: string }>;
  const lastUser = [...messages].reverse().find((m) => m.role === "user")?.content?.trim();
  if (!lastUser) {
    res.status(400).json({ error: { message: "no user message found" } });
    return;
  }

  // ---- streaming path (OpenAI-style SSE chunks) ----
  if (req.body?.stream === true) {
    let sessionId: string;
    try {
      ({ sessionId } = await ensureUser(userId));
      await markSessionUsed(userId);
    } catch (err) {
      console.error("[chat] provisioning failed:", err);
      res.status(502).json({ error: { message: "agent backend error" } });
      return;
    }

    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();

    const id = `chatcmpl-${randomUUID()}`;
    const created = Math.floor(Date.now() / 1000);
    const chunk = (delta: object, finish: string | null = null) =>
      `data: ${JSON.stringify({
        id,
        object: "chat.completion.chunk",
        created,
        model: "visionclaw-cloud",
        choices: [{ index: 0, delta, finish_reason: finish }],
      })}\n\n`;
    res.write(chunk({ role: "assistant" }));

    let clientDone = false;
    const finishClient = (ackText?: string) => {
      if (clientDone) return;
      clientDone = true;
      if (ackText) res.write(chunk({ content: ackText }));
      res.write(chunk({}, "stop"));
      res.write("data: [DONE]\n\n");
      res.end();
    };
    res.on("close", () => {
      clientDone = true;
    });

    // In spawn mode the request never carries the result: acknowledge now, keep
    // draining, and deliver the answer over the WebSocket when it lands. The ack
    // is an instruction rather than a line to read out, so the cover comes back
    // in the assistant's own voice instead of a canned status message.
    if (config.spawnMode) finishClient(SPAWN_ACK);

    // Otherwise race the agent against the budget; past it, same deferral.
    const capTimer = config.spawnMode
      ? null
      : setTimeout(
          () => finishClient("\n\nI'm still working on that. I'll let you know the moment it's done."),
          config.quickAnswerTimeoutMs,
        );

    try {
      const wasCappedBeforeFinal = () => clientDone;
      const finalText = await runTurnStreaming(sessionId, lastUser, drainContext(userId), (text) => {
        if (!clientDone) res.write(chunk({ content: text }));
      });
      if (capTimer) clearTimeout(capTimer);
      if (finalText) void recordTask(userId, lastUser, finalText);
      if (wasCappedBeforeFinal()) {
        if (finalText && !notifyUser(userId, finalText)) {
          console.warn(`[turn] late streamed result for ${userId} parked for next call`);
          void queuePending(userId, finalText);
        }
      } else {
        finishClient();
      }
    } catch (err) {
      if (capTimer) clearTimeout(capTimer);
      console.error("[chat] streaming turn failed:", err);
      finishClient("Something went wrong while working on that. Try again in a moment.");
    }
    return;
  }

  try {
    const { sessionId } = await ensureUser(userId);
    await markSessionUsed(userId);
    // The agent worker (service token) blocks on its own tool timeout and
    // relays the finished answer into the voice session -- handing IT the
    // spawn-mode acknowledgement would make the assistant read a status
    // message aloud. Spawn semantics are for the app's own HTTP calls.
    const bearer = req.header("authorization")?.slice("Bearer ".length).trim();
    const isServiceCall = !!config.serviceToken && bearer === config.serviceToken;
    // What the user is looking at, when the voice layer judged it relevant.
    const image = typeof req.body?.image === "string" && req.body.image ? req.body.image : undefined;
    // The worker's job process dies with the call. If that happens while this
    // request is still running, the finished answer would be written to a dead
    // socket -- observed live: a 25s research task lost outright. Track it so
    // the result can be parked for the next call instead.
    let clientGone = false;
    res.on("close", () => {
      if (!res.writableFinished) clientGone = true;
    });
    // The session owns durable history; only the newest user turn is sent.
    // What the subagent did, for the study trace: tool calls (name, server,
    // ok, ms), chain depth, stop reason. Text only, args/results clipped.
    const traceSubagent = (stats: TurnStats | undefined, outcome: string, resultText: string) => {
      if (!stats) return;
      appendTrace(userId, [
        {
          type: "subagent_turn",
          task: lastUser.slice(0, 200),
          outcome,
          tool_count: stats.tools.length,
          tools: stats.tools,
          thinking: stats.thinking,
          messages: stats.messages,
          stop_reason: stats.stop_reason,
          duration_ms: stats.duration_ms,
          result_chars: resultText.length,
          errors: stats.tools.filter((t) => t.ok === false).length,
        },
      ]);
    };
    const result = await runTurn(
      sessionId,
      lastUser,
      isServiceCall ? 110_000 : config.spawnMode ? 0 : config.quickAnswerTimeoutMs,
      (lateText, stats) => {
        traceSubagent(stats, "late", lateText);
        void recordTask(userId, lastUser, lateText);
        const delivered = notifyUser(userId, lateText);
        if (!delivered) {
          console.warn(`[turn] late result for ${userId} parked for next call`);
          void queuePending(userId, lateText);
        }
      },
      drainContext(userId),
      image,
    );

    if (!result.deferred && result.text) void recordTask(userId, lastUser, result.text);
    if (!result.deferred) traceSubagent(result.stats, clientGone ? "caller_gone" : "quick", result.text ?? "");
    if (!result.deferred && result.text && clientGone) {
      console.warn(`[turn] caller gone before completion; parking result for ${userId}`);
      // Same wrapper the worker uses when it parks, so the next call's relay
      // instruction reads identically.
      void queuePending(
        userId,
        `A task from an earlier call finished. Task: ${lastUser.slice(0, 200)}\nResult: ${result.text}`,
      );
      return;
    }
    const content = result.deferred
      ? config.spawnMode
        ? SPAWN_ACK
        : "I'm still working on that. I'll let you know the moment it's done."
      : (result.text ?? "Done.");

    res.json({
      id: `chatcmpl-${randomUUID()}`,
      object: "chat.completion",
      created: Math.floor(Date.now() / 1000),
      model: "visionclaw-cloud",
      choices: [
        {
          index: 0,
          message: { role: "assistant", content },
          finish_reason: "stop",
        },
      ],
    });
  } catch (err) {
    console.error("[chat] turn failed:", err);
    res.status(502).json({ error: { message: "agent backend error" } });
  }
});

// Live web browsing via Browser Use Cloud. Split into start + await so the
// voice worker can show a live-view card the moment the browser is up (only the
// worker holds the LiveKit room), then keep waiting for the result.
app.post("/browse/start", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  if (!browseEnabled()) {
    res.status(503).json({ error: { message: "browser tasks are not enabled on this gateway" } });
    return;
  }
  const task = String(req.body?.task ?? "").trim();
  if (!task) {
    res.status(400).json({ error: { message: "task is required" } });
    return;
  }
  try {
    const { runId, liveUrl } = await startBrowse(task);
    res.json({ runId, liveUrl });
  } catch (err) {
    console.error("[browse] start failed:", err);
    res.status(502).json({ error: { message: "browser backend error" } });
  }
});

app.post("/browse/await", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const runId = String(req.body?.runId ?? "").trim();
  const task = String(req.body?.task ?? "").trim();
  if (!runId) {
    res.status(400).json({ error: { message: "runId is required" } });
    return;
  }
  const wrap = (t: string) => `A task from an earlier call finished. Task: ${task.slice(0, 200)}\nResult: ${t}`;
  let clientGone = false;
  res.on("close", () => {
    if (!res.writableFinished) clientGone = true;
  });
  try {
    const outcome = await awaitBrowse(runId, 170_000, (lateText, meta) => {
      appendTrace(userId, [
        {
          type: "browser_task",
          task: task.slice(0, 200),
          outcome: "late",
          run_id: meta.runId,
          cost_usd: meta.cost,
          ...browseDetailFields(meta.detail),
        },
      ]);
      void recordTask(userId, task, lateText);
      if (!notifyUser(userId, wrap(lateText))) void queuePending(userId, wrap(lateText));
    });
    if (!outcome.deferred) {
      appendTrace(userId, [
        {
          type: "browser_task",
          task: task.slice(0, 200),
          outcome: clientGone ? "caller_gone" : "quick",
          run_id: outcome.runId,
          cost_usd: outcome.cost,
          ...browseDetailFields(outcome.detail),
        },
      ]);
      if (outcome.text) void recordTask(userId, task, outcome.text);
      if (outcome.text && clientGone) {
        void queuePending(userId, wrap(outcome.text));
        res.json({ result: SPAWN_ACK });
        return;
      }
    }
    res.json({ result: outcome.deferred ? SPAWN_ACK : (outcome.text ?? "Done.") });
  } catch (err) {
    console.error("[browse] await failed:", err);
    res.status(502).json({ error: { message: "browser backend error" } });
  }
});

// LiveKit room ticket: the phone trades its gateway token for a short-lived
// room JWT. The LiveKit API secret never leaves the server, and each user gets
// their own room -- the same isolation boundary as their CMA session.
app.post("/livekit-token", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const { LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET } = process.env;
  if (!LIVEKIT_URL || !LIVEKIT_API_KEY || !LIVEKIT_API_SECRET) {
    res.status(503).json({ error: { message: "LiveKit is not configured on this gateway" } });
    return;
  }
  // Ephemeral sessions: every call starts a fresh CMA session, briefed from
  // the task ledger instead of dragging the old session's transcript --
  // prefill cost stays flat as usage accumulates. Sessions that never ran a
  // turn are reused as-is: their briefing is still queued, and rotating them
  // would stack a second one (the API allows one system.message per turn).
  const u = await userResources(userId);
  if (u.sessionId && u.sessionUsed) {
    u.sessionId = undefined;
    u.sessionUsed = false;
    if (u.recentTasks?.length) queueContext(userId, buildBriefing(u.recentTasks));
    await saveStore();
  }
  // Pre-warm: session creation costs ~3-4s, which on a lazy path lands on the
  // call's first task. Kicking it off now hides the cold start behind call
  // setup and greeting time.
  void ensureUser(userId).catch((err) => console.warn(`[provision] pre-warm failed for ${userId}:`, err));
  const { AccessToken } = await import("livekit-server-sdk");
  // The engine choice (gemini | openai | openclaw) rides as participant metadata; the
  // worker reads it when the user joins and picks the realtime model.
  // For openclaw, agentSessionKey routes to a specific agent (e.g. "agent:coo:glass").
  const engine = ["openai", "openclaw"].includes(req.body?.engine) ? req.body.engine : "gemini";
  const agentSessionKey = typeof req.body?.agentSessionKey === "string" ? req.body.agentSessionKey : undefined;
  const metadata: Record<string, unknown> = { engine };
  if (agentSessionKey) metadata.agentSessionKey = agentSessionKey;
  const at = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, {
    identity: userId,
    ttl: "15m",
    metadata: JSON.stringify(metadata),
  });
  // One room per call, not per user: agent dispatch fires on room creation,
  // so a redial into a still-draining room from the previous call would get
  // no agent at all (observed live -- ~90s dead window after every hangup).
  const room = `vc-${userId}-${Date.now().toString(36)}`;
  at.addGrant({ roomJoin: true, room, canPublish: true, canSubscribe: true });
  res.json({ url: LIVEKIT_URL, room, token: await at.toJwt() });
});

// OTA install page: static files on the volume, uploaded out of band. Public
// by design -- the ipa is development-signed and installs only on provisioned
// devices, and install links need to work without typing a token on a phone.
app.use("/install", express.static("/data/ota", { index: "install.html" }));

// Parked task results: answers that finished after their call ended. The voice
// worker drains this at call start and speaks them; it can also park a result
// it was holding when the user hung up. Draining is destructive by design --
// each result is spoken exactly once.
app.get("/pending-notifications", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  res.json({ notifications: await drainPending(userId) });
});

app.post("/pending-notifications", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const text = String(req.body?.text ?? "").trim();
  if (!text) {
    res.status(400).json({ error: { message: "text is required" } });
    return;
  }
  await queuePending(userId, text);
  res.sendStatus(204);
});

// Task history for the app's Recent Tasks view -- served from the gateway's
// own ledger. (The old implementation replayed the CMA session event log,
// which silently froze once a session exceeded 500 events; sessions are also
// ephemeral now, so no single session has the full history anyway.)
app.get("/tasks", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const limit = Math.min(Number(req.query.limit ?? 20) || 20, 100);
  const u = await userResources(userId);
  const tasks = (u.recentTasks ?? [])
    .slice(-limit)
    .reverse()
    .map((t) => ({ id: `task_${t.ts}`, ts: t.ts, prompt: t.prompt, result: t.result }));
  res.json({ tasks });
});

// ---------- notes (voice-created memos and lists) ----------

const MAX_NOTES = 200;

app.post("/notes", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const text = String(req.body?.text ?? "").trim();
  if (!text) {
    res.status(400).json({ error: { message: "text is required" } });
    return;
  }
  const tag = String(req.body?.tag ?? "").trim().toLowerCase() || undefined;
  const u = await userResources(userId);
  u.notes ??= [];
  const note = { id: randomUUID().slice(0, 8), ts: new Date().toISOString(), text, tag };
  u.notes.push(note);
  if (u.notes.length > MAX_NOTES) u.notes = u.notes.slice(-MAX_NOTES);
  await saveStore();
  res.status(201).json({ note });
});

app.get("/notes", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const tag = typeof req.query.tag === "string" ? req.query.tag.trim().toLowerCase() : undefined;
  const limit = Math.min(Number(req.query.limit ?? 50) || 50, MAX_NOTES);
  const u = await userResources(userId);
  const notes = (u.notes ?? []).filter((n) => !tag || n.tag === tag).slice(-limit).reverse();
  res.json({ notes });
});

// Deletes the newest note whose text contains `match` (case-insensitive) --
// voice can say "remove milk from the shopping list" but never quote an id.
app.delete("/notes", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const match = String(req.body?.match ?? "").trim().toLowerCase();
  if (!match) {
    res.status(400).json({ error: { message: "match is required" } });
    return;
  }
  const tag = String(req.body?.tag ?? "").trim().toLowerCase() || undefined;
  const u = await userResources(userId);
  const notes = u.notes ?? [];
  for (let i = notes.length - 1; i >= 0; i--) {
    if (tag && notes[i].tag !== tag) continue;
    if (notes[i].text.toLowerCase().includes(match)) {
      const [deleted] = notes.splice(i, 1);
      await saveStore();
      res.json({ deleted });
      return;
    }
  }
  res.status(404).json({ error: { message: "no note matched" } });
});

// Trace dashboard: sessions list + per-call timeline over /trace. Static, no
// auth of its own -- the token typed into the page is what talks to the API.
app.get("/dashboard", (_req, res) => {
  res.sendFile(path.resolve("public/dashboard.html"));
});

// Participant guide: install, access code, Google connection, glasses setup,
// and what the study logs. Public by design, like /install.
app.get("/guide", (_req, res) => {
  res.sendFile(path.resolve("public/guide.html"));
});

// Participant roster for the dashboard's picker. Service token only: a user
// token names one user and has no business enumerating the others.
app.get("/users", (req, res) => {
  const bearer = req.header("authorization")?.slice("Bearer ".length).trim();
  if (!config.serviceToken || bearer !== config.serviceToken) {
    res.status(401).json({ error: { message: "service token required" } });
    return;
  }
  res.json({ users: [...new Set([...config.tokens.values(), ...approvedAccountIds()])] });
});

// Interaction trace: what the user said, what the voice model said, what
// actions ran. The voice worker batches events here; text only -- image-like
// fields are stripped in appendTrace, never stored.
app.post("/trace", (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const events = req.body?.events;
  if (!Array.isArray(events) || events.length === 0) {
    res.status(400).json({ error: { message: "events array is required" } });
    return;
  }
  res.status(200).json({ accepted: appendTrace(userId, events) });
});

app.get("/trace", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const limit = Math.min(Number(req.query.limit ?? 200) || 200, 1000);
  const since = typeof req.query.since === "string" ? req.query.since : undefined;
  res.json({ events: await readTrace(userId, limit, since) });
});

// Voice-session context handoff (summaries, what the user is looking at).
app.post("/context", async (req, res) => {
  const userId = userFromRequest(req);
  if (!userId) {
    res.status(401).json({ error: { message: "invalid or missing gateway token" } });
    return;
  }
  const context = String(req.body?.context ?? "").trim();
  if (!context) {
    res.status(400).json({ error: { message: "context is required" } });
    return;
  }
  // Queued (not sent immediately): the API only accepts system.message events
  // trailing a user.message, so this rides along with the user's next turn.
  queueContext(userId, context);
  res.sendStatus(204);
});

// ---------- WS: the app's event channel (protocol v3 handshake) ----------

const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer });

wss.on("connection", (ws: WebSocket) => {
  // Mirror the local gateway's opening move so OpenClawEventClient handshakes unchanged.
  ws.send(JSON.stringify({ type: "event", event: "connect.challenge", payload: {} }));

  ws.on("message", (raw) => {
    let msg: { type?: string; id?: string; method?: string; params?: { auth?: { token?: string } } };
    try {
      msg = JSON.parse(String(raw));
    } catch {
      return;
    }
    if (msg.type !== "req" || msg.method !== "connect") return;

    const token = msg.params?.auth?.token;
    const userId = token ? (config.tokens.get(token) ?? null) : null;
    if (!userId) {
      ws.send(
        JSON.stringify({ type: "res", id: msg.id, ok: false, error: { message: "invalid token" } }),
      );
      ws.close();
      return;
    }

    registerSocket(userId, ws);
    ws.send(JSON.stringify({ type: "res", id: msg.id, ok: true }));
    console.log(`[ws] client connected for ${userId}`);
  });
});

httpServer.listen(config.port, () => {
  console.log(`[gateway] listening on :${config.port}`);
  console.log(`[gateway] app settings -> host: http://<this-host>  port: ${config.port}`);
});
