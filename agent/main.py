"""VisionClaw voice agent: LiveKit room <-> realtime model, tools via the gateway.

The phone publishes mic + camera into a LiveKit room and this worker joins as
the assistant. Which brain answers is the user's choice, carried in their
participant metadata: Gemini Live (native audio+video) or OpenAI Realtime
(gpt-realtime-2; video frames arrive as image items). The framework owns what
the direct-connection client had to hand-roll -- echo cancellation lives in
WebRTC on the phone, interruption is playback-position-aware here.

Per-user identity: the room token's identity IS the gateway userId, so tool
calls hit the gateway with the service token plus X-User-Id, landing in that
user's own CMA session, vault and calendar.

Env (Fly secrets): LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET,
GOOGLE_API_KEY, OPENAI_API_KEY (optional; absent disables the openai engine),
GATEWAY_URL, GATEWAY_SERVICE_TOKEN. Models via GEMINI_MODEL /
OPENAI_REALTIME_MODEL.
"""

import asyncio
import base64
import io
import json
import logging
import os
import re
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp
from google import genai
from google.genai import types as genai_types
from livekit import rtc
from PIL import Image
from livekit.agents import (
    Agent,
    AgentSession,
    JobContext,
    RoomInputOptions,
    RunContext,
    WorkerOptions,
    cli,
    function_tool,
)
from livekit.plugins import google, openai
from openai.types.beta.realtime.session import TurnDetection

logger = logging.getLogger("visionclaw-agent")

INSTRUCTIONS = """You are VisionClaw, an AI assistant the user talks to while showing you the
world through their phone camera or smart glasses. Keep responses concise and natural.

You can see live video. Answer visual questions directly from what you see.

The browse tool is your computer-use agent: it drives a real web browser on a live site, both
to read and to ACT. Use it for anything that happens on a website -- shopping (add an item to a
cart, buy, place an order, check out), booking, signing up, filling and submitting a public web
form, as well as reading (find a specific product with its price and reviews, compare items in a
store, check live availability or hours). Anything on a shopping site, store, or general website
that is not one of the user's own connected accounts is browse -- "add this to my Amazon cart" is
browse, not execute. If the user says "computer use agent", "browser", or "shopping agent", they
mean browse. It is slower than quick_search, so speak a brief acknowledgment before calling it.

For quick factual lookups -- weather, sports scores, stock prices, news, opening hours,
current facts about the world -- use quick_search. It answers in a couple of seconds;
just relay the result. This includes looking up things you can see on camera. A basic
result card appears on screen automatically; when the answer has real structure
(forecast days, scores, prices, comparisons), upgrade it by calling show_card with
uuid "search" and structured facts or items rows -- same uuid, so it replaces the
basic card instead of stacking.

Whenever an answer you composed yourself has visual structure -- schedules, lists,
comparisons, step-by-step results -- call show_card with the essentials in the same
turn as your spoken answer. Card first or alongside, then speak a short summary; never
read the card aloud row by row. One card per answer; reuse its uuid to update it.

For notes and lists, use the note tools directly -- they are instant. "Remember this" or
"note that down" is save_note; "add milk to my shopping list" is save_note with
tag="shopping"; "what's on my list" is recall_notes; "remove the milk" is delete_note.
Every note tool puts the up-to-date list card on screen by itself -- never call show_card
for note content, just confirm briefly in speech. When the user asks to note something
they are showing on camera, save what you SEE as text -- one item per save_note call.

The execute tool is the user's personal account agent -- it acts inside their OWN connected
accounts and data: messages and email, reminders, calendars, Notion pages and databases, Slack
(send a message to a channel or person, search Slack, post a canvas), general web research, smart
home. It CANNOT open shopping sites or take actions on a website -- for anything that happens on a
website, including shopping or adding to a cart, use browse instead, even when the user calls it
"an action". Speak a brief natural acknowledgment BEFORE calling it, never call it silently. Results may arrive as a follow-up; relay them as the
answer to what was asked, not as a notification. If the task is about something the user
is showing on camera, set attach_view=true so the actual image travels with the task --
still describe what you see in the task text as well.

SOCIAL SCAN: When the user says "scan this person", "who is this", "look them up", or similar,
use the social_scan tool. It captures a single frame from the camera, reads the person's name
tag or badge, and searches the web for their public bio. A card with their info appears on screen.
This works without continuous video streaming — one snapshot frame is enough. If no name is
visible, ask the user to say the person's name and use quick_search instead.

ADD CONTACT: After a social scan (or when the user says "add to contacts", "save contact",
"add this person"), use the add_contact tool. If called right after a scan with no arguments,
it automatically uses the scanned person's data. The contact is saved to SuiteCRM (the CRM).
If SuiteCRM is not configured, it falls back to saving as a tagged note. A confirmation card
appears on screen when saved."""


class FrameHolder:
    """Most recent camera frame from the user's video track. When the user
    freezes/pins a frame, the phone mutes the track, so the pinned frame stays
    the latest one here -- pin semantics carry through to attached images."""

    def __init__(self) -> None:
        self.frame: rtc.VideoFrame | None = None
        self.rotation: int = 0


def encode_latest_frame(holder: FrameHolder) -> str | None:
    """JPEG-encode the latest frame (capped at 1280px, rotation applied) as base64.

    1280 is the DAT source ceiling (glasses stream at 720x1280), so a full-res
    frame passes through untouched -- DAT is the only limiter, not this cap."""
    frame = holder.frame
    if frame is None:
        return None
    rgba = frame.convert(rtc.VideoBufferType.RGBA)
    img = Image.frombuffer("RGBA", (rgba.width, rgba.height), bytes(rgba.data)).convert("RGB")
    rot = holder.rotation % 360
    if rot:
        img = img.rotate(-rot, expand=True)
    img.thumbnail((1280, 1280))
    buf = io.BytesIO()
    img.save(buf, "JPEG", quality=80)
    return base64.b64encode(buf.getvalue()).decode()


class Tracer:
    """Interaction trace for the deployment study: what the user said, what the
    voice model said, and what actions ran. Text only BY DESIGN -- events must
    never carry frames or base64 image payloads (the gateway strips image-like
    fields as a second line of defense). Events batch to the gateway; a failed
    delivery drops that batch rather than ever stalling the voice loop."""

    FLUSH_AFTER = 20
    FLUSH_SECONDS = 5.0
    MAX_FIELD_CHARS = 2000

    def __init__(self, user_id: str) -> None:
        self.user_id = user_id
        self._events: list[dict] = []
        self._lock = asyncio.Lock()

    def emit(self, event_type: str, **fields) -> None:
        event: dict = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "type": event_type,
        }
        for key, value in fields.items():
            if isinstance(value, str) and len(value) > self.MAX_FIELD_CHARS:
                value = value[: self.MAX_FIELD_CHARS] + "...[truncated]"
            event[key] = value
        self._events.append(event)
        if len(self._events) >= self.FLUSH_AFTER:
            t = asyncio.create_task(self.flush())
            _relay_tasks.add(t)
            t.add_done_callback(_relay_tasks.discard)

    async def flush(self) -> None:
        async with self._lock:
            if not self._events:
                return
            batch, self._events = self._events, []
            try:
                async with aiohttp.ClientSession() as http:
                    async with http.post(
                        f"{_gateway_url()}/trace",
                        headers=_gateway_headers(self.user_id),
                        json={"events": batch},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        resp.raise_for_status()
            except Exception:
                # Study data: better late than lost. Re-queue for the next pump
                # tick (a transient timeout was observed dropping a save_note);
                # the cap only matters if the gateway stays down a whole call.
                self._events = batch + self._events
                if len(self._events) > 500:
                    self._events = self._events[-500:]
                logger.exception(
                    "trace flush failed, requeued: user=%s batch=%d", self.user_id, len(batch)
                )

    async def pump(self) -> None:
        while True:
            await asyncio.sleep(self.FLUSH_SECONDS)
            await self.flush()


@dataclass
class Userdata:
    user_id: str
    frames: FrameHolder
    tracer: Tracer = field(default_factory=lambda: Tracer("unknown"))
    room: rtc.Room | None = None
    # Everything the assistant has said this session, in order; relay checks
    # read it to confirm a delivered result was actually spoken.
    spoken: list[str] = field(default_factory=list)
    # The Browser Use run whose live-view card is currently on screen. A browse
    # run dismisses the card only while it still owns this slot, so an earlier
    # run finishing cannot tear down a newer run's card (the double-fire race).
    live_browse_run: str | None = None
    # The in-flight browse job, if any. A second browse call while one is still
    # running is refused instead of spinning up a second Browser Use run -- the
    # model tends to re-fire the tool when the first is slow (double-fire), and
    # two live CUA runs mean double cost and duplicate relays.
    browse_job: "asyncio.Future[str] | None" = None
    # Last social_scan result, so add_contact can use it without re-scanning.
    last_scan: dict | None = None


_RELAY_STOPWORDS = {"finished", "earlier", "result", "research", "summary", "shopping", "however", "because"}


def _relay_keywords(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-z]{7,}", text.lower()) if w not in _RELAY_STOPWORDS}


def _relay_hits(spoken: list[str], keywords: set[str]) -> int:
    said = " ".join(spoken).lower()
    return sum(1 for w in keywords if w in said)


def _relay_needed(keywords: set[str]) -> int:
    # How many of the result's distinctive words must appear in what the model
    # actually said for us to count it as relayed. Scales with result length,
    # but a short result (few keywords) must not demand more hits than it has --
    # that made every one-line result look "not relayed" and re-park duplicates.
    n = len(keywords)
    return 0 if n == 0 else min(n, max(1, n // 10))


_search_client: genai.Client | None = None


def _get_search_client() -> genai.Client:
    global _search_client
    if _search_client is None:
        _search_client = genai.Client()
    return _search_client


@function_tool
async def quick_search(ctx: RunContext[Userdata], query: str) -> str:
    """Fast grounded web lookup for facts that change: weather, sports scores, stock
    prices, news, opening hours, and quick factual questions. Returns a spoken-ready
    answer in about two seconds. For actions or multi-step research, use execute."""
    t0 = time.monotonic()
    try:
        resp = await _get_search_client().aio.models.generate_content(
            model=os.environ.get("QUICK_SEARCH_MODEL", "gemini-3.5-flash-lite"),
            contents=query,
            config=genai_types.GenerateContentConfig(
                # Grounded generation: Google searches internally and returns a
                # synthesized answer in one round-trip -- no link-reading loop.
                # thinking_level has no "off"; "low" measured faster than default.
                tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                thinking_config=genai_types.ThinkingConfig(thinking_level="low"),
            ),
        )
    except Exception:
        logger.exception("quick_search failed: user=%s query=%r", ctx.userdata.user_id, query[:120])
        ctx.userdata.tracer.emit("agent_action", tool="quick_search", query=query, error=True)
        return "The quick search failed. Offer to try again, or use execute for a deeper attempt."
    text = (resp.text or "").strip()
    logger.info(
        "quick_search: user=%s query=%r latency_s=%.2f len=%d",
        ctx.userdata.user_id, query[:120], time.monotonic() - t0, len(text),
    )
    ctx.userdata.tracer.emit(
        "agent_action",
        tool="quick_search",
        query=query,
        result=text[:500],
        latency_s=round(time.monotonic() - t0, 2),
    )
    if text:
        await _push_search_card(ctx, query, text)
    return text or "The search returned nothing useful; try execute for a deeper attempt."


# How long a tool call may hold the model's turn open before the answer is
# demoted to a follow-up. Past this, users assume the call is dead and hang up
# -- which kills the session, the tool call, and the answer with it. Simple
# CMA tasks measure 8-9s on fresh sessions; 10 keeps them one clean answer
# instead of a progress beat plus a follow-up.
QUICK_ANSWER_S = 10

# While a slow task runs, keep the channel alive with brief spoken progress
# notes: a working agent should never be indistinguishable from a dead one.
HEARTBEAT_S = 30
MAX_HEARTBEATS = 4
# How long to wait for a free turn slot before injecting a result relay, so it
# does not collide with an active response or a barge-in.
DELIVER_WAIT_S = 8
# After a browse finishes, its cloud browser lingers on the final page for a
# couple of minutes, so the live card keeps showing the result. Hold it for this
# window, then clear it.
BROWSE_KEEPALIVE_S = 90

# The gateway echoes this ack when a task outlives its own 110s wait; it is an
# instruction blob for a voice model, not an answer, so never relay it as one.
GATEWAY_DEFERRAL_PREFIX = "[task started]"

_relay_tasks: set[asyncio.Task] = set()


def _gateway_headers(user_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {os.environ['GATEWAY_SERVICE_TOKEN']}",
        "X-User-Id": user_id,
    }


def _gateway_url() -> str:
    return os.environ["GATEWAY_URL"].rstrip("/")


async def _gateway_execute(user_id: str, task: str, image_b64: str | None = None) -> str:
    payload: dict = {"messages": [{"role": "user", "content": task}]}
    if image_b64:
        payload["image"] = image_b64
    async with aiohttp.ClientSession() as http:
        async with http.post(
            f"{_gateway_url()}/v1/chat/completions",
            headers=_gateway_headers(user_id),
            json=payload,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as resp:
            body = await resp.json()
    try:
        return body["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        logger.warning("gateway returned unexpected shape: %s", json.dumps(body)[:300])
        return "The action agent returned an unexpected response."


async def _park_result(user_id: str, task: str, result: str) -> None:
    """Hand a result we can no longer speak (call over) back to the gateway;
    the next call's worker drains and delivers it."""
    await _park_text(user_id, f"A task from an earlier call finished. Task: {task[:200]}\nResult: {result}")


async def _park_text(user_id: str, text: str) -> None:
    try:
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"{_gateway_url()}/pending-notifications",
                headers=_gateway_headers(user_id),
                json={"text": text},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                resp.raise_for_status()
        logger.info("parked result for next call: user=%s", user_id)
    except Exception:
        logger.exception("failed to park result: user=%s", user_id)


async def _apps_needing_reconnect(user_id: str) -> list[str]:
    """Connected apps whose stored credential no longer works (Gmail's
    testing-mode refresh tokens expire every 7 days by policy). Only the user
    can fix it, from Settings, so this exists to say so once per call."""
    try:
        async with aiohttp.ClientSession() as http:
            async with http.get(
                f"{_gateway_url()}/apps",
                headers=_gateway_headers(user_id),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                body = await resp.json()
        return [
            str(a.get("displayName") or a.get("id"))
            for a in body.get("apps") or []
            if a.get("connected") and a.get("needs_reconnect")
        ]
    except Exception:
        logger.exception("app health check failed: user=%s", user_id)
        return []


async def _drain_pending(user_id: str) -> list[str]:
    try:
        async with aiohttp.ClientSession() as http:
            async with http.get(
                f"{_gateway_url()}/pending-notifications",
                headers=_gateway_headers(user_id),
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                body = await resp.json()
        return [str(n) for n in body.get("notifications") or []]
    except Exception:
        logger.exception("pending drain failed: user=%s", user_id)
        return []


def _card_text(raw: str, limit: int) -> str:
    """Markdown-ish search prose -> plain card text."""
    text = re.sub(r"[*_`#]+", "", raw)
    text = re.sub(r"[ \t]+", " ", text).strip()
    return text if len(text) <= limit else text[: limit - 3].rsplit(" ", 1)[0] + "..."


async def _push_search_card(ctx: RunContext[Userdata], query: str, answer: str) -> None:
    """Deterministic baseline card for every search answer -- the model may
    replace it with a richer structured card under the same uuid, but a result
    is never invisible just because the model skipped show_card."""
    room = ctx.userdata.room
    if room is None:
        return
    try:
        card = {
            "uuid": "search",
            "version": 1,
            "type": "info",
            "title": _card_text(query, 60),
            "body": _card_text(answer, 350),
            "fallback_text": _card_text(answer, 100),
        }
        await _publish_card(room, card)
        ctx.userdata.tracer.emit(
            "agent_action",
            tool="show_card",
            card_type="info",
            title=card["title"],
            fallback_text=card["fallback_text"],
            auto=True,
        )
    except Exception:
        logger.exception("search card publish failed: user=%s", ctx.userdata.user_id)


async def _publish_card(room: rtc.Room, card: dict) -> None:
    payload = json.dumps({k: v for k, v in card.items() if v is not None})
    if len(payload) > 16_384:
        return
    await room.local_participant.send_text(payload, topic="vc.ui")


def _notes_card(tag: str | None, notes: list[dict]) -> dict:
    """Card built in code from the actual store contents -- the model narrates,
    it never writes this UI, so the card cannot drift from the data."""
    title = f"{tag.title()} List" if tag else "Notes"
    shown = notes[:8]  # gateway returns newest first
    return {
        "uuid": f"notes-{tag or 'all'}",
        "version": 1,
        "type": "list",
        "title": title,
        "body": (
            "List is empty."
            if not shown
            else f"{len(notes)} items; showing the latest {len(shown)}." if len(notes) > len(shown) else None
        ),
        "items": [{"title": n["text"]} for n in shown],
        "fallback_text": f"{title}: " + (", ".join(n["text"] for n in shown) if shown else "empty"),
    }


async def _push_notes_card(ctx: RunContext[Userdata], tag: str | None, notes: list[dict] | None = None) -> None:
    """Show the current state of a list after any note action. Deterministic:
    fires on every save/recall/delete, from a fresh read when the caller has no
    data in hand. A stable uuid per tag updates the card in place."""
    room = ctx.userdata.room
    if room is None:
        return
    try:
        if notes is None:
            body = await _notes_call("GET", ctx.userdata.user_id, query=f"?tag={tag}" if tag else "")
            notes = body.get("notes") or []
        card = _notes_card(tag, notes)
        await _publish_card(room, card)
        ctx.userdata.tracer.emit(
            "agent_action",
            tool="show_card",
            card_type="list",
            title=card["title"],
            fallback_text=card["fallback_text"],
            auto=True,
        )
    except Exception:
        logger.exception("notes card publish failed: user=%s", ctx.userdata.user_id)


async def _notes_call(method: str, user_id: str, payload: dict | None = None, query: str = "") -> dict:
    async with aiohttp.ClientSession() as http:
        async with http.request(
            method,
            f"{_gateway_url()}/notes{query}",
            headers=_gateway_headers(user_id),
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            body = await resp.json()
            return {"status": resp.status, **(body if isinstance(body, dict) else {})}


@function_tool
async def save_note(ctx: RunContext[Userdata], text: str, tag: str | None = None) -> str:
    """Save a note or add an item to a named list, instantly. Use it when the user
    says "remember this", "note that down", or "add X to my <name> list". One item
    per call. tag groups items into a list (e.g. tag="shopping" for a shopping
    list); omit it for a standalone note."""
    try:
        body = await _notes_call("POST", ctx.userdata.user_id, {"text": text, "tag": tag})
        if body.get("status") != 201:
            raise RuntimeError(f"gateway returned {body.get('status')}")
    except Exception:
        logger.exception("save_note failed: user=%s", ctx.userdata.user_id)
        ctx.userdata.tracer.emit("agent_action", tool="save_note", text=text, tag=tag, error=True)
        return "Saving the note failed. Offer to try again."
    ctx.userdata.tracer.emit("agent_action", tool="save_note", text=text, tag=tag)
    await _push_notes_card(ctx, tag)
    where = f"the {tag} list" if tag else "your notes"
    return f"Saved to {where}. The updated list card is already on screen; confirm briefly in your own words."


@function_tool
async def recall_notes(ctx: RunContext[Userdata], tag: str | None = None) -> str:
    """Read back the user's saved notes, newest first. Pass tag to read one list
    (e.g. tag="shopping"); omit it for everything. The list card appears on
    screen automatically -- do not build your own card for it."""
    query = f"?tag={tag}" if tag else ""
    try:
        body = await _notes_call("GET", ctx.userdata.user_id, query=query)
    except Exception:
        logger.exception("recall_notes failed: user=%s", ctx.userdata.user_id)
        ctx.userdata.tracer.emit("agent_action", tool="recall_notes", tag=tag, error=True)
        return "Reading the notes failed. Offer to try again."
    notes = body.get("notes") or []
    ctx.userdata.tracer.emit("agent_action", tool="recall_notes", tag=tag, count=len(notes))
    if not notes:
        where = f"the {tag} list" if tag else "your notes"
        return f"There is nothing on {where} yet."
    await _push_notes_card(ctx, tag, notes)
    lines = [f"- {n['text']}" + (f" [{n['tag']}]" if n.get("tag") and not tag else "") for n in notes]
    return "Saved notes, newest first (already shown on screen as a card):\n" + "\n".join(lines)


@function_tool
async def delete_note(ctx: RunContext[Userdata], match: str, tag: str | None = None) -> str:
    """Remove a saved note or list item: the newest one whose text contains
    `match`. Use when the user says "remove X from the list" or "delete that
    note". Pass tag when they name the list."""
    try:
        body = await _notes_call("DELETE", ctx.userdata.user_id, {"match": match, "tag": tag})
    except Exception:
        logger.exception("delete_note failed: user=%s", ctx.userdata.user_id)
        ctx.userdata.tracer.emit("agent_action", tool="delete_note", match=match, tag=tag, error=True)
        return "Deleting the note failed. Offer to try again."
    if body.get("status") == 404:
        ctx.userdata.tracer.emit("agent_action", tool="delete_note", match=match, tag=tag, found=False)
        return f"No saved note contains {match!r}. Tell the user, and offer to read the list."
    deleted = (body.get("deleted") or {}).get("text", match)
    ctx.userdata.tracer.emit("agent_action", tool="delete_note", match=match, tag=tag, deleted=deleted)
    await _push_notes_card(ctx, tag)
    return f"Removed: {deleted}. The updated list card is already on screen; confirm briefly."


# ---------------------------------------------------------------------------
# Social Scan — capture a single frame from the glasses, read a name tag,
# search the web for that person, and return a bio card. Battery-friendly:
# no continuous video streaming needed.
# ---------------------------------------------------------------------------

@function_tool
async def social_scan(ctx: RunContext[Userdata]) -> str:
    """Scan the person in front of the camera: capture a single frame, read
    their name tag (name, company, role), and search the web for their public
    profile. Returns a bio summary and shows a card on screen. Use when the
    user says "scan this person", "who is this", or "look them up". Works
    without continuous video — captures one snapshot frame."""
    t0 = time.monotonic()
    # Grab the current frame from the video track (or a frozen one)
    image_b64 = encode_latest_frame(ctx.userdata.frames)
    if not image_b64:
        return ("I don't have a camera frame to scan. Make sure the glasses or phone camera "
                "is active, then try again.")

    # Step 1: Use Gemini vision to extract name/company from the frame (name tag reading)
    try:
        search_client = _get_search_client()
        vision_resp = await search_client.aio.models.generate_content(
            model=os.environ.get("QUICK_SEARCH_MODEL", "gemini-3.5-flash-lite"),
            contents=[
                genai_types.Part.from_bytes(data=base64.b64decode(image_b64), mime_type="image/jpeg"),
                "Extract the person's name and affiliation from any visible name tag, badge, or "
                "text in this image. Also note any other visible details (company logo, job title, "
                "event name). Return as JSON: {\"name\": \"\", \"company\": \"\", \"title\": \"\", "
                "\"other\": \"\"}. If no name is visible, return {\"name\": null}.",
            ],
            config=genai_types.GenerateContentConfig(
                response_mime_type="application/json",
                thinking_config=genai_types.ThinkingConfig(thinking_level="low"),
            ),
        )
        import json as _json
        vision_data = _json.loads(vision_resp.text or "{}")
    except Exception as e:
        logger.exception("social_scan vision failed: user=%s", ctx.userdata.user_id)
        ctx.userdata.tracer.emit("agent_action", tool="social_scan", error=True, stage="vision")
        return f"I couldn't read the frame. Error: {e}. Ask the user to say the person's name instead."

    name = (vision_data.get("name") or "").strip()
    company = (vision_data.get("company") or "").strip()
    title = (vision_data.get("title") or "").strip()
    other = (vision_data.get("other") or "").strip()

    if not name:
        ctx.userdata.tracer.emit("agent_action", tool="social_scan", stage="vision", no_name=True)
        return ("I couldn't read a name from what I see. Ask the user to say the person's name "
                "and company, then use quick_search to look them up.")

    logger.info("social_scan: name=%s company=%s title=%s", name, company, title)

    # Step 2: Web search for the person
    query_parts = [name]
    if company:
        query_parts.append(company)
    if title:
        query_parts.append(title)
    query = " ".join(query_parts)
    # Add LinkedIn/social hints
    search_query = f"{name} {company} {title} LinkedIn bio background".strip()

    try:
        search_resp = await search_client.aio.models.generate_content(
            model=os.environ.get("QUICK_SEARCH_MODEL", "gemini-3.5-flash-lite"),
            contents=search_query,
            config=genai_types.GenerateContentConfig(
                tools=[genai_types.Tool(google_search=genai_types.GoogleSearch())],
                thinking_config=genai_types.ThinkingConfig(thinking_level="low"),
            ),
        )
        bio_text = (search_resp.text or "").strip()
    except Exception:
        logger.exception("social_scan search failed: user=%s query=%r", ctx.userdata.user_id, search_query[:120])
        bio_text = ""

    # Step 3: Build and show a bio card
    facts = []
    if name:
        facts.append({"label": "Name", "value": name})
    if title:
        facts.append({"label": "Title", "value": title})
    if company:
        facts.append({"label": "Company", "value": company})
    if other:
        facts.append({"label": "Notes", "value": other})

    # If search found bio info, add a summary
    if bio_text:
        # Truncate to keep card readable
        bio_summary = bio_text[:500]
    else:
        bio_summary = "No public bio found."

    latency = round(time.monotonic() - t0, 2)
    logger.info("social_scan complete: name=%s latency_s=%.2f bio_len=%d", name, latency, len(bio_text or ""))
    ctx.userdata.tracer.emit(
        "agent_action",
        tool="social_scan",
        name=name,
        company=company,
        title=title,
        latency_s=latency,
        bio_len=len(bio_text or ""),
    )

    # Store scan result in userdata so add_contact can use it
    ctx.userdata.last_scan = {
        "name": name,
        "title": title,
        "company": company,
        "other": other,
        "bio": bio_text or "",
    }

    # Show a card with the scan results
    import json as _json2
    await ctx.room.local_participant.send_text(
        _json2.dumps({
            "uuid": "social-scan",
            "version": 1,
            "type": "info",
            "title": name,
            "value": f"{title} at {company}" if title and company else (title or company or ""),
            "body": bio_summary,
            "facts": facts,
            "fallback_text": f"{name}, {title} at {company}" if title and company else name,
        }),
        topic="vc.ui",
    )

    # Spoken summary for the user
    spoken = f"Scanned: {name}"
    if title:
        spoken += f", {title}"
    if company:
        spoken += f" at {company}"
    if bio_text:
        # Add a key fact from the bio
        spoken += f". {bio_text[:200]}"
    return spoken + ". Card shown on screen. Say 'add to contacts' to save them."


# ---------------------------------------------------------------------------
# Add Contact — push contact info to SuiteCRM via REST API
# ---------------------------------------------------------------------------

@function_tool
async def add_contact(
    ctx: RunContext[Userdata],
    first_name: str | None = None,
    last_name: str | None = None,
    email: str | None = None,
    phone: str | None = None,
    company: str | None = None,
    title: str | None = None,
    notes: str | None = None,
) -> str:
    """Add a contact to the CRM (SuiteCRM). Use after a social scan or when
    the user says "add this person to contacts", "save contact", or "add to
    my contacts". If called right after social_scan with no arguments, it
    uses the scanned person's data automatically."""
    # If no name provided, try to use the last scan result
    if not first_name and not last_name and hasattr(ctx.userdata, 'last_scan') and ctx.userdata.last_scan:
        scan = ctx.userdata.last_scan
        full_name = scan.get("name", "")
        # Try to split into first/last
        parts = full_name.split(" ", 1)
        first_name = parts[0] if parts else ""
        last_name = parts[1] if len(parts) > 1 else ""
        if not company:
            company = scan.get("company", "")
        if not title:
            title = scan.get("title", "")
        if not notes:
            bio = scan.get("bio", "")
            other = scan.get("other", "")
            note_parts = []
            if bio:
                note_parts.append(f"Bio: {bio[:300]}")
            if other:
                note_parts.append(f"Notes: {other}")
            notes = " | ".join(note_parts) if note_parts else None
        logger.info("add_contact: using scan data for %s %s", first_name, last_name)

    if not first_name and not last_name:
        return ("I don't have a name to save. Either scan the person first (say 'scan this person') "
                "or tell me their name.")

    # SuiteCRM REST API
    suitecrm_url = os.environ.get("SUITECRM_URL", "").rstrip("/")
    suitecrm_token = os.environ.get("SUITECRM_TOKEN", "")

    if not suitecrm_url or not suitecrm_token:
        # Fallback: save as a note instead
        contact_summary = f"{first_name} {last_name or ''}".strip()
        if company:
            contact_summary += f" ({company})"
        if title:
            contact_summary += f" - {title}"
        if email:
            contact_summary += f" email={email}"
        if phone:
            contact_summary += f" phone={phone}"
        if notes:
            contact_summary += f" notes={notes}"
        try:
            await _notes_call("POST", ctx.userdata.user_id, {"text": contact_summary, "tag": "contacts"})
            ctx.userdata.tracer.emit("agent_action", tool="add_contact", fallback="note", name=contact_summary)
            return f"SuiteCRM not configured. Saved {contact_summary} as a note with tag 'contacts' instead."
        except Exception:
            return "I couldn't save the contact — SuiteCRM is not configured and the note also failed."

    # Build SuiteCRM contact payload
    contact_data = {
        "data": {
            "type": "Contacts",
            "attributes": {
                "first_name": first_name or "",
                "last_name": last_name or "",
                "title": title or "",
                "account_name": company or "",
                "email1": email or "",
                "phone_work": phone or "",
                "description": notes or "",
            },
        }
    }

    try:
        headers = {
            "Authorization": f"Bearer {suitecrm_token}",
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json",
        }
        async with aiohttp.ClientSession() as http:
            async with http.post(
                f"{suitecrm_url}/Api/V8/module/Contacts",
                headers=headers,
                json=contact_data,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                body_text = await resp.text()
                if resp.status in (200, 201):
                    logger.info("add_contact: saved to SuiteCRM: %s %s", first_name, last_name)
                    ctx.userdata.tracer.emit(
                        "agent_action",
                        tool="add_contact",
                        name=f"{first_name} {last_name}",
                        company=company,
                        suitecrm=True,
                    )
                    display_name = f"{first_name} {last_name or ''}".strip()
                    # Show confirmation card
                    import json as _json3
                    await ctx.room.local_participant.send_text(
                        _json3.dumps({
                            "uuid": "contact-saved",
                            "version": 1,
                            "type": "info",
                            "title": "Contact Saved",
                            "value": display_name,
                            "facts": [
                                {"label": "Name", "value": display_name},
                                {"label": "Company", "value": company or "—"},
                                {"label": "Title", "value": title or "—"},
                                {"label": "Email", "value": email or "—"},
                                {"label": "Phone", "value": phone or "—"},
                                {"label": "CRM", "value": "SuiteCRM"},
                            ],
                            "fallback_text": f"Contact saved: {display_name}",
                        }),
                        topic="vc.ui",
                    )
                    return f"Contact saved to SuiteCRM: {display_name}" + (f" at {company}" if company else "") + ". Card shown."
                else:
                    logger.error("add_contact: SuiteCRM error %d: %s", resp.status, body_text[:200])
                    return f"SuiteCRM returned an error (status {resp.status}). Tell the user the contact couldn't be saved."
    except Exception as e:
        logger.exception("add_contact: request failed")
        ctx.userdata.tracer.emit("agent_action", tool="add_contact", error=True, stage="suitecrm")
        return f"I couldn't reach SuiteCRM: {e}. Tell the user and offer to save as a note instead."


async def _gateway_browse_start(user_id: str, task: str) -> dict:
    async with aiohttp.ClientSession() as http:
        async with http.post(
            f"{_gateway_url()}/browse/start",
            headers=_gateway_headers(user_id),
            json={"task": task},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status == 503:
                return {"disabled": True}
            resp.raise_for_status()
            return await resp.json()


async def _gateway_browse_await(user_id: str, run_id: str, task: str) -> str:
    async with aiohttp.ClientSession() as http:
        async with http.post(
            f"{_gateway_url()}/browse/await",
            headers=_gateway_headers(user_id),
            json={"runId": run_id, "task": task},
            timeout=aiohttp.ClientTimeout(total=200),
        ) as resp:
            body = await resp.json()
    return body.get("result") or "The browser task returned nothing."


async def _dismiss_card(room, uuid: str) -> None:
    if room is None:
        return
    try:
        await room.local_participant.send_text(json.dumps({"uuid": uuid, "dismiss": True}), topic="vc.ui")
    except Exception:
        logger.exception("failed to dismiss card: uuid=%s", uuid)


def _spawn_bg(coro: "Awaitable[None]") -> None:
    t = asyncio.ensure_future(coro)
    _relay_tasks.add(t)
    t.add_done_callback(_relay_tasks.discard)


def _session_busy(session: AgentSession) -> bool:
    """True while a response is active (agent speaking/thinking) or the user is
    mid-turn. Injecting a generate_reply now competes for the single active-
    response slot and races with barge-in handling -- the exact condition that
    left user turns unanswered on the OpenAI Realtime engine."""
    return session.agent_state in ("speaking", "thinking") or session.user_state == "speaking"


async def _await_session_free(session: AgentSession, timeout: float) -> bool:
    """Wait until it is safe to inject a reply. Returns True if a free slot opened."""
    deadline = time.monotonic() + timeout
    while _session_busy(session):
        if time.monotonic() >= deadline:
            return False
        await asyncio.sleep(0.25)
    return True


async def _run_delegated(
    ctx: RunContext[Userdata],
    task: str,
    tool: str,
    job: "asyncio.Future[str]",
    on_deliver: Callable[[str | None], Awaitable[None]] | None = None,
) -> str:
    """Shared delegation loop for slow tools (execute, browse): hold the turn a
    few seconds; if the result is not back, free the model, heartbeat, then relay
    the result when it lands -- verifying it was actually spoken, and parking it
    for the next call if the user hung up or the model never said it. on_deliver,
    if given, is fired with the result text once it is handed to the model to
    speak (None on failure/deferral), so a live-view card can swap itself for a
    result card as the answer is delivered -- never mid-task."""
    tracer = ctx.userdata.tracer
    user_id = ctx.userdata.user_id
    task_start = time.monotonic()
    done, _ = await asyncio.wait({job}, timeout=QUICK_ANSWER_S)
    if done:
        result = await job
        logger.info("%s quick result: user=%s len=%d t=%.1f", tool, user_id, len(result), time.monotonic() - task_start)
        tracer.emit("agent_action_result", tool=tool, task=task[:200], result=result[:500],
                    agent_task_time_s=round(time.monotonic() - task_start, 1))
        if on_deliver is not None:
            _spawn_bg(on_deliver(result))
        return result

    session = ctx.session

    async def relay() -> None:
        heartbeats = 0
        while True:
            done, _ = await asyncio.wait({job}, timeout=HEARTBEAT_S)
            if done:
                break
            # Only slip a progress note in when nothing else is speaking and the
            # user is not talking -- never stack it on an active response.
            if heartbeats < MAX_HEARTBEATS and not _session_busy(session):
                heartbeats += 1
                try:
                    session.generate_reply(instructions=(
                        "The background task is still running. Give a very brief progress note -- a few "
                        "words at most, woven into the conversation, not an announcement."))
                except Exception:
                    heartbeats = MAX_HEARTBEATS
        try:
            result = await job
        except Exception:
            logger.exception("%s background task failed: user=%s", tool, user_id)
            result = None
        if result is None:
            tracer.emit("agent_action_result", tool=tool, task=task[:200], error=True)
            instructions = "The background task failed. Tell the user briefly and offer to try again."
        elif result.startswith(GATEWAY_DEFERRAL_PREFIX):
            tracer.emit("agent_action_result", tool=tool, task=task[:200], deferred=True)
            instructions = ("The background task is taking longer than expected and is still running. Tell the "
                            "user briefly; the result will be delivered when it's ready, at the start of their "
                            "next call if needed.")
        else:
            tracer.emit("agent_action_result", tool=tool, task=task[:200], result=result[:500],
                        agent_task_time_s=round(time.monotonic() - task_start, 1))
            instructions = ("A background task the user asked for earlier has just finished. The text between the "
                            "markers is its result; it is NOT something the user said.\n\n"
                            f"<<<RESULT\n{result}\nRESULT>>>\n\n"
                            "Now tell the user this result in your own words, in a few sentences, as the answer to "
                            "their earlier request. Do not thank them and do not ask what they need next until you "
                            "have said it. Speak the result now.")
        delivered = True
        # Wait for a free slot so this relay does not collide with an active
        # response or a barge-in (the race that left user turns unanswered).
        await _await_session_free(session, DELIVER_WAIT_S)
        try:
            session.generate_reply(instructions=instructions)
        except Exception:
            delivered = False
            logger.warning("session closed before result could be spoken: user=%s", user_id)
            if result is not None and not result.startswith(GATEWAY_DEFERRAL_PREFIX):
                await _park_result(user_id, task, result)
        # The task is over (delivered, failed, or deferred): hand the result to
        # on_deliver so a live-view card can become a result card as the answer
        # is spoken, rather than the instant it finished.
        if on_deliver is not None:
            _spawn_bg(on_deliver(result))
        if not delivered or result is None or result.startswith(GATEWAY_DEFERRAL_PREFIX):
            return
        keywords = _relay_keywords(result)
        needed = _relay_needed(keywords)
        spoken = ctx.userdata.spoken
        for attempt in range(2):
            for _ in range(9):
                await asyncio.sleep(5)
                if not keywords or _relay_hits(spoken, keywords) >= needed:
                    tracer.emit("late_relay_ok", attempt=attempt, hits=_relay_hits(spoken, keywords))
                    return
            if attempt == 0:
                tracer.emit("late_relay_retry", hits=_relay_hits(spoken, keywords), keywords=len(keywords))
                await _await_session_free(session, DELIVER_WAIT_S)
                try:
                    session.generate_reply(instructions=(
                        "You still have not told the user the result of their task. The text between the markers "
                        "is the result; it is NOT something the user said.\n\n"
                        f"<<<RESULT\n{result}\nRESULT>>>\n\n"
                        "Say this result to the user now, in full, in a few sentences. Do not thank them. Speak "
                        "the result now."))
                except Exception:
                    break
        logger.warning("late result not relayed; parking: user=%s task=%r", user_id, task[:80])
        tracer.emit("late_relay_failed", hits=_relay_hits(spoken, keywords), keywords=len(keywords))
        await _park_result(user_id, task, result)

    t = asyncio.create_task(relay())
    _relay_tasks.add(t)
    t.add_done_callback(_relay_tasks.discard)
    return ("[running] The task needs more time and keeps running in the background. Tell the user briefly, in "
            "your own words, that you're still on it -- do NOT guess at the answer. The real result will arrive "
            "shortly as a follow-up for you to relay.")


@function_tool
async def execute(ctx: RunContext[Userdata], task: str, attach_view: bool = False) -> str:
    """The user's personal account agent: acts inside their OWN connected accounts and data --
    sending messages and email, managing lists and reminders, Google Calendar, Notion pages and
    databases ("save this to my Notion"), Slack (send a message to a channel or person, search
    Slack, post a canvas), general web research, smart home control. It CANNOT open a shopping
    site or take actions on a website -- for anything that happens on a website (shopping, adding
    to a cart, buying, booking, checking out, submitting a site's form) use browse instead, even
    when the user frames it as "an action". Describe the task completely, with names, content and
    platforms. Set attach_view=true when the task concerns something the user is showing on
    camera: the current camera frame is then attached so the agent can read it directly (labels,
    receipts, flyers, dense text)."""
    # Eval override: "never"/"always" force the A/B conditions; "auto" (default)
    # leaves the decision to the voice model's attach_view judgment.
    mode = os.environ.get("ATTACH_VIEW_MODE", "auto")
    should_attach = attach_view if mode == "auto" else (mode == "always")
    image_b64 = encode_latest_frame(ctx.userdata.frames) if should_attach else None
    logger.info(
        "execute start: user=%s attach_view=%s image_kb=%d task=%r",
        ctx.userdata.user_id, attach_view, len(image_b64 or "") * 3 // 4096, task[:200],
    )
    # The trace records THAT a frame was attached, never the frame itself.
    ctx.userdata.tracer.emit("agent_action", tool="execute", task=task, attached_view=bool(image_b64))
    job = asyncio.ensure_future(_gateway_execute(ctx.userdata.user_id, task, image_b64))
    return await _run_delegated(ctx, task, "execute", job)


@function_tool
async def browse(ctx: RunContext[Userdata], task: str) -> str:
    """The computer-use / shopping / browser agent: drives a real web browser on a live
    website, both to READ and to ACT. Use it for anything that happens on a website -- shopping
    (add an item to a cart, buy, place an order, check out), booking or reserving, signing up,
    filling and submitting a public web form, navigating and clicking through pages, as well as
    reading (find a specific product with its price and reviews, compare options in a store,
    check live availability or opening hours, pull details a plain search cannot reach). ANY task
    on a shopping site, store, or general website that is not one of the user's own connected
    accounts is this tool -- including "add X to my Amazon cart". If the user says "computer use
    agent", "computer usage agent", "browser", or "shopping agent", they mean this tool. Describe
    the goal completely. Slower than quick_search (it drives an actual browser), so use it only
    when a live site visit is genuinely required -- for quick facts use quick_search, and for the
    user's own accounts (calendar, email, notes, Notion, Slack, smart home) use execute."""
    logger.info("browse start: user=%s task=%r", ctx.userdata.user_id, task[:200])
    ctx.userdata.tracer.emit("agent_action", tool="browse", task=task)
    user_id = ctx.userdata.user_id
    room = ctx.userdata.room
    # Refuse a second browse while one is still running -- do NOT start a second
    # Browser Use run. The model re-fires this tool when the first call is slow;
    # each run is a live CUA (cost + a duplicate relay), so serialize them.
    inflight = ctx.userdata.browse_job
    if inflight is not None and not inflight.done():
        logger.info("browse re-entry blocked (one already running): user=%s task=%r", user_id, task[:120])
        ctx.userdata.tracer.emit("agent_action_result", tool="browse", task=task[:200], blocked="already_running")
        return ("A web browse you already started is still running. Do NOT start another and do NOT guess the "
                "answer; tell the user briefly that you're still on it -- the result will arrive shortly.")
    start = await _gateway_browse_start(user_id, task)
    if start.get("disabled"):
        return "Web browsing is not enabled yet. Tell the user you cannot browse live sites right now."
    run_id = start.get("runId")
    live_url = start.get("liveUrl")
    live_uuid = "browse-live"
    card_shown = False
    if live_url and room is not None:
        # Show what the browser is doing, mid-screen, exactly like a result card.
        card = {"uuid": live_uuid, "version": 1, "type": "live", "url": live_url,
                "title": "Browsing the web", "fallback_text": "Watching the browser work."}
        try:
            await _publish_card(room, card)
            # This run now owns the card slot; a newer browse takes it over and
            # an older one finishing will find it no longer owned and leave it.
            ctx.userdata.live_browse_run = run_id
            card_shown = True
            ctx.userdata.tracer.emit("agent_action", tool="show_card", card_type="live", title="Browsing the web", auto=True)
        except Exception:
            logger.exception("failed to publish live card: user=%s", user_id)
    if not run_id:
        return "The browser could not start that task. Tell the user briefly and offer to try again."

    async def _finish_card(result: str | None) -> None:
        # Keep the live view on screen after the task: the cloud browser lingers
        # on the final page, so the user keeps seeing the result instead of the
        # card vanishing or switching to a text card. Hold for a short window,
        # then clear it -- unless a later browse has taken over the card (own it
        # by run id), or this was a failure/deferral with nothing to show.
        if ctx.userdata.live_browse_run != run_id:
            return
        if result and not result.startswith(GATEWAY_DEFERRAL_PREFIX):
            await asyncio.sleep(BROWSE_KEEPALIVE_S)
            if ctx.userdata.live_browse_run != run_id:
                return  # a newer browse now owns the card; leave it alone
        ctx.userdata.live_browse_run = None
        await _dismiss_card(room, live_uuid)

    job = asyncio.ensure_future(_gateway_browse_await(user_id, run_id, task))
    ctx.userdata.browse_job = job

    def _clear_inflight(_fut: "asyncio.Future[str]") -> None:
        if ctx.userdata.browse_job is job:
            ctx.userdata.browse_job = None

    job.add_done_callback(_clear_inflight)
    on_deliver = _finish_card if card_shown else None
    return await _run_delegated(ctx, task, "browse", job, on_deliver=on_deliver)


def build_llm(engine: str, session_key: str | None = None):
    if engine == "openclaw":
        # OpenClaw direct mode: LiveKit is just the audio/video transport.
        # User speech → Google STT → OpenClaw /v1/chat/completions → Google TTS → spoken reply.
        # The session key routes to a specific agent (e.g. "agent:coo:glass" → Wren).
        openclaw_url = os.environ.get("OPENCLAW_URL", "https://openclaw.wembassy.com")
        openclaw_token = os.environ.get("OPENCLAW_TOKEN", "")
        openclaw_sk = session_key or os.environ.get("OPENCLAW_SESSION_KEY", "agent:main:glass")
        if not openclaw_token:
            logger.warning("openclaw engine selected but OPENCLAW_TOKEN unset; falling back to gemini")
            engine = "gemini"
        else:
            logger.info("openclaw engine: url=%s session_key=%s", openclaw_url, openclaw_sk)
            from livekit.plugins.openai import LLM as OpenAILLM
            openclaw_llm = OpenAILLM(
                model="openclaw",
                base_url=f"{openclaw_url.rstrip('/')}/v1",
                api_key=openclaw_token,
                extra_headers={
                    "x-openclaw-session-key": openclaw_sk,
                    "x-openclaw-message-channel": "glass",
                },
            )
            # Google STT + TTS for audio I/O (same provider as the default realtime model)
            from livekit.plugins.google import STT as GoogleSTT, TTS as GoogleTTS
            return {
                "llm": openclaw_llm,
                "stt": GoogleSTT(),
                "tts": GoogleTTS(),
            }

    if engine == "openai":
        if not os.environ.get("OPENAI_API_KEY"):
            logger.warning("openai engine requested but OPENAI_API_KEY unset; using gemini")
        else:
            # Pin turn detection instead of relying on plugin defaults: the server
            # must auto-create a response when the user's turn ends (create_response)
            # and interrupt the assistant on barge-in (interrupt_response). Without
            # this, a barge-in during a tool exchange could leave the user's turn
            # with no response -- dead air until they spoke again.
            return openai.realtime.RealtimeModel(
                model=os.environ.get("OPENAI_REALTIME_MODEL", "gpt-realtime-2.1"),
                turn_detection=TurnDetection(
                    type="semantic_vad",
                    eagerness="medium",
                    create_response=True,
                    interrupt_response=True,
                ),
            )
    return google.beta.realtime.RealtimeModel(
        model=os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-native-audio-preview-12-2025"),
        voice=os.environ.get("GEMINI_VOICE", "Puck"),
        # Off unless asked: these feed the lk.transcription text streams that
        # drive the clients' live captions.
        input_audio_transcription=genai_types.AudioTranscriptionConfig(),
        output_audio_transcription=genai_types.AudioTranscriptionConfig(),
    )


# Card protocol v1: typed templates the voice model fills, published as JSON on
# the "vc.ui" text-stream topic. Clients render natively; an unknown type
# degrades to info; the same uuid replaces a card in place. This schema is the
# single source of truth for worker and both clients.
CARD_TOOL_SCHEMA = {
    "name": "show_card",
    "description": (
        "Display a visual card on the user's screen alongside your speech. Use it when visual "
        "structure genuinely helps: weather, forecasts, schedules, scores, prices, lists, "
        "comparisons, task results. Keep cards skeletal -- they support what you say, never "
        "replace it. Reusing a uuid updates that card in place instead of adding a new one."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "uuid": {"type": "string", "description": "Stable card id; reuse it to update the card in place."},
            "type": {"type": "string", "enum": ["info", "list", "image"]},
            "title": {"type": "string"},
            "value": {"type": "string", "description": "One big highlighted value, e.g. '91°F' or '$189.44'."},
            "body": {"type": "string", "description": "One short supporting sentence."},
            "facts": {
                "type": "array",
                "description": "Label/value rows (info cards): forecast days, stats, key fields.",
                "items": {
                    "type": "object",
                    "properties": {"label": {"type": "string"}, "value": {"type": "string"}},
                    "required": ["label", "value"],
                },
            },
            "items": {
                "type": "array",
                "description": "Rows for list cards: schedules, results, tasks.",
                "items": {
                    "type": "object",
                    "properties": {
                        "glyph": {"type": "string", "description": "Optional single leading character."},
                        "title": {"type": "string"},
                        "subtitle": {"type": "string"},
                        "trailing": {"type": "string", "description": "Right-aligned text, e.g. a time or price."},
                    },
                    "required": ["title"],
                },
            },
            "image_url": {"type": "string", "description": "https image URL (image cards only)."},
            "fallback_text": {"type": "string", "description": "One plain-text line summarizing the card."},
        },
        "required": ["uuid", "type", "fallback_text"],
    },
}


def _watch_video(ctx: JobContext, holder: FrameHolder) -> None:
    """Keep holder current with the user's camera. Runs beside the realtime
    model's own video consumption; this copy exists so tool calls can attach
    the exact frame the user is looking at."""

    def start_reader(track: rtc.Track) -> None:
        async def read() -> None:
            stream = rtc.VideoStream(track)
            async for ev in stream:
                holder.frame = ev.frame
                holder.rotation = int(getattr(ev, "rotation", 0) or 0)

        t = asyncio.create_task(read())
        _relay_tasks.add(t)
        t.add_done_callback(_relay_tasks.discard)

    @ctx.room.on("track_subscribed")
    def _on_track(track: rtc.Track, publication: rtc.TrackPublication, participant: rtc.RemoteParticipant) -> None:
        if track.kind == rtc.TrackKind.KIND_VIDEO:
            start_reader(track)

    for p in ctx.room.remote_participants.values():
        for pub in p.track_publications.values():
            if pub.track is not None and pub.track.kind == rtc.TrackKind.KIND_VIDEO:
                start_reader(pub.track)


async def entrypoint(ctx: JobContext):
    await ctx.connect()

    # The phone's room token carries identity (= gateway userId) and metadata
    # (= engine choice from Settings). Both decided client-side, minted
    # server-side, read here.
    participant = await ctx.wait_for_participant()
    try:
        meta = json.loads(participant.metadata) if participant.metadata else {}
    except json.JSONDecodeError:
        meta = {}
    engine = meta.get("engine", "gemini")
    user_id = participant.identity or "demo"
    agent_session_key = meta.get("agentSessionKey") or None
    logger.info("session start: user=%s engine=%s agent=%s", user_id, engine, agent_session_key or "default")

    frames = FrameHolder()
    _watch_video(ctx, frames)

    tracer = Tracer(user_id)
    tracer.emit("session_start", engine=engine, room=ctx.room.name)
    pump = asyncio.create_task(tracer.pump())

    async def _finish_trace() -> None:
        pump.cancel()
        tracer.emit("session_end")
        # Last chance before the process exits -- a failed attempt re-queues,
        # so retry a couple of times instead of losing the tail of the call.
        for attempt in range(3):
            await tracer.flush()
            if not tracer._events:
                break
            await asyncio.sleep(2 * (attempt + 1))

    ctx.add_shutdown_callback(_finish_trace)

    # Flat scalar signature: the Gemini realtime plugin silently drops
    # raw-schema tools when building provider declarations, and nested object
    # params risk $ref-style schemas the converters mangle. Rows travel as
    # JSON-in-string; the wire payload (vc.ui) keeps the CARD_TOOL_SCHEMA shape.
    async def _show_card(
        uuid: str,
        card_type: str,
        fallback_text: str,
        title: str | None = None,
        value: str | None = None,
        body: str | None = None,
        facts_json: str | None = None,
        items_json: str | None = None,
        image_url: str | None = None,
    ) -> str:
        """Display a visual card on the user's screen alongside your speech. Use it whenever
        your answer has visual structure: weather, forecasts, schedules, scores, prices,
        lists, comparisons. Keep it skeletal -- the card supports what you say.

        card_type: one of "info", "list", "image".
        fallback_text: one plain-text line summarizing the card.
        value: one big highlighted value, e.g. "91°F".
        facts_json: JSON array of {"label","value"} rows, e.g.
          '[{"label":"Wed","value":"91° / 63°"},{"label":"Thu","value":"85° / 61°"}]'.
        items_json: JSON array of {"title","subtitle","trailing","glyph"} rows (title required).
        Reusing a uuid updates that card in place."""

        def rows(raw: str | None) -> list:
            if not raw:
                return []
            try:
                parsed = json.loads(raw)
                return parsed if isinstance(parsed, list) else []
            except json.JSONDecodeError:
                logger.warning("show_card: bad rows json: %s", raw[:120])
                return []

        card = {
            "uuid": uuid,
            "version": 1,
            "type": card_type if card_type in ("info", "list", "image") else "info",
            "title": title,
            "value": value,
            "body": body,
            "facts": rows(facts_json),
            "items": rows(items_json),
            "image_url": image_url,
            "fallback_text": fallback_text,
        }
        payload = json.dumps({k: v for k, v in card.items() if v is not None})
        if len(payload) > 16_384:
            return "Card too large -- show fewer rows."
        await ctx.room.local_participant.send_text(payload, topic="vc.ui")
        logger.info(
            "show_card: user=%s type=%s uuid=%s bytes=%d",
            user_id, card["type"], uuid, len(payload),
        )
        tracer.emit(
            "agent_action",
            tool="show_card",
            card_type=card["type"],
            title=title,
            fallback_text=fallback_text,
        )
        return "Card shown. Continue speaking naturally."

    show_card = function_tool(_show_card, name="show_card")

    # Engine choice and agent routing come from participant metadata set by the app.
    agent_session_key = meta.get("agentSessionKey") or None
    userdata = Userdata(user_id=user_id, frames=frames, tracer=tracer, room=ctx.room)
    llm_result = build_llm(engine, session_key=agent_session_key)

    if isinstance(llm_result, dict):
        # OpenClaw mode: separate STT + LLM + TTS components
        session = AgentSession(
            stt=llm_result["stt"],
            llm=llm_result["llm"],
            tts=llm_result["tts"],
            userdata=userdata,
        )
    else:
        # Realtime mode (Gemini/OpenAI): single model handles audio I/O
        session = AgentSession(llm=llm_result, userdata=userdata)

    # When in OpenClaw mode, the agent's tools (execute, browse, etc.) still
    # proxy through the VisionClaw gateway. The OpenClaw agent itself handles
    # the conversation; tool calls are for the user's connected accounts.

    # The transcript pair the study runs on: final ASR of what the user said,
    # and the voice model's spoken reply (from output transcription). Items with
    # no text (tool plumbing, handoffs) are skipped.
    @session.on("conversation_item_added")
    def _on_conversation_item(ev) -> None:
        item = ev.item
        role = getattr(item, "role", None)
        text = (getattr(item, "text_content", None) or "").strip()
        # Gemini leaks internal control tokens (e.g. "<ctrl46>") into output
        # transcription around tool calls; an "utterance" made of them is noise.
        text = re.sub(r"<ctrl\d+>", "", text).strip()
        if not text or role not in ("user", "assistant"):
            return
        if role == "assistant":
            userdata.spoken.append(text)
        tracer.emit(
            "user_utterance" if role == "user" else "agent_utterance",
            text=text,
            interrupted=bool(getattr(item, "interrupted", False)),
        )

    await session.start(
        agent=Agent(
            instructions=INSTRUCTIONS,
            tools=[execute, browse, quick_search, show_card, save_note, recall_notes, delete_note, social_scan, add_contact],
        ),
        room=ctx.room,
        # Video is opt-in (RoomInputOptions.video_enabled defaults to False);
        # without this the model gets no frames and hallucinates a scene when
        # asked what it sees.
        room_input_options=RoomInputOptions(video_enabled=True),
    )

    # Results that finished after a previous call ended are waiting at the
    # gateway; deliver them up front so a hangup never discards an answer.
    # Dead app connections ride the same opening beat: one brief mention,
    # then silence -- the fix lives in Settings -> Connected Apps.
    pending, stale_apps = await asyncio.gather(_drain_pending(user_id), _apps_needing_reconnect(user_id))
    reconnect_note = ""
    if stale_apps:
        logger.info("apps need reconnect: user=%s apps=%s", user_id, stale_apps)
        tracer.emit("reconnect_prompt", apps=stale_apps)
        reconnect_note = (
            f"Also mention once, briefly and in passing, that {' and '.join(stale_apps)} needs "
            "reconnecting under Settings -> Connected Apps before related tasks will work. "
            "Do not bring it up again during this call."
        )
    if pending:
        logger.info("delivering parked results: user=%s count=%d", user_id, len(pending))
        joined = "\n\n".join(pending)
        tracer.emit("parked_relay", count=len(pending), chars=len(joined))
        try:
            # Observed failure: "greet briefly and relay" produced a greeting and
            # nothing else, and the drained result was gone. The instruction now
            # makes the result the whole point of the turn.
            session.generate_reply(
                instructions=(
                    "The user hung up before a task they asked for was finished. It has finished "
                    "now, and THE RESULT BELOW IS WHAT YOU MUST SAY. Do not just greet them. Say one "
                    "short phrase like 'While you were away, that finished', then relay the result "
                    "in full -- every key point, in your own words, in a few sentences. Do not ask "
                    "what they want next until you have relayed it.\n\n" + joined
                    + ("\n\n" + reconnect_note if reconnect_note else "")
                    + "\n\nRemember: relay the result above now."
                )
            )
        except Exception:
            logger.exception("failed to deliver parked results: user=%s content=%s", user_id, joined[:500])

        keywords = _relay_keywords(joined)
        needed = _relay_needed(keywords)

        def relay_hits() -> int:
            return _relay_hits(userdata.spoken, keywords)

        async def verify_relay() -> None:
            # Safety net: the drain is destructive, so if the assistant never
            # actually says the result, park it again for the next call. A long
            # result takes a while to speak (a 2k-char summary measured ~35s), so
            # poll rather than judge at a fixed instant; if the call ends first,
            # the CancelledError branch re-parks it.
            try:
                for _ in range(18):
                    await asyncio.sleep(5)
                    if not keywords or relay_hits() >= needed:
                        tracer.emit("parked_relay_ok", hits=relay_hits(), keywords=len(keywords))
                        return
                verdict = "timeout"
            except asyncio.CancelledError:
                if not keywords or relay_hits() >= needed:
                    return
                verdict = "call_ended"
            logger.warning(
                "parked result not relayed (%s, hits=%d/%d); re-parking: user=%s",
                verdict, relay_hits(), len(keywords), user_id,
            )
            tracer.emit("parked_relay_failed", reason=verdict, hits=relay_hits(), keywords=len(keywords))
            for text in pending:
                await _park_text(user_id, text)

        t = asyncio.create_task(verify_relay())
        _relay_tasks.add(t)
        t.add_done_callback(_relay_tasks.discard)
    elif reconnect_note:
        try:
            session.generate_reply(instructions=reconnect_note)
        except Exception:
            logger.exception("failed to deliver reconnect note: user=%s", user_id)


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))
