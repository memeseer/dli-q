import os
import json
import time
import re
import requests
from datetime import datetime, timedelta, timezone
from sys import stderr
from loguru import logger

# =============================================================================
# Quote of Day (Discord)
# - fetch until since_dt (no 2000 fixed limit; safety cap exists)
# - ignore ONLY leaks/dox/invites/tokens + minimal hate/slur filter (variant 1)
# - humor-first selection, joke-gate shortlist
# - Discord post format like screenshot
# - Outputs:
#   1) quote_run.json (debug, includes all_quotes)
#   2) quotes_selected_only.json (APPEND-ONLY archive, only meta + selected)
# =============================================================================

ENV_PATH = os.getenv("ENV_PATH", ".env")
try:
    from dotenv import load_dotenv  # type: ignore
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH, override=False)
except Exception:
    pass

API_BASE = "https://discord.com/api/v9"
OPENROUTER_BASE = "https://openrouter.ai/api/v1"
DISCORD_CDN = "https://cdn.discordapp.com"

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
QUOTE_SOURCE_CHANNELS = os.getenv("QUOTE_SOURCE_CHANNELS", "").strip()
QUOTE_POST_CHANNEL_ID = os.getenv("QUOTE_POST_CHANNEL_ID", "").strip()
GUILD_ID = os.getenv("GUILD_ID", "").strip()

LOOKBACK_HOURS = int(os.getenv("QUOTE_LOOKBACK_HOURS", "24"))

HARD_MAX_MESSAGES_PER_CHANNEL = int(os.getenv("QUOTE_HARD_MAX_MESSAGES_PER_CHANNEL", "20000"))
PAGE_SLEEP_SEC = float(os.getenv("QUOTE_PAGE_SLEEP_SEC", "0.2"))

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini").strip()
MAX_CANDIDATES_TO_LLM = int(os.getenv("QUOTE_MAX_CANDIDATES_TO_LLM", "80"))
LLM_RETRIES = int(os.getenv("QUOTE_LLM_RETRIES", "3"))
LLM_RETRY_SLEEP_SEC = float(os.getenv("QUOTE_LLM_RETRY_SLEEP_SEC", "1.0"))
LLM_LOG_RAW_ON_ERROR = os.getenv("QUOTE_LLM_LOG_RAW_ON_ERROR", "1") == "1"

DRY_RUN = os.getenv("QUOTE_DRY_RUN", "0") == "1"

RUN_OUT_JSON_PATH = os.getenv("QUOTE_RUN_OUT_JSON_PATH", "quote_run.json")
STATE_PATH = os.getenv("QUOTE_STATE_PATH", "quote_state.json")

# NEW: append-only selected-only archive
SELECTED_ONLY_ARCHIVE_PATH = os.getenv("QUOTE_SELECTED_ONLY_ARCHIVE_PATH", "quotes_selected_only.json")

QUOTE_LANG = os.getenv("QUOTE_LANG", "ru").strip().lower()
DISCORD_HEADER = os.getenv("QUOTE_DISCORD_HEADER", "ВНИАНИЕ!!! ЦИТАТА ДНЯ").strip()

logger.remove()
logger.add(stderr, format="<white>{time:HH:mm:ss}</white> | <level>{level: <8}</level> | <white>{message}</white>")

if not DISCORD_TOKEN:
    raise SystemExit("ERROR: DISCORD_TOKEN is empty (env DISCORD_TOKEN).")
if not QUOTE_SOURCE_CHANNELS:
    raise SystemExit("ERROR: QUOTE_SOURCE_CHANNELS is empty (env QUOTE_SOURCE_CHANNELS).")

if not DRY_RUN:
    if not QUOTE_POST_CHANNEL_ID or not QUOTE_POST_CHANNEL_ID.isdigit():
        raise SystemExit("ERROR: QUOTE_POST_CHANNEL_ID is empty or not numeric, but DRY_RUN=0.")
    if not OPENROUTER_API_KEY:
        raise SystemExit("ERROR: OPENROUTER_API_KEY is empty, but DRY_RUN=0.")
else:
    if not OPENROUTER_API_KEY:
        logger.info("DRY_RUN=1: OPENROUTER_API_KEY not set -> LLM disabled; fallback used.")

discord_session = requests.Session()
discord_session.headers.update({
    "authorization": DISCORD_TOKEN,
    "user-agent": "Mozilla/5.0",
    "accept-encoding": "gzip, deflate",
    "content-type": "application/json",
})
openrouter_session = requests.Session()

# =============================================================================
# Filters: leaks/dox only + minimal hate/slur filter (variant 1)
# =============================================================================
BLOCK_PATTERNS = [
    r"(?:https?://)?(?:www\.)?discord\.gg/\S+",
    r"(?:https?://)?discord\.com/invite/\S+",
    r"\bpassword\b", r"\bpasswd\b", r"\bsecret\b", r"\bapi[_-]?key\b",
    r"\baccess[_-]?token\b", r"\brefresh[_-]?token\b",
    r"\bprivate key\b", r"\bseed phrase\b", r"\brecovery phrase\b",
    r"\bsk-[A-Za-z0-9]{20,}\b",
    r"\bghp_[A-Za-z0-9]{20,}\b",
    r"\bgithub_pat_[A-Za-z0-9_]{20,}\b",
    r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b",
    r"\bAKIA[0-9A-Z]{16}\b",
    r"\b(?:AIza)[A-Za-z0-9_\-]{20,}\b",
    r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\b",
]
BLOCK_RE = re.compile("|".join(BLOCK_PATTERNS), re.IGNORECASE)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PHONE_RE = re.compile(r"(?<!\d)(?:\+?\d[\d\s\-\(\)]{7,}\d)(?!\d)")

def looks_leak_or_dox(text: str) -> bool:
    if not text or not text.strip():
        return True
    if BLOCK_RE.search(text):
        return True
    if EMAIL_RE.search(text):
        return True
    if PHONE_RE.search(text):
        return True
    return False

HATE_RE = re.compile(r"\b(nigg(?:a|er)|faggot|ниггер)\b", re.IGNORECASE)

def looks_hate(text: str) -> bool:
    return bool(HATE_RE.search(text or ""))

# =============================================================================
# Humor ranking
# =============================================================================
JOKE_MARKERS_RE = re.compile(
    r"(\b(ахах|ахаха|хаха|хах|ору|орнул|лол|кек|кекв|ржака|пхаха|жиза|кринж|угар)\b|[😂🤣😹]|(:\)|;\)|=\))|(\)\)\)+))",
    re.IGNORECASE
)
SERIOUS_RE = re.compile(
    r"\b(важн|мудр|актуальн|напоминан|прежде всего|самое главное|в современном мире|цель|развитие|мотивац|совет)\b",
    re.IGNORECASE
)
CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")

def is_ru(text: str) -> bool:
    return bool(CYRILLIC_RE.search(text or ""))

def is_joke_like(text: str) -> bool:
    if not text:
        return False
    if JOKE_MARKERS_RE.search(text):
        return True
    if text.count(")") >= 2 or "))" in text:
        return True
    return False

def humor_score(text: str, reactions: list, reply_count: int) -> float:
    t = text or ""
    l = len(t)

    react_sum = 0
    for r in reactions or []:
        react_sum += int(r.get("count") or 0)

    score = react_sum * 2.0 + reply_count * 1.2

    if t.count(")") >= 2:
        score += 5.0
    if re.search(r"\b(хаха|ахаха|ахах|хах)\b", t, re.IGNORECASE):
        score += 6.0
    if re.search(r"\b(ору|орнул|ржака|пхаха|кек|лол)\b", t, re.IGNORECASE):
        score += 4.0
    if re.search(r"[😂🤣😹]", t):
        score += 4.0

    if 10 <= l <= 160:
        score += 2.0
    elif 10 <= l <= 260:
        score += 0.8

    if SERIOUS_RE.search(t):
        score -= 6.0

    if l > 450:
        score -= 3.0

    return score

# =============================================================================
# Helpers
# =============================================================================
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)

def build_message_link(guild_id: str, channel_id: str, message_id: str) -> str:
    if guild_id and str(guild_id).isdigit():
        return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"
    return ""

def load_json(path: str, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default

def save_json(path: str, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def avatar_url(author: dict) -> str:
    uid = str(author.get("id") or "").strip()
    av = author.get("avatar")
    if uid and av:
        ext = "gif" if str(av).startswith("a_") else "png"
        return f"{DISCORD_CDN}/avatars/{uid}/{av}.{ext}?size=256"
    idx = int(uid) % 6 if uid.isdigit() else 0
    return f"{DISCORD_CDN}/embed/avatars/{idx}.png"

def redact_keep_style(text: str) -> str:
    if not text:
        return ""
    t = text.strip()
    t = re.sub(r"```.*?```", "[code]", t, flags=re.DOTALL)
    t = re.sub(r"`[^`]+`", "[code]", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

# =============================================================================
# Discord HTTP
# =============================================================================
class NoAccessError(Exception):
    pass

def discord_get_json(url: str, max_retries: int = 8, timeout: int = 25):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = discord_session.get(url, timeout=timeout)
            if r.status_code == 429:
                try:
                    ra = float(r.json().get("retry_after", 1.5))
                except Exception:
                    ra = 1.5
                time.sleep(max(ra, 0.2))
                continue
            if r.status_code == 403:
                try:
                    j = r.json()
                    if j.get("code") == 50001:
                        raise NoAccessError()
                except NoAccessError:
                    raise
                except Exception:
                    pass
            if r.status_code >= 400:
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                raise RuntimeError(last_err)
            return r.json()
        except NoAccessError:
            raise
        except Exception as e:
            last_err = str(e)
            if attempt == max_retries:
                raise
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(last_err or "Unknown error")

def discord_post_json(url: str, payload: dict, max_retries: int = 8, timeout: int = 25):
    last_err = None
    body = json.dumps(payload, ensure_ascii=False)
    for attempt in range(1, max_retries + 1):
        try:
            r = discord_session.post(url, data=body.encode("utf-8"), timeout=timeout)
            if r.status_code == 429:
                try:
                    ra = float(r.json().get("retry_after", 1.5))
                except Exception:
                    ra = 1.5
                time.sleep(max(ra, 0.2))
                continue
            if r.status_code == 403:
                raise NoAccessError()
            if r.status_code >= 400:
                last_err = f"HTTP {r.status_code}: {r.text[:200]}"
                raise RuntimeError(last_err)
            return r.json() if r.text else {}
        except NoAccessError:
            raise
        except Exception as e:
            last_err = str(e)
            if attempt == max_retries:
                raise
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(last_err or "Unknown error")

# =============================================================================
# Fetch until since_dt (no fixed 2000 limit; safety cap exists)
# =============================================================================
def fetch_recent_messages_channel_until(channel_id: str, since_dt: datetime, hard_max_messages: int):
    out = []
    before = None
    pages = 0

    while True:
        if len(out) >= hard_max_messages:
            logger.warning(f"Hard cap reached channel={channel_id} msgs={len(out)} cap={hard_max_messages}")
            break

        url = f"{API_BASE}/channels/{channel_id}/messages?limit=100"
        if before:
            url += f"&before={before}"

        try:
            page = discord_get_json(url)
        except NoAccessError:
            logger.warning(f"NO ACCESS channel={channel_id}")
            break

        if not page:
            break

        pages += 1
        out.extend(page)
        before = page[-1]["id"]

        try:
            last_ts = parse_iso(page[-1].get("timestamp") or "1970-01-01T00:00:00+00:00")
        except Exception:
            last_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)

        if pages % 10 == 0:
            logger.info(f"Fetched channel={channel_id} pages={pages} msgs={len(out)} last_ts={last_ts.isoformat()}")

        if last_ts < since_dt:
            break

        time.sleep(PAGE_SLEEP_SEC)

    filtered = []
    for m in out:
        ts = m.get("timestamp")
        if not ts:
            continue
        try:
            dt = parse_iso(ts)
        except Exception:
            continue
        if dt >= since_dt:
            filtered.append(m)
    return filtered

# =============================================================================
# LLM pick (ID-based) + fallback
# =============================================================================
def _strip_json_fences(content: str) -> str:
    c = (content or "").strip()
    c = re.sub(r"^```(?:json)?\s*", "", c)
    c = re.sub(r"\s*```$", "", c)
    return c.strip()

def _normalize_llm_pick(out: dict) -> dict:
    if not isinstance(out, dict):
        out = {}
    quote_mid = str(out.get("quote_message_id") or out.get("message_id") or "").strip()
    why = str(out.get("why") or out.get("reason") or "").strip()
    tags = out.get("tags") or []
    alt = out.get("alt_quotes") or out.get("alternatives") or []
    if not isinstance(tags, list):
        tags = []
    tags = [str(t).strip() for t in tags if str(t).strip()]
    if not isinstance(alt, list):
        alt = []
    alt2 = []
    for a in alt[:2]:
        if isinstance(a, dict):
            mid = str(a.get("message_id") or a.get("quote_message_id") or "").strip()
            if mid:
                alt2.append({"message_id": mid})
        elif isinstance(a, str) and a.strip():
            alt2.append({"message_id": a.strip()})
    return {"quote_message_id": quote_mid, "why": why, "tags": tags, "alt_quotes": alt2}

def openrouter_pick_funny_quote(short_list: list[dict]) -> dict:
    if not OPENROUTER_API_KEY:
        return {"quote_message_id": "", "why": "LLM disabled", "tags": [], "alt_quotes": []}

    headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}

    candidates = []
    for c in short_list[:MAX_CANDIDATES_TO_LLM]:
        candidates.append({
            "message_id": c["message_id"],
            "text": c["text"],
            "author_name": c.get("author_name", ""),
            "timestamp": c.get("timestamp", ""),
            "score": c.get("score", 0.0),
            "reactions_sum": c.get("reactions_sum", 0),
            "reply_count": c.get("reply_count", 0),
        })

    lang_rule = "Выбирай преимущественно русскоязычные сообщения, если есть выбор.\n" if QUOTE_LANG == "ru" else ""

    system = (
        "Ты выбираешь ЦИТАТУ ДНЯ из сообщений Discord.\n"
        "Это деген/мем-сообщество. Нужна САМАЯ СМЕШНАЯ/УГАРНАЯ реплика.\n"
        "Мат и токсичный стиль допустимы, если это шутка.\n"
        "НЕ выбирай мотивацию/советы/мудрость/лекции.\n"
        + lang_rule +
        "НЕ выбирай: сливы (ключи/токены/пароли/инвайты), доксинг/контакты/адреса, призывы к насилию, хейт по защищённым признакам.\n"
        "ВАЖНО: НИЧЕГО НЕ ПЕРЕПИСЫВАЙ. Просто выбери message_id.\n"
        "Ответ СТРОГО валидный JSON БЕЗ markdown:\n"
        "{\n"
        '  "quote_message_id":"<message_id из candidates>",\n'
        '  "why":"<коротко почему смешно>",\n'
        '  "tags":["угар","кринж","мем" ...],\n'
        '  "alt_quotes":[{"message_id":"..."},{"message_id":"..."}]\n'
        "}\n"
    )

    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps({"candidates": candidates}, ensure_ascii=False)},
        ],
        "temperature": 0.7,
    }

    last_raw = ""
    last_err = None
    for attempt in range(1, LLM_RETRIES + 1):
        try:
            r = openrouter_session.post(
                f"{OPENROUTER_BASE}/chat/completions",
                headers=headers,
                data=json.dumps(payload).encode("utf-8"),
                timeout=60,
            )
            if r.status_code >= 400:
                raise RuntimeError(f"OpenRouter HTTP {r.status_code}: {r.text[:400]}")
            data = r.json()
            content = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "")
            last_raw = content
            content = _strip_json_fences(content)
            if not content:
                raise RuntimeError("OpenRouter returned empty content")
            out = json.loads(content)
            pick = _normalize_llm_pick(out)
            if pick.get("quote_message_id"):
                return pick
            raise RuntimeError("LLM JSON but empty quote_message_id")
        except Exception as e:
            last_err = str(e)
            logger.warning(f"LLM pick attempt {attempt}/{LLM_RETRIES} failed: {last_err}")
            if attempt < LLM_RETRIES:
                time.sleep(LLM_RETRY_SLEEP_SEC)

    if LLM_LOG_RAW_ON_ERROR and last_raw:
        logger.error(f"LLM raw response (first 1200 chars): {last_raw[:1200]}")

    return {"quote_message_id": "", "why": f"LLM failed: {last_err or 'unknown'}", "tags": [], "alt_quotes": []}

# =============================================================================
# Discord post format like screenshot
# =============================================================================
def format_discord_message(author_id: str, quote_text: str) -> str:
    mention = f"<@{author_id}>" if author_id and author_id.isdigit() else "@unknown"
    qt = quote_text.replace("\n", " ").strip()
    return f"# {DISCORD_HEADER}\n\n© {mention}: \"**{qt}**\""

def post_quote_to_discord(content: str):
    payload = {"content": content}
    return discord_post_json(f"{API_BASE}/channels/{QUOTE_POST_CHANNEL_ID}/messages", payload)

# =============================================================================
# Selected-only archive (append one record per run)
# =============================================================================
def append_selected_only_record(path: str, record: dict):
    """
    File format: JSON array
    [
      {schema, generated_at, since, lookback_hours, dry_run, selected},
      ...
    ]
    """
    arr = load_json(path, [])
    if not isinstance(arr, list):
        arr = []

    # Avoid accidental duplicates (same selected message_id + generated_at not needed)
    # We'll dedupe only by selected.message_id
    mid = str((record.get("selected") or {}).get("message_id") or "").strip()
    if mid:
        for it in arr:
            if str((it.get("selected") or {}).get("message_id") or "").strip() == mid:
                # already present -> do nothing
                return

    arr.append(record)
    save_json(path, arr)

# =============================================================================
# Main
# =============================================================================
def main():
    state = load_json(STATE_PATH, {})
    now = now_utc()
    since_dt = now - timedelta(hours=LOOKBACK_HOURS)

    channel_ids = [c.strip() for c in QUOTE_SOURCE_CHANNELS.split(",") if c.strip().isdigit()]
    logger.info(
        f"QuoteOfDay | lookback={LOOKBACK_HOURS}h since={since_dt.isoformat()} | "
        f"channels={len(channel_ids)} | dry_run={DRY_RUN} | hard_cap={HARD_MAX_MESSAGES_PER_CHANNEL}/ch"
    )

    all_quotes = []
    candidates = []
    total_msgs_scanned = 0

    for idx, cid in enumerate(channel_ids, start=1):
        logger.info(f"[{idx}/{len(channel_ids)}] Fetch channel={cid}")
        msgs = fetch_recent_messages_channel_until(cid, since_dt, HARD_MAX_MESSAGES_PER_CHANNEL)
        total_msgs_scanned += len(msgs)

        for m in msgs:
            raw = m.get("content") or ""
            if not raw:
                continue

            text = redact_keep_style(raw)

            if looks_leak_or_dox(text) or looks_hate(text):
                continue

            author = m.get("author") or {}
            author_name = (author.get("global_name") or author.get("username") or "unknown").strip()
            author_id = str(author.get("id") or "").strip()
            pfp = avatar_url(author)

            ts = m.get("timestamp") or ""
            mid = str(m.get("id") or "").strip()

            reactions_raw = m.get("reactions") or []
            reply_count = int(m.get("reply_count") or 0)

            reactions_sum = 0
            reactions_norm = []
            for r in reactions_raw:
                reactions_sum += int(r.get("count") or 0)
                reactions_norm.append({
                    "emoji": (r.get("emoji") or {}).get("name"),
                    "count": int(r.get("count") or 0),
                })

            q = {
                "message_id": mid,
                "channel_id": str(cid),
                "timestamp": ts,
                "author_name": author_name,
                "author_id": author_id,
                "author_avatar_url": pfp,
                "text": text,
                "text_raw": raw,
                "is_ru": is_ru(text),
            }
            sc = humor_score(text, reactions_norm, reply_count)

            all_quotes.append(q)
            candidates.append({
                **q,
                "score": sc,
                "reactions": reactions_norm,
                "reactions_sum": reactions_sum,
                "reply_count": reply_count,
            })

    if not candidates:
        logger.warning("No candidates kept. Writing run output and exiting.")
        run_out = {
            "schema": 1,
            "generated_at": now.isoformat(),
            "since": since_dt.isoformat(),
            "lookback_hours": LOOKBACK_HOURS,
            "dry_run": DRY_RUN,
            "selected": None,
            "all_quotes": all_quotes,
            "debug": {"total_messages_scanned": total_msgs_scanned, "kept_quotes": len(all_quotes)},
        }
        save_json(RUN_OUT_JSON_PATH, run_out)
        return

    # Prefer RU if configured and available
    if QUOTE_LANG == "ru":
        ru_candidates = [c for c in candidates if c.get("is_ru")]
        candidates_for_rank = ru_candidates if ru_candidates else candidates
    else:
        candidates_for_rank = candidates

    candidates_for_rank.sort(key=lambda x: x["score"], reverse=True)

    # Joke-gate shortlist
    joke_candidates = [c for c in candidates_for_rank if is_joke_like(c["text"])]
    if joke_candidates:
        short_list = joke_candidates[:MAX_CANDIDATES_TO_LLM]
        shortlist_mode = "joke_gate"
    else:
        short_list = candidates_for_rank[:MAX_CANDIDATES_TO_LLM]
        shortlist_mode = "top_score"

    logger.success(
        f"Scanned={total_msgs_scanned} | kept_quotes={len(all_quotes)} | "
        f"candidates={len(candidates)} | shortlist={len(short_list)} | mode={shortlist_mode}"
    )

    pick = openrouter_pick_funny_quote(short_list)
    chosen_id = str(pick.get("quote_message_id") or "").strip()

    if not chosen_id:
        chosen = short_list[0]
        chosen_id = chosen["message_id"]
        pick["why"] = pick.get("why") or "fallback: joke-gated top"
        pick["tags"] = pick.get("tags") or ["угар", "мем"]

    chosen_meta = next((c for c in candidates if c["message_id"] == chosen_id), None) or short_list[0]

    link = ""
    if GUILD_ID and chosen_meta.get("channel_id") and chosen_meta.get("message_id"):
        link = build_message_link(GUILD_ID, chosen_meta["channel_id"], chosen_meta["message_id"])

    selected = {
        "message_id": chosen_meta["message_id"],
        "channel_id": chosen_meta.get("channel_id", ""),
        "timestamp": chosen_meta.get("timestamp", ""),
        "author_name": chosen_meta.get("author_name", ""),
        "author_id": chosen_meta.get("author_id", ""),
        "author_avatar_url": chosen_meta.get("author_avatar_url", ""),
        "text": chosen_meta.get("text", ""),
        "text_raw": chosen_meta.get("text_raw", ""),
        "is_ru": bool(chosen_meta.get("is_ru", False)),
        "link": link,
        "why": str(pick.get("why") or "").strip(),
        "tags": pick.get("tags") or [],
        "alt_quotes": pick.get("alt_quotes") or [],
        "score": chosen_meta.get("score"),
        "shortlist_mode": shortlist_mode,
    }

    # 1) Debug run dump (as before)
    run_out = {
        "schema": 1,
        "generated_at": now.isoformat(),
        "since": since_dt.isoformat(),
        "lookback_hours": LOOKBACK_HOURS,
        "dry_run": DRY_RUN,
        "selected": selected,
        "all_quotes": all_quotes,
        "debug": {
            "total_messages_scanned": total_msgs_scanned,
            "kept_quotes": len(all_quotes),
            "candidates": len(candidates),
            "shortlist": len(short_list),
            "llm_used": bool(OPENROUTER_API_KEY),
        },
    }
    save_json(RUN_OUT_JSON_PATH, run_out)
    logger.success(f"Saved run dump: {RUN_OUT_JSON_PATH}")

    # 2) Selected-only record (append one per run) — ALWAYS (even dry-run)
    selected_only_record = {
        "schema": 1,
        "generated_at": now.isoformat(),
        "since": since_dt.isoformat(),
        "lookback_hours": LOOKBACK_HOURS,
        "dry_run": DRY_RUN,
        "selected": selected,
    }
    append_selected_only_record(SELECTED_ONLY_ARCHIVE_PATH, selected_only_record)
    logger.success(f"Updated selected-only archive: {SELECTED_ONLY_ARCHIVE_PATH}")

    # Post (unless dry-run)
    if DRY_RUN:
        logger.info("DRY_RUN=1 -> skipping Discord post.")
    else:
        content = format_discord_message(selected.get("author_id", ""), selected.get("text", ""))
        post_res = post_quote_to_discord(content)
        logger.success("Posted quote to Discord.")
        state["last_post_result"] = post_res

    # Save state
    state["last_run_utc"] = now.isoformat()
    state["last_since_utc"] = since_dt.isoformat()
    state["dry_run"] = DRY_RUN
    state["lang"] = QUOTE_LANG
    save_json(STATE_PATH, state)

if __name__ == "__main__":
    main()