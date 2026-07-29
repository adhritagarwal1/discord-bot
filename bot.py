# -------------------------------------------------------------------
# TradeSight AI -- Discord chart-logging & edge-finding bot
#
# SaaS model: single home server, gated by a Whop subscription. Whop's native
# Discord role sync grants PREMIUM_ROLE_ID in HOME_GUILD_ID on purchase; this bot
# just checks for that role. Customers interact with the bot over DM, so every
# premium check resolves membership against HOME_GUILD_ID directly rather than
# relying on interaction.guild (which is None in a DM).
#
# Requirements: discord.py, flask, aiosqlite, google-genai, python-dotenv, Pillow
# Optional (only needed if DATABASE_URL is set): asyncpg
# -------------------------------------------------------------------
import os
import io
import re
import json
import time
import asyncio
import logging
import threading
from datetime import timezone, datetime, timedelta
from dotenv import load_dotenv
from flask import Flask
import discord
from discord import app_commands
from discord.ext import commands
from PIL import Image
import aiosqlite
from google import genai
from google.genai import types

load_dotenv()  # Loads .env locally; ignored gracefully on Render

# -------------------------------------------------------------------
# LOGGING & CONFIGURATION
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")  # Optional: Postgres URL for persistent storage
WHOP_CHECKOUT_URL = os.getenv("WHOP_CHECKOUT_URL", "")  # Optional: shown in the upsell message

if not DISCORD_TOKEN or not GEMINI_API_KEY:
    raise ValueError("Missing critical environment variables (DISCORD_TOKEN or GEMINI_API_KEY).")

# HOME_GUILD_ID / PREMIUM_ROLE_ID are required now that every feature is gated by a Whop-synced
# Discord role. Since customers talk to the bot over DM, we can't rely on interaction.guild to
# find their roles -- we always look them up in this specific guild.
try:
    HOME_GUILD_ID = int(os.getenv("HOME_GUILD_ID", ""))
    PREMIUM_ROLE_ID = int(os.getenv("PREMIUM_ROLE_ID", ""))
except ValueError:
    raise ValueError(
        "HOME_GUILD_ID and PREMIUM_ROLE_ID must both be set to valid Discord IDs (integers). "
        "HOME_GUILD_ID is your server's ID; PREMIUM_ROLE_ID is the role Whop assigns on purchase."
    )

genai_client = genai.Client(api_key=GEMINI_API_KEY)

GEMINI_MODEL = "gemini-3.6-flash"
GEMINI_MAX_RETRIES = 2  # total attempts = this + 1, with exponential backoff

CHART_COOLDOWN_SECONDS = 15       # per-user cooldown between chart analyses
FINDMYEDGE_COOLDOWN_SECONDS = 60  # per-user cooldown on /findmyedge
MAX_IMAGE_BYTES = 8 * 1024 * 1024  # 8 MB -- protects RAM on constrained hosts
MAX_STRATEGY_LENGTH = 500

MIN_TRADES_FOR_EDGE = 3
EDGE_LOOKBACK = 10  # /findmyedge only ever looks at the last 10 completed trades

SQLITE_DB_PATH = "tradesight.db"

# Per-user cooldown tracking for chart analysis (in-memory, resets on restart -- fine for this use)
_last_chart_analysis_at = {}

# Premium-membership lookups hit the Discord API (guild.fetch_member), so cache briefly to
# avoid hammering it every time a customer sends a chart in DM.
PREMIUM_CACHE_TTL_SECONDS = 60
_premium_cache = {}  # user_id -> (is_premium: bool, checked_at: float)

# -------------------------------------------------------------------
# RENDER KEEP-ALIVE SERVER (FLASK)
# -------------------------------------------------------------------
web_app = Flask(__name__)


@web_app.route('/')
def health_check():
    return "Bot is live and running 24/7!", 200


def run_flask():
    port = int(os.getenv("PORT", 8080))
    web_app.run(host="0.0.0.0", port=port)


threading.Thread(target=run_flask, daemon=True).start()

# -------------------------------------------------------------------
# DISCORD BOT INTENTS
# -------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
# "members" is a privileged intent -- must also be enabled for this bot
# in the Discord Developer Portal, or the /findmyedge premium check will fail silently.
intents.members = True

# -------------------------------------------------------------------
# DATABASE LAYER
# Supports SQLite (default, zero-config) or Postgres (set DATABASE_URL) so trade
# history survives redeploys/restarts. Render's free/starter disks are ephemeral --
# a SQLite file there gets wiped on every redeploy. Point DATABASE_URL at a Postgres
# instance (Render offers a free tier) for real persistence.
# -------------------------------------------------------------------
SQLITE_STRATEGIES_TABLE = """
CREATE TABLE IF NOT EXISTS strategies (
    user_id INTEGER PRIMARY KEY,
    prompt TEXT NOT NULL
)
"""
SQLITE_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    message_id INTEGER UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    result TEXT,
    direction TEXT,
    entry TEXT,
    stop_loss TEXT,
    take_profit TEXT,
    matches_strategy INTEGER,
    note TEXT,
    session TEXT,
    risk_reward REAL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

PG_STRATEGIES_TABLE = """
CREATE TABLE IF NOT EXISTS strategies (
    user_id BIGINT PRIMARY KEY,
    prompt TEXT NOT NULL
)
"""
PG_TRADES_TABLE = """
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    message_id BIGINT UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    result TEXT,
    direction TEXT,
    entry TEXT,
    stop_loss TEXT,
    take_profit TEXT,
    matches_strategy INTEGER,
    note TEXT,
    session TEXT,
    risk_reward REAL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


class Database:
    """Thin async wrapper that dispatches to Postgres (asyncpg) if DATABASE_URL is
    set, otherwise falls back to local SQLite. Queries are always written with
    '?' placeholders; they're translated to '$1, $2, ...' for Postgres internally,
    so calling code never needs to know which backend is active.
    """

    def __init__(self):
        self.use_postgres = bool(DATABASE_URL)
        self.pool = None
        if not self.use_postgres:
            logging.warning(
                "DATABASE_URL not set -- using local SQLite (%s). On Render this file "
                "is wiped on every redeploy/restart. Set DATABASE_URL to a Postgres "
                "instance for persistent trade history.", SQLITE_DB_PATH
            )

    async def init(self):
        if self.use_postgres:
            import asyncpg  # lazy import -- only required when Postgres is actually used
            self.pool = await asyncpg.create_pool(DATABASE_URL, ssl="require")
            async with self.pool.acquire() as conn:
                await conn.execute(PG_STRATEGIES_TABLE)
                await conn.execute(PG_TRADES_TABLE)
        else:
            async with aiosqlite.connect(SQLITE_DB_PATH) as conn:
                await conn.execute(SQLITE_STRATEGIES_TABLE)
                await conn.execute(SQLITE_TRADES_TABLE)
                # Best-effort migration for bots upgraded from the pre-session/R:R schema
                for col, coltype in (("session", "TEXT"), ("risk_reward", "REAL")):
                    try:
                        await conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {coltype}")
                    except Exception:
                        pass  # column already exists
                await conn.commit()
        logging.info(f"Database ready ({'Postgres' if self.use_postgres else 'SQLite'}).")

    @staticmethod
    def _to_pg(query: str) -> str:
        if "?" not in query:
            return query
        parts = query.split("?")
        out = parts[0]
        for i, part in enumerate(parts[1:], start=1):
            out += f"${i}" + part
        return out

    async def execute(self, query: str, params: tuple = ()):
        if self.use_postgres:
            async with self.pool.acquire() as conn:
                await conn.execute(self._to_pg(query), *params)
        else:
            async with aiosqlite.connect(SQLITE_DB_PATH) as conn:
                await conn.execute(query, params)
                await conn.commit()

    async def fetchone(self, query: str, params: tuple = ()):
        if self.use_postgres:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(self._to_pg(query), *params)
                return tuple(row) if row else None
        else:
            async with aiosqlite.connect(SQLITE_DB_PATH) as conn:
                async with conn.execute(query, params) as cursor:
                    return await cursor.fetchone()

    async def fetchall(self, query: str, params: tuple = ()):
        if self.use_postgres:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(self._to_pg(query), *params)
                return [tuple(r) for r in rows]
        else:
            async with aiosqlite.connect(SQLITE_DB_PATH) as conn:
                async with conn.execute(query, params) as cursor:
                    return await cursor.fetchall()


db = Database()

# -------------------------------------------------------------------
# PREMIUM / SUBSCRIPTION GATING (Whop -> Discord role sync)
# -------------------------------------------------------------------
async def is_premium_member(user_id: int) -> bool:
    """Checks whether user_id currently holds PREMIUM_ROLE_ID in HOME_GUILD_ID.
    Whop's native Discord integration adds/removes that role automatically as
    subscriptions start/lapse, so this is the single source of truth -- no
    separate billing webhook needed.
    """
    now = time.time()
    cached = _premium_cache.get(user_id)
    if cached and now - cached[1] < PREMIUM_CACHE_TTL_SECONDS:
        return cached[0]

    guild = bot.get_guild(HOME_GUILD_ID)
    if guild is None:
        logging.error("Bot cannot see HOME_GUILD_ID -- is it actually a member of that server?")
        return False

    member = guild.get_member(user_id)
    if member is None:
        try:
            member = await guild.fetch_member(user_id)
        except discord.NotFound:
            member = None
        except Exception as e:
            logging.error(f"Failed to fetch member {user_id} from home guild: {e}")
            member = None

    result = bool(member and any(r.id == PREMIUM_ROLE_ID for r in member.roles))
    _premium_cache[user_id] = (result, now)
    return result


def build_upgrade_message() -> str:
    base = "🔒 **This is a premium feature.** Subscribe to unlock TradeSight AI"
    return f"{base}: {WHOP_CHECKOUT_URL}" if WHOP_CHECKOUT_URL else f"{base}."


# -------------------------------------------------------------------
# HELPERS -- SESSION TAGGING, RISK:REWARD & TILT WARNINGS
# -------------------------------------------------------------------
def get_trading_session(dt_utc) -> str:
    """Tags a trade by UTC hour rather than asking the vision model to guess it
    from the screenshot -- chart images rarely show a reliable timestamp, but the
    Discord message timestamp always is one.
    """
    hour = dt_utc.hour
    sessions = []
    if 0 <= hour < 9:
        sessions.append("Asia")
    if 7 <= hour < 16:
        sessions.append("London")
    if 12 <= hour < 21:
        sessions.append("New York")
    return "/".join(sessions) if sessions else "Off-hours"


def try_parse_price(text) -> float | None:
    if not text or not isinstance(text, str):
        return None
    if text.strip().lower() in ("unclear", "n/a", "-", ""):
        return None
    cleaned = re.sub(r"[^\d.\-]", "", text)  # drop currency symbols, commas, etc.
    try:
        return float(cleaned)
    except ValueError:
        return None


def compute_risk_reward(direction: str, entry, stop_loss, take_profit) -> float | None:
    e, s, t = try_parse_price(entry), try_parse_price(stop_loss), try_parse_price(take_profit)
    if e is None or s is None or t is None:
        return None
    if direction == "Long":
        risk, reward = e - s, t - e
    elif direction == "Short":
        risk, reward = s - e, e - t
    else:
        return None
    if risk <= 0:
        return None
    return round(reward / risk, 2)


def check_user_tilt_status(user_trades: list) -> dict:
    """
    Analyzes recent completed trade history for a specific user to detect tilt triggers.
    `user_trades` should be a list of trade tuples or dicts.
    """
    today = datetime.utcnow().date()
    
    # Filter trades for today based on timestamp field
    todays_trades = []
    for t in user_trades:
        # t format from completed queries: (result, direction, entry, stop_loss, take_profit, note, session, risk_reward, timestamp)
        # Handle cases where timestamp might be at the end
        ts = t[8] if len(t) > 8 and isinstance(t[8], datetime) else datetime.utcnow()
        if ts.date() == today:
            todays_trades.append({"outcome": t[0], "r_multiple": -1.0 if t[0] == "LOSS" else (t[7] if t[0] == "WIN" and t[7] else 0.0), "timestamp": ts})

    total_daily_r = sum(t['r_multiple'] for t in todays_trades)
    
    consecutive_losses = 0
    for t in reversed(todays_trades):
        if t['outcome'] == 'LOSS':
            consecutive_losses += 1
        else:
            break
            
    thirty_mins_ago = datetime.utcnow() - timedelta(minutes=30)
    recent_trade_count = sum(1 for t in todays_trades if t['timestamp'] >= thirty_mins_ago)
    
    return {
        "consecutive_losses": consecutive_losses,
        "total_daily_r": total_daily_r,
        "recent_trade_count": recent_trade_count,
        "todays_trade_count": len(todays_trades)
    }


def generate_tilt_warning_embed(user_mention: str, tilt_data: dict) -> discord.Embed | None:
    consecutive_losses = tilt_data["consecutive_losses"]
    total_daily_r = tilt_data["total_daily_r"]
    recent_count = tilt_data["recent_trade_count"]
    
    triggers = []
    if consecutive_losses >= 3:
        triggers.append(f"• **{consecutive_losses} Consecutive Losses:** High risk of revenge trading.")
    if total_daily_r <= -3.0:
        triggers.append(f"• **Daily Loss Limit Reached:** Current daily total is `{total_daily_r:+.1f}R`.")
    if recent_count >= 3:
        triggers.append(f"• **Rapid Trade Frequency:** {recent_count} trades logged in the last 30 minutes.")

    if not triggers:
        return None

    embed = discord.Embed(
        title="⚠️ TILT WARNING: RISK PARAMETERS EXCEEDED",
        description=(
            f"Hey {user_mention}, your recent trade log triggered risk protection rules:\n\n"
            + "\n".join(triggers)
            + "\n\n**Recommended Action:** Step away from the charts for 30–60 minutes. Close your trading platform and reassess during the next session."
        ),
        color=discord.Color.red()
    )
    embed.set_footer(text="Protect your capital first. Market edge only works with disciplined execution.")
    return embed


def _truncate(text: str, limit: int = 1000) -> str:
    text = text or "-"
    return text if len(text) <= limit else text[: limit - 3] + "..."


# -------------------------------------------------------------------
# GEMINI HELPERS
# -------------------------------------------------------------------
async def call_gemini(contents, config=None):
    """Wraps generate_content with a couple of retries and exponential backoff,
    so a transient Gemini error/rate-limit doesn't just fail the whole request.
    """
    last_err = None
    for attempt in range(GEMINI_MAX_RETRIES + 1):
        try:
            return await genai_client.aio.models.generate_content(
                model=GEMINI_MODEL, contents=contents, config=config
            )
        except Exception as e:
            last_err = e
            logging.warning(f"Gemini call failed (attempt {attempt + 1}/{GEMINI_MAX_RETRIES + 1}): {e}")
            if attempt < GEMINI_MAX_RETRIES:
                await asyncio.sleep(2 ** attempt)
    raise last_err


def build_chart_prompt(user_strategy: str) -> str:
    """Minimal, structured prompt. No narrative price-action essays."""
    return (
        "You are a trade-logging assistant. You are NOT a signal provider and must NEVER "
        "recommend a future trade. This screenshot shows a trade the user has already taken "
        "or planned -- only describe what is already visible on the chart.\n\n"
        f"User's strategy rules: {user_strategy}\n\n"
        "Return ONLY valid JSON with exactly these keys, no other text:\n"
        '{"direction": "Long" | "Short" | "Unclear", '
        '"entry": "<price as text, or Unclear>", '
        '"stop_loss": "<price as text, or Unclear>", '
        '"take_profit": "<price as text, or Unclear>", '
        '"matches_strategy": true | false, '
        '"note": "<10 words max on whether it matches the strategy>"}'
    )


def parse_trade_json(raw_text: str) -> dict:
    """Safely parse Gemini's JSON reply, with a graceful fallback."""
    fallback = {
        "direction": "Unclear",
        "entry": "Unclear",
        "stop_loss": "Unclear",
        "take_profit": "Unclear",
        "matches_strategy": None,
        "note": "Could not read this chart.",
    }
    if not raw_text:
        return fallback

    cleaned = raw_text.strip().strip("`")
    if cleaned.lower().startswith("json"):
        cleaned = cleaned[4:].strip()

    try:
        data = json.loads(cleaned)
        matches = data.get("matches_strategy")
        return {
            "direction": data.get("direction") or "Unclear",
            "entry": data.get("entry") or "Unclear",
            "stop_loss": data.get("stop_loss") or "Unclear",
            "take_profit": data.get("take_profit") or "Unclear",
            "matches_strategy": bool(matches) if matches is not None else None,
            "note": str(data.get("note") or "")[:150],
        }
    except (json.JSONDecodeError, AttributeError):
        logging.warning(f"Could not parse Gemini JSON: {raw_text[:200]}")
        fallback["note"] = raw_text[:100]
        return fallback


def parse_edge_sections(raw_text: str) -> dict:
    """Parses the CORE_EDGE / PRIMARY_LEAK / ACTION_PLAN labeled response into a dict."""
    sections = {"core_edge": "", "primary_leak": "", "action_plan": ""}
    patterns = {
        "core_edge": r"CORE_EDGE:\s*(.*?)(?=PRIMARY_LEAK:|ACTION_PLAN:|$)",
        "primary_leak": r"PRIMARY_LEAK:\s*(.*?)(?=ACTION_PLAN:|$)",
        "action_plan": r"ACTION_PLAN:\s*(.*)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, raw_text or "", re.DOTALL | re.IGNORECASE)
        if match:
            sections[key] = match.group(1).strip()

    if not any(sections.values()):
        sections["core_edge"] = (raw_text or "").strip()[:1000] or "No analysis returned."

    for key in sections:
        if not sections[key]:
            sections[key] = "Not enough data yet."

    return sections


def compute_stats(trades: list) -> dict:
    """trades: list of (result, direction, entry, stop_loss, take_profit, note, session, risk_reward)"""
    total = len(trades)
    wins = [t for t in trades if t[0] == "WIN"]
    losses = [t for t in trades if t[0] == "LOSS"]
    win_rate = round(len(wins) / total * 100, 1) if total else 0.0

    def breakdown(index):
        counts = {}
        for t in trades:
            key = t[index] or "Unclear"
            counts.setdefault(key, {"wins": 0, "total": 0})
            counts[key]["total"] += 1
            if t[0] == "WIN":
                counts[key]["wins"] += 1
        return {
            k: round(v["wins"] / v["total"] * 100, 1)
            for k, v in counts.items()
            if v["total"] >= 2 and k != "Unclear"
        }

    rr_win = [t[7] for t in wins if t[7] is not None]
    rr_loss = [t[7] for t in losses if t[7] is not None]

    return {
        "total": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": win_rate,
        "direction_breakdown": breakdown(1),
        "session_breakdown": breakdown(6),
        "avg_rr_win": round(sum(rr_win) / len(rr_win), 2) if rr_win else None,
        "avg_rr_loss": round(sum(rr_loss) / len(rr_loss), 2) if rr_loss else None,
    }


# -------------------------------------------------------------------
# EMBED BUILDERS
# -------------------------------------------------------------------
def build_trade_embed(user, t: dict, session: str, risk_reward, result: str = None) -> discord.Embed:
    if result == "WIN":
        color = discord.Color.green()
    elif result == "LOSS":
        color = discord.Color.red()
    elif result == "BREAKEVEN":
        color = discord.Color.light_grey()
    else:
        color = discord.Color.blurple()

    embed = discord.Embed(title="📊 Trade Logged", color=color)
    avatar_url = user.display_avatar.url if getattr(user, "display_avatar", None) else None
    embed.set_author(name=getattr(user, "display_name", str(user)), icon_url=avatar_url)

    embed.add_field(name="Direction", value=t["direction"], inline=True)
    embed.add_field(name="Session", value=session or "Unclear", inline=True)
    embed.add_field(name="R:R", value=f"1:{risk_reward}" if risk_reward else "—", inline=True)
    embed.add_field(name="Entry", value=t["entry"], inline=True)
    embed.add_field(name="Stop Loss", value=t["stop_loss"], inline=True)
    embed.add_field(name="Take Profit", value=t["take_profit"], inline=True)

    if t["matches_strategy"] is True:
        match_icon = "✅"
    elif t["matches_strategy"] is False:
        match_icon = "❌"
    else:
        match_icon = "❔"
    embed.add_field(name="Matches Strategy", value=_truncate(f"{match_icon} {t['note']}"), inline=False)

    if result:
        if result == 'WIN':
            res_emoji = '🟢'
        elif result == 'LOSS':
            res_emoji = '🔴'
        else:
            res_emoji = '⚪'
        embed.add_field(name="Result", value=f"{res_emoji} {result}", inline=False)
    else:
        embed.set_footer(text="Tap a button below to log the result")

    return embed


def build_edge_embed(user, stats: dict, sections: dict, trades_count: int) -> discord.Embed:
    embed = discord.Embed(title="🧠 Trading Edge Audit", color=discord.Color.gold())
    avatar_url = user.display_avatar.url if getattr(user, "display_avatar", None) else None
    embed.set_author(name=getattr(user, "display_name", str(user)), icon_url=avatar_url)

    embed.add_field(
        name="Win Rate", value=f"{stats['win_rate']}% ({stats['wins']}W / {stats['losses']}L)", inline=True
    )
    if stats["avg_rr_win"] is not None:
        embed.add_field(name="Avg R:R (Wins)", value=f"1:{stats['avg_rr_win']}", inline=True)
    if stats["avg_rr_loss"] is not None:
        embed.add_field(name="Avg R:R (Losses)", value=f"1:{stats['avg_rr_loss']}", inline=True)

    embed.add_field(name="🎯 Core Edge", value=_truncate(sections.get("core_edge")), inline=False)
    embed.add_field(name="🕳️ Primary Leak", value=_truncate(sections.get("primary_leak")), inline=False)
    embed.add_field(name="🛠️ Action Plan", value=_truncate(sections.get("action_plan")), inline=False)

    embed.set_footer(text=f"Based on your last {trades_count} completed trades")
    return embed


# -------------------------------------------------------------------
# PERSISTENT UI -- WIN/LOSS/BREAKEVEN BUTTONS (replaces emoji reactions)
# -------------------------------------------------------------------
class TradeResultView(discord.ui.View):
    """Buttons instead of reactions: clearer affordance, disables the ambiguity of
    reaction removal, and survives bot restarts since it's registered as a
    persistent view with static custom_ids.
    """

    def __init__(self):
        super().__init__(timeout=None)

    async def _handle(self, interaction: discord.Interaction, result_type: str):
        trade = await db.fetchone(
            "SELECT user_id, status, result, direction, entry, stop_loss, take_profit, "
            "matches_strategy, note, session, risk_reward FROM trades WHERE message_id = ?",
            (interaction.message.id,),
        )
        if not trade:
            await interaction.response.send_message(
                "⚠️ I couldn't find this trade in the database.", ephemeral=True
            )
            return

        (user_id, status, current_result, direction, entry, stop_loss,
         take_profit, matches_strategy, note, session, risk_reward) = trade

        if interaction.user.id != user_id:
            await interaction.response.send_message(
                "Only the trader who posted this chart can log its result.", ephemeral=True
            )
            return

        if status == "COMPLETED" and current_result == result_type:
            await interaction.response.send_message(f"Already logged as {result_type}.", ephemeral=True)
            return

        await db.execute(
            "UPDATE trades SET status = 'COMPLETED', result = ? WHERE message_id = ?",
            (result_type, interaction.message.id),
        )

        t = {
            "direction": direction,
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "matches_strategy": bool(matches_strategy) if matches_strategy is not None else None,
            "note": note,
        }
        embed = build_trade_embed(interaction.user, t, session or "Unclear", risk_reward, result=result_type)
        await interaction.response.edit_message(embed=embed, view=self)

        # Check for tilt warnings after updating trade result
        try:
            user_trades = await db.fetchall(
                "SELECT result, direction, entry, stop_loss, take_profit, note, session, risk_reward, timestamp "
                "FROM trades WHERE user_id = ? AND status = 'COMPLETED' ORDER BY id ASC",
                (user_id,)
            )
            tilt_status = check_user_tilt_status(user_trades)
            warning_embed = generate_tilt_warning_embed(interaction.user.mention, tilt_status)
            if warning_embed:
                await interaction.followup.send(content=f"{interaction.user.mention}", embed=warning_embed)
        except Exception as e:
            logging.error(f"Error checking tilt status: {e}")

    @discord.ui.button(label="Win", style=discord.ButtonStyle.success, custom_id="tradesight:win", emoji="🟢")
    async def win_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle(interaction, "WIN")

    @discord.ui.button(label="Loss", style=discord.ButtonStyle.danger, custom_id="tradesight:loss", emoji="🔴")
    async def loss_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle(interaction, "LOSS")

    @discord.ui.button(label="Breakeven", style=discord.ButtonStyle.secondary, custom_id="tradesight:breakeven", emoji="⚪")
    async def breakeven_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle(interaction, "BREAKEVEN")


# -------------------------------------------------------------------
# BOT
# -------------------------------------------------------------------
class TradeSightBot(commands.Bot):
    async def setup_hook(self):
        await db.init()
        self.add_view(TradeResultView())  # register persistent view once, before login


bot = TradeSightBot(command_prefix="!", intents=intents)


@bot.event
async def on_ready():
    try:
        synced = await bot.tree.sync()
        logging.info(f"Synced {len(synced)} slash command(s).")
    except Exception as e:
        logging.error(f"Failed to sync slash commands: {e}")

    if bot.get_guild(HOME_GUILD_ID) is None:
        logging.error(
            f"Bot is not in HOME_GUILD_ID ({HOME_GUILD_ID}) -- every premium check will fail "
            "until it's invited there."
        )

    logging.info(f"Bot logged in as {bot.user} (ID: {bot.user.id})")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    image_attachments = [
        att for att in message.attachments
        if att.content_type and att.content_type.startswith("image/")
    ]

    if not image_attachments:
        await bot.process_commands(message)
        return

    if not await is_premium_member(message.author.id):
        await message.channel.send(build_upgrade_message())
        await bot.process_commands(message)
        return

    now = time.time()
    last_call = _last_chart_analysis_at.get(message.author.id, 0)
    if now - last_call < CHART_COOLDOWN_SECONDS:
        await bot.process_commands(message)
        return
    _last_chart_analysis_at[message.author.id] = now

    attachment = image_attachments[0]

    if attachment.size and attachment.size > MAX_IMAGE_BYTES:
        await message.channel.send(
            f"⚠️ That image is too large to analyze (max {MAX_IMAGE_BYTES // (1024 * 1024)}MB)."
        )
        await bot.process_commands(message)
        return

    processing_msg = await message.channel.send("🔍 **Reading chart...**")

    try:
        image_bytes = await attachment.read()

        strategy_row = await db.fetchone(
            "SELECT prompt FROM strategies WHERE user_id = ?", (message.author.id,)
        )
        user_strategy = strategy_row[0] if strategy_row else (
            "No custom strategy set -- just identify direction, entry, stop-loss "
            "and take-profit levels visible on the chart."
        )

        with Image.open(io.BytesIO(image_bytes)) as img:
            full_prompt = build_chart_prompt(user_strategy)
            response = await call_gemini(
                [img, full_prompt],
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )

        trade_data = parse_trade_json(response.text)
        del image_bytes

        session = get_trading_session(message.created_at.astimezone(timezone.utc))
        risk_reward = compute_risk_reward(
            trade_data["direction"], trade_data["entry"], trade_data["stop_loss"], trade_data["take_profit"]
        )

        embed = build_trade_embed(message.author, trade_data, session, risk_reward)
        analysis_msg = await message.channel.send(embed=embed, view=TradeResultView())
        await processing_msg.delete()

        matches_int = (
            int(trade_data["matches_strategy"]) if trade_data["matches_strategy"] is not None else None
        )

        await db.execute(
            "INSERT INTO trades "
            "(user_id, message_id, status, direction, entry, stop_loss, take_profit, "
            "matches_strategy, note, session, risk_reward) "
            "VALUES (?, ?, 'PENDING', ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                message.author.id,
                analysis_msg.id,
                trade_data["direction"],
                trade_data["entry"],
                trade_data["stop_loss"],
                trade_data["take_profit"],
                matches_int,
                trade_data["note"],
                session,
                risk_reward,
            ),
        )

    except Exception as e:
        logging.error(f"Error during chart analysis: {e}")
        await processing_msg.edit(content="❌ Couldn't analyze that chart right now. Please try again shortly.")

    await bot.process_commands(message)


# -------------------------------------------------------------------
# SLASH COMMANDS
# -------------------------------------------------------------------
@bot.tree.command(name="setstrategy", description="Set your custom trading strategy for chart analysis.")
@app_commands.describe(strategy="Describe your trading rules, indicators, entry triggers, and risk model.")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def set_strategy(interaction: discord.Interaction, strategy: str):
    await interaction.response.defer(ephemeral=True)

    if not await is_premium_member(interaction.user.id):
        await interaction.followup.send(build_upgrade_message(), ephemeral=True)
        return

    if len(strategy) > MAX_STRATEGY_LENGTH:
        await interaction.followup.send(
            f"⚠️ Your strategy is {len(strategy)} characters -- please keep it under "
            f"{MAX_STRATEGY_LENGTH} so it doesn't bloat every chart analysis.",
            ephemeral=True,
        )
        return

    await db.execute(
        "INSERT INTO strategies (user_id, prompt) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET prompt = excluded.prompt",
        (interaction.user.id, strategy),
    )

    embed = discord.Embed(title="✅ Strategy Updated", description=strategy, color=discord.Color.green())
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="findmyedge", description="Find patterns in your last 10 logged trades.")
@app_commands.checks.cooldown(1, FINDMYEDGE_COOLDOWN_SECONDS, key=lambda i: i.user.id)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def find_my_edge(interaction: discord.Interaction):
    await interaction.response.defer()

    if not await is_premium_member(interaction.user.id):
        await interaction.followup.send(build_upgrade_message())
        return

    trades = await db.fetchall(
        "SELECT result, direction, entry, stop_loss, take_profit, note, session, risk_reward "
        "FROM trades WHERE user_id = ? AND status = 'COMPLETED' ORDER BY id DESC LIMIT ?",
        (interaction.user.id, EDGE_LOOKBACK),
    )

    if len(trades) < MIN_TRADES_FOR_EDGE:
        await interaction.followup.send(
            f"⚠️ You need at least **{MIN_TRADES_FOR_EDGE} completed trades** (out of your last "
            f"{EDGE_LOOKBACK}) to run an edge analysis. Currently logged: `{len(trades)}`."
        )
        return

    stats = compute_stats(trades)

    lines = []
    for idx, (result, direction, entry, sl, tp, note, session, rr) in enumerate(reversed(trades), 1):
        rr_text = f"1:{rr}" if rr is not None else "-"
        lines.append(
            f"#{idx} [{result}] {direction or 'Unclear'} | Entry {entry or '-'} | SL {sl or '-'} | "
            f"TP {tp or '-'} | R:R {rr_text} | {session or '-'} | {note or ''}"
        )
    trade_block = "\n".join(lines)

    stats_lines = [f"Overall win rate: {stats['win_rate']}% ({stats['wins']}W / {stats['losses']}L)"]
    if stats["direction_breakdown"]:
        stats_lines.append(
            "Win rate by direction: " + ", ".join(f"{k} {v}%" for k, v in stats["direction_breakdown"].items())
        )
    if stats["session_breakdown"]:
        stats_lines.append(
            "Win rate by session: " + ", ".join(f"{k} {v}%" for k, v in stats["session_breakdown"].items())
        )
    if stats["avg_rr_win"] is not None:
        stats_lines.append(f"Average R:R on wins: 1:{stats['avg_rr_win']}")
    if stats["avg_rr_loss"] is not None:
        stats_lines.append(f"Average R:R on losses: 1:{stats['avg_rr_loss']}")
    stats_block = "\n".join(stats_lines)

    prompt = (
        "You are a trading performance analyst. Based ONLY on the data below (this trader's last "
        f"{len(trades)} completed trades), find their edge.\n\n"
        f"STATS:\n{stats_block}\n\n"
        f"TRADES (most recent first):\n{trade_block}\n\n"
        "Respond in exactly this format, with no extra commentary before or after:\n"
        "CORE_EDGE: <2-3 sentences on the common condition behind the winning trades>\n"
        "PRIMARY_LEAK: <2-3 sentences on the common condition behind the losing trades>\n"
        "ACTION_PLAN: <2 specific, concrete rules to improve win rate, separated by ' | '>\n"
        "Do not suggest any new trades."
    )

    try:
        response = await call_gemini(prompt)
        sections = parse_edge_sections(response.text or "")
        embed = build_edge_embed(interaction.user, stats, sections, len(trades))
        await interaction.followup.send(embed=embed)
    except Exception as e:
        logging.error(f"Error generating edge audit: {e}")
        await interaction.followup.send("❌ Couldn't generate your edge audit right now. Please try again shortly.")


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        message = f"⏳ Slow down -- you can run this again in {error.retry_after:.0f}s."
    else:
        logging.error(f"Unhandled app command error: {error}")
        message = "❌ Something went wrong running that command."

    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except Exception:
        pass


# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
