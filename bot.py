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
GEMINI_MAX_RETRIES = 2

CHART_COOLDOWN_SECONDS = 15
FINDMYEDGE_COOLDOWN_SECONDS = 60
MAX_IMAGE_BYTES = 8 * 1024 * 1024  # 8 MB
MAX_STRATEGY_LENGTH = 500

MIN_TRADES_FOR_EDGE = 3
EDGE_LOOKBACK = 10

SQLITE_DB_PATH = "tradesight.db"

_last_chart_analysis_at = {}
PREMIUM_CACHE_TTL_SECONDS = 60
_premium_cache = {}

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
intents.members = True

# -------------------------------------------------------------------
# DATABASE LAYER
# -------------------------------------------------------------------
SQLITE_STRATEGIES_TABLE = """
CREATE TABLE IF NOT EXISTS strategies (
    user_id INTEGER PRIMARY KEY,
    prompt TEXT NOT NULL
)
"""
SQLITE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    chart_timezone TEXT DEFAULT 'UTC'
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
PG_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    chart_timezone TEXT DEFAULT 'UTC'
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
    def __init__(self):
        self.use_postgres = bool(DATABASE_URL)
        self.pool = None
        self.sqlite_conn = None
        if not self.use_postgres:
            logging.warning(
                "DATABASE_URL not set -- using local SQLite (%s). On Render this file "
                "is wiped on every redeploy/restart. Set DATABASE_URL to a Postgres "
                "instance for persistent trade history.", SQLITE_DB_PATH
            )

    async def init(self):
        if self.use_postgres:
            import asyncpg
            self.pool = await asyncpg.create_pool(DATABASE_URL, ssl="require")
            async with self.pool.acquire() as conn:
                await conn.execute(PG_STRATEGIES_TABLE)
                await conn.execute(PG_USERS_TABLE)
                await conn.execute(PG_TRADES_TABLE)
        else:
            self.sqlite_conn = await aiosqlite.connect(SQLITE_DB_PATH)
            await self.sqlite_conn.execute(SQLITE_STRATEGIES_TABLE)
            await self.sqlite_conn.execute(SQLITE_USERS_TABLE)
            await self.sqlite_conn.execute(SQLITE_TRADES_TABLE)
            for col, coltype in (("session", "TEXT"), ("risk_reward", "REAL")):
                try:
                    await self.sqlite_conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {coltype}")
                except Exception:
                    pass
            await self.sqlite_conn.commit()
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
            await self.sqlite_conn.execute(query, params)
            await self.sqlite_conn.commit()

    async def fetchone(self, query: str, params: tuple = ()):
        if self.use_postgres:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(self._to_pg(query), *params)
                return tuple(row) if row else None
        else:
            async with self.sqlite_conn.execute(query, params) as cursor:
                return await cursor.fetchone()

    async def fetchall(self, query: str, params: tuple = ()):
        if self.use_postgres:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(self._to_pg(query), *params)
                return [tuple(r) for r in rows]
        else:
            async with self.sqlite_conn.execute(query, params) as cursor:
                return await cursor.fetchall()


db = Database()

# -------------------------------------------------------------------
# PREMIUM / SUBSCRIPTION GATING
# -------------------------------------------------------------------
async def is_premium_member(user_id: int) -> bool:
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


def build_upgrade_message() -> discord.Embed:
    embed = discord.Embed(
        title="🔒 Premium Feature",
        description="Subscribe to **TradeSight AI** to unlock full chart analysis, automated journaling, and edge audits.",
        color=discord.Color.dark_theme()
    )
    if WHOP_CHECKOUT_URL:
        embed.add_field(name="Ready to Upgrade?", value=f"[Click here to get your Access Pass]({WHOP_CHECKOUT_URL})")
    return embed


# -------------------------------------------------------------------
# HELPERS -- PROGRESS BARS, SESSION TAGGING, RISK:REWARD & TILT WARNINGS
# -------------------------------------------------------------------
def generate_progress_bar(wins: int, total: int, length: int = 10) -> str:
    if total <= 0:
        return "⬜" * length
    win_count = max(0, min(length, round((wins / total) * length)))
    loss_count = length - win_count
    return "🟩" * win_count + "🟥" * loss_count


def get_trading_session(dt_utc: datetime) -> str:
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
    cleaned = re.sub(r"[^\d.\-]", "", text)
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
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    
    todays_trades = []
    for t in user_trades:
        raw_ts = t[8] if len(t) > 8 else None
        if isinstance(raw_ts, datetime):
            ts = raw_ts if raw_ts.tzinfo else raw_ts.replace(tzinfo=timezone.utc)
        elif isinstance(raw_ts, str):
            try:
                ts = datetime.fromisoformat(raw_ts.replace(" ", "T"))
                if not ts.tzinfo:
                    ts = ts.replace(tzinfo=timezone.utc)
            except ValueError:
                ts = now_utc
        else:
            ts = now_utc

        if ts.date() == today:
            r_multiple = 0.0
            if t[0] == "LOSS":
                r_multiple = -1.0
            elif t[0] == "WIN" and t[7] is not None:
                r_multiple = float(t[7])
                
            todays_trades.append({
                "outcome": t[0], 
                "r_multiple": r_multiple, 
                "timestamp": ts
            })

    total_daily_r = sum(t['r_multiple'] for t in todays_trades)
    
    consecutive_losses = 0
    for t in reversed(todays_trades):
        if t['outcome'] == 'LOSS':
            consecutive_losses += 1
        else:
            break
            
    thirty_mins_ago = now_utc - timedelta(minutes=30)
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
        triggers.append(f"> 📉 **{consecutive_losses} Consecutive Losses:** High risk of revenge trading.")
    if total_daily_r <= -3.0:
        triggers.append(f"> 🛑 **Daily Loss Limit Reached:** Current daily total is `{total_daily_r:+.1f}R`.")
    if recent_count >= 3:
        triggers.append(f"> ⚡ **Rapid Trade Frequency:** `{recent_count}` trades logged in the last 30 minutes.")

    if not triggers:
        return None

    embed = discord.Embed(
        title="⚠️ TILT WARNING: RISK PARAMETERS EXCEEDED",
        description=(
            f"Hey {user_mention}, your recent trade log triggered risk protection rules:\n\n"
            + "\n".join(triggers)
            + "\n\n**Recommended Action:**\nStep away from the charts for 30–60 minutes. Close your trading platform and reassess during the next session."
        ),
        color=discord.Color.brand_red(),
        timestamp=datetime.now(timezone.utc)
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


def build_chart_prompt(user_strategy: str, user_timezone: str = "UTC") -> str:
    return (
        "You are an ultra-strict, ruthless prop firm risk auditor. Your ONLY job is to rigidly enforce the user's trading strategy. "
        "You must DISQUALIFY the setup if even ONE rule is bent, missing, or unclear. Do NOT be lenient. Do NOT assume anything that isn't clearly visible.\n\n"
        f"User's strategy rules: {user_strategy}\n"
        f"User's chart x-axis timezone: {user_timezone}\n\n"
        "CRITICAL TIMEZONE & SESSION INSTRUCTION:\n"
        f"1. Read the time visible on the chart's time axis (x-axis). The chart uses timezone: {user_timezone}.\n"
        "2. Convert that chart time to UTC.\n"
        "3. Identify the trading session based on the converted UTC time:\n"
        "   - Asia: 00:00 - 09:00 UTC\n"
        "   - London: 07:00 - 16:00 UTC\n"
        "   - New York: 12:00 - 21:00 UTC\n"
        "   If overlapping, combine them with a slash (e.g. 'London/New York'). If off-hours, use 'Off-hours'. If time is unreadable, use 'Unclear'.\n\n"
        "STRATEGY AUDIT INSTRUCTIONS:\n"
        "- Evaluate the visible chart against EVERY single rule in the user's strategy.\n"
        "- If ANY rule is violated, or if you cannot visually confirm a rule is met, 'matches_strategy' MUST be false.\n"
        "- No assumptions, no hindsight bias, no 'close enough'.\n\n"
        "Return ONLY valid JSON with exactly these keys, no other text:\n"
        '{"direction": "Long" | "Short" | "Unclear", '
        '"entry": "<price as text, or Unclear>", '
        '"stop_loss": "<price as text, or Unclear>", '
        '"take_profit": "<price as text, or Unclear>", '
        '"session": "<Asia | London | New York | London/New York | Off-hours | Unclear>", '
        '"matches_strategy": true | false, '
        '"note": "<Max 15 words. If false, aggressively state EXACTLY which rule failed. If true, describe the entry trigger/rationale directly (e.g., \'Valid entry at 15m FVG after liquidity sweep\').>"}'
    )

def parse_trade_json(raw_text: str) -> dict:
    fallback = {
        "direction": "Unclear",
        "entry": "Unclear",
        "stop_loss": "Unclear",
        "take_profit": "Unclear",
        "session": "Unclear",
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
            "session": data.get("session") or "Unclear",
            "matches_strategy": bool(matches) if matches is not None else None,
            "note": str(data.get("note") or "")[:150],
        }
    except (json.JSONDecodeError, AttributeError):
        logging.warning(f"Could not parse Gemini JSON: {raw_text[:200]}")
        fallback["note"] = raw_text[:100]
        return fallback


def parse_edge_sections(raw_text: str) -> dict:
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
        color = discord.Color.brand_green()
    elif result == "LOSS":
        color = discord.Color.brand_red()
    elif result == "BREAKEVEN":
        color = discord.Color.light_grey()
    else:
        color = discord.Color.blurple()

    embed = discord.Embed(title="📊 Trade Log Analysis", color=color, timestamp=datetime.now(timezone.utc))
    avatar_url = user.display_avatar.url if getattr(user, "display_avatar", None) else None
    embed.set_author(name=getattr(user, "display_name", str(user)), icon_url=avatar_url)

    embed.add_field(name="🧭 Direction", value=f"`{t['direction']}`", inline=True)
    embed.add_field(name="🕒 Session", value=f"`{session or 'Unclear'}`", inline=True)
    embed.add_field(name="⚖️ R:R", value=f"`1:{risk_reward}`" if risk_reward else "`—`", inline=True)
    
    embed.add_field(name="🟢 Entry", value=f"`{t['entry']}`", inline=True)
    embed.add_field(name="🔴 Stop Loss", value=f"`{t['stop_loss']}`", inline=True)
    embed.add_field(name="🎯 Take Profit", value=f"`{t['take_profit']}`", inline=True)

    if t["matches_strategy"] is True:
        match_icon = "✅"
    elif t["matches_strategy"] is False:
        match_icon = "❌"
    else:
        match_icon = "❔"
    
    embed.add_field(name="📋 Strategy Check", value=f"> {match_icon} **{_truncate(t['note'])}**", inline=False)

    if result:
        if result == 'WIN':
            res_emoji = '🟢'
        elif result == 'LOSS':
            res_emoji = '🔴'
        else:
            res_emoji = '⚪'
        embed.add_field(name="🏁 Final Result", value=f"> {res_emoji} **{result}**", inline=False)
    else:
        embed.set_footer(text="Tap a button below to log the final result of this trade")

    return embed


def build_edge_embed(user, stats: dict, sections: dict, trades_count: int) -> discord.Embed:
    embed = discord.Embed(
        title="🧠 Trading Edge Audit", 
        description="Here is your personalized performance breakdown.", 
        color=discord.Color.gold(),
        timestamp=datetime.now(timezone.utc)
    )
    avatar_url = user.display_avatar.url if getattr(user, "display_avatar", None) else None
    embed.set_author(name=getattr(user, "display_name", str(user)), icon_url=avatar_url)

    progress_bar = generate_progress_bar(stats['wins'], stats['total'])
    embed.add_field(
        name="📊 Win Rate", 
        value=f"`{stats['win_rate']}%`\n{progress_bar}\n*({stats['wins']}W / {stats['losses']}L)*", 
        inline=True
    )
    if stats["avg_rr_win"] is not None:
        embed.add_field(name="📈 Avg R:R (Wins)", value=f"`1:{stats['avg_rr_win']}`", inline=True)
    if stats["avg_rr_loss"] is not None:
        embed.add_field(name="📉 Avg R:R (Losses)", value=f"`1:{stats['avg_rr_loss']}`", inline=True)

    embed.add_field(name="🎯 Core Edge", value=f"> {_truncate(sections.get('core_edge'))}", inline=False)
    embed.add_field(name="🕳️ Primary Leak", value=f"> {_truncate(sections.get('primary_leak'))}", inline=False)
    embed.add_field(name="🛠️ Action Plan", value=f"> {_truncate(sections.get('action_plan'))}", inline=False)

    embed.set_footer(text=f"Based on your last {trades_count} completed trades")
    return embed


# -------------------------------------------------------------------
# DISCORD UI COMPONENTS -- MODALS, PAGINATION & DROPDOWNS
# -------------------------------------------------------------------
class StrategyModal(discord.ui.Modal, title="Set Custom Trading Strategy"):
    strategy_input = discord.ui.TextInput(
        label="Trading Rules & Parameters",
        style=discord.TextStyle.paragraph,
        placeholder="Describe your strategy, entry triggers, indicators, and risk parameters...",
        required=True,
        max_length=MAX_STRATEGY_LENGTH,
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        strategy = self.strategy_input.value.strip()

        await db.execute(
            "INSERT INTO strategies (user_id, prompt) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET prompt = excluded.prompt",
            (interaction.user.id, strategy),
        )

        embed = discord.Embed(
            title="✅ Strategy Successfully Updated", 
            description=f"> {strategy}", 
            color=discord.Color.brand_green()
        )
        await interaction.followup.send(embed=embed, ephemeral=True)


class TradeLogFilterSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="All Results", value="ALL", emoji="🌐", default=True),
            discord.SelectOption(label="Wins Only", value="WIN", emoji="🟢"),
            discord.SelectOption(label="Losses Only", value="LOSS", emoji="🔴"),
            discord.SelectOption(label="Breakeven Only", value="BREAKEVEN", emoji="⚪"),
        ]
        super().__init__(placeholder="Filter logs by trade outcome...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        view: TradeLogPaginator = self.view
        view.selected_filter = self.values[0]
        
        for option in self.options:
            option.default = (option.value == view.selected_filter)
            
        view.apply_filter()
        view.current_page = 0
        await view.update_message(interaction)


class TradeLogPaginator(discord.ui.View):
    def __init__(self, user, all_trades: list, page_size: int = 3):
        super().__init__(timeout=180)
        self.user = user
        self.all_trades = all_trades
        self.page_size = page_size
        self.selected_filter = "ALL"
        self.filtered_trades = list(all_trades)
        self.current_page = 0

        self.add_item(TradeLogFilterSelect())

    def apply_filter(self):
        if self.selected_filter == "ALL":
            self.filtered_trades = list(self.all_trades)
        else:
            self.filtered_trades = [t for t in self.all_trades if t[0] == self.selected_filter]

    @property
    def max_pages(self) -> int:
        return max(1, (len(self.filtered_trades) + self.page_size - 1) // self.page_size)

    def build_embed(self) -> discord.Embed:
        if not self.filtered_trades:
            embed = discord.Embed(
                title="📜 Trade Log History",
                description=f"No trades logged matching filter **{self.selected_filter}**.",
                color=discord.Color.orange()
            )
            avatar_url = self.user.display_avatar.url if getattr(self.user, "display_avatar", None) else None
            embed.set_author(name=getattr(self.user, "display_name", str(self.user)), icon_url=avatar_url)
            return embed

        start_idx = self.current_page * self.page_size
        end_idx = start_idx + self.page_size
        page_items = self.filtered_trades[start_idx:end_idx]

        embed = discord.Embed(
            title="📜 Trade Log History",
            color=discord.Color.blurple()
        )
        avatar_url = self.user.display_avatar.url if getattr(self.user, "display_avatar", None) else None
        embed.set_author(name=getattr(self.user, "display_name", str(self.user)), icon_url=avatar_url)

        for idx, t in enumerate(page_items, start=start_idx + 1):
            res, dir_, entry, sl, tp, note, sess, rr, status = t
            res_display = res if res else status
            rr_text = f"1:{rr}" if rr is not None else "-"
            value = (
                f"> **Direction:** `{dir_ or '?'}` | **Result:** `{res_display}`\n"
                f"> **Entry:** `{entry or '-'}` | **SL:** `{sl or '-'}` | **TP:** `{tp or '-'}`\n"
                f"> **R:R:** `{rr_text}` | **Session:** `{sess or '-'}`\n"
                f"> **Note:** {note or '-'}"
            )
            embed.add_field(name=f"🔖 Trade #{idx}", value=value, inline=False)

        embed.set_footer(text=f"Page {self.current_page + 1} of {self.max_pages} • Total Logged: {len(self.filtered_trades)}")
        return embed

    def update_button_states(self):
        self.prev_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page >= self.max_pages - 1)

    async def update_message(self, interaction: discord.Interaction):
        self.update_button_states()
        embed = self.build_embed()
        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.primary, emoji="◀️")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            await self.update_message(interaction)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, emoji="▶️")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.max_pages - 1:
            self.current_page += 1
            await self.update_message(interaction)


# -------------------------------------------------------------------
# PERSISTENT UI -- WIN/LOSS/BREAKEVEN BUTTONS
# -------------------------------------------------------------------
class TradeResultView(discord.ui.View):
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
                "⚠️ **Error:** I couldn't find this trade in the database.", ephemeral=True
            )
            return

        (user_id, status, current_result, direction, entry, stop_loss,
         take_profit, matches_strategy, note, session, risk_reward) = trade

        if interaction.user.id != user_id:
            await interaction.response.send_message(
                "🔒 Only the trader who posted this chart can log its result.", ephemeral=True
            )
            return

        if status == "COMPLETED" and current_result == result_type:
            await interaction.response.send_message(f"✅ Already logged as **{result_type}**.", ephemeral=True)
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
        self.add_view(TradeResultView())


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
    global _last_chart_analysis_at
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
        await message.channel.send(embed=build_upgrade_message())
        await bot.process_commands(message)
        return

    now = time.time()
    # Prune old timestamps periodically to prevent memory leaks
    if len(_last_chart_analysis_at) > 500:
        _last_chart_analysis_at = {uid: ts for uid, ts in _last_chart_analysis_at.items() if now - ts < 3600}

    last_call = _last_chart_analysis_at.get(message.author.id, 0)
    if now - last_call < CHART_COOLDOWN_SECONDS:
        await bot.process_commands(message)
        return
    _last_chart_analysis_at[message.author.id] = now

    attachment = image_attachments[0]

    if attachment.size and attachment.size > MAX_IMAGE_BYTES:
        err_embed = discord.Embed(
            description=f"⚠️ **File Too Large:** Max image size is `{MAX_IMAGE_BYTES // (1024 * 1024)}MB`.",
            color=discord.Color.red()
        )
        await message.channel.send(embed=err_embed)
        await bot.process_commands(message)
        return

    proc_embed = discord.Embed(
        description="🔍 **Analyzing chart data...**\n*Extracting strategy matching, session, and technical levels.*",
        color=discord.Color.blue()
    )
    processing_msg = await message.channel.send(embed=proc_embed)

    try:
        image_bytes = await attachment.read()

        strategy_row = await db.fetchone(
            "SELECT prompt FROM strategies WHERE user_id = ?", (message.author.id,)
        )
        user_strategy = strategy_row[0] if strategy_row else (
            "No custom strategy set -- just identify direction, entry, stop-loss "
            "and take-profit levels visible on the chart."
        )

        user_tz_row = await db.fetchone(
            "SELECT chart_timezone FROM users WHERE user_id = ?", (message.author.id,)
        )
        user_timezone = user_tz_row[0] if user_tz_row and user_tz_row[0] else "UTC"

        with Image.open(io.BytesIO(image_bytes)) as img:
            full_prompt = build_chart_prompt(user_strategy, user_timezone)
            response = await call_gemini(
                [img, full_prompt],
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )

        trade_data = parse_trade_json(response.text)
        del image_bytes

        session = trade_data.get("session", "Unclear")
        if session == "Unclear" or not session:
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
        fail_embed = discord.Embed(
            description="❌ **Error:** Couldn't analyze that chart right now. Please try again shortly.",
            color=discord.Color.red()
        )
        await processing_msg.edit(content=None, embed=fail_embed)

    await bot.process_commands(message)


# -------------------------------------------------------------------
# SLASH COMMANDS
# -------------------------------------------------------------------
@bot.tree.command(name="settimezone", description="Set your TradingView chart timezone for accurate session detection.")
@app_commands.describe(timezone_str="Your chart timezone setting (e.g. UTC, UTC+5:30, EST, PST, IST)")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def set_timezone(interaction: discord.Interaction, timezone_str: str):
    await interaction.response.defer(ephemeral=True)

    if not await is_premium_member(interaction.user.id):
        await interaction.followup.send(embed=build_upgrade_message(), ephemeral=True)
        return

    clean_tz = timezone_str.strip().upper()

    await db.execute(
        "INSERT INTO users (user_id, chart_timezone) VALUES (?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET chart_timezone = excluded.chart_timezone",
        (interaction.user.id, clean_tz),
    )

    embed = discord.Embed(
        title="✅ Timezone Updated",
        description=f"Your chart timezone is now securely set to **`{clean_tz}`**.\n\n> AI chart analysis will now convert x-axis timestamps using this setting.",
        color=discord.Color.brand_green(),
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="setstrategy", description="Set your custom trading strategy via interactive modal pop-up.")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def set_strategy(interaction: discord.Interaction):
    if not await is_premium_member(interaction.user.id):
        await interaction.response.send_message(embed=build_upgrade_message(), ephemeral=True)
        return

    strategy_row = await db.fetchone(
        "SELECT prompt FROM strategies WHERE user_id = ?", (interaction.user.id,)
    )
    
    modal = StrategyModal()
    if strategy_row and strategy_row[0]:
        modal.strategy_input.default = strategy_row[0]

    await interaction.response.send_modal(modal)


@bot.tree.command(name="findmyedge", description="Find patterns in your last 10 logged trades.")
@app_commands.checks.cooldown(1, FINDMYEDGE_COOLDOWN_SECONDS, key=lambda i: i.user.id)
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def find_my_edge(interaction: discord.Interaction):
    await interaction.response.defer()

    if not await is_premium_member(interaction.user.id):
        await interaction.followup.send(embed=build_upgrade_message())
        return

    trades = await db.fetchall(
        "SELECT result, direction, entry, stop_loss, take_profit, note, session, risk_reward "
        "FROM trades WHERE user_id = ? AND status = 'COMPLETED' ORDER BY id DESC LIMIT ?",
        (interaction.user.id, EDGE_LOOKBACK),
    )

    if len(trades) < MIN_TRADES_FOR_EDGE:
        err_embed = discord.Embed(
            description=f"⚠️ **Insufficient Data:** You need at least **`{MIN_TRADES_FOR_EDGE}` completed trades** (out of your last {EDGE_LOOKBACK}) to run an edge analysis. Currently logged: `{len(trades)}`.",
            color=discord.Color.orange()
        )
        await interaction.followup.send(embed=err_embed)
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
        err_embed = discord.Embed(description="❌ **Error:** Couldn't generate your edge audit right now. Please try again shortly.", color=discord.Color.red())
        await interaction.followup.send(embed=err_embed)


@bot.tree.command(name="stats", description="View your overall trading statistics and performance metrics.")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def stats_command(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    if not await is_premium_member(interaction.user.id):
        await interaction.followup.send(embed=build_upgrade_message(), ephemeral=True)
        return

    trades = await db.fetchall(
        "SELECT result, direction, entry, stop_loss, take_profit, note, session, risk_reward "
        "FROM trades WHERE user_id = ? AND status = 'COMPLETED'",
        (interaction.user.id,)
    )

    if not trades:
        err_embed = discord.Embed(description="⚠️ You don't have any completed trades logged yet.", color=discord.Color.orange())
        await interaction.followup.send(embed=err_embed, ephemeral=True)
        return

    st = compute_stats(trades)
    progress_bar = generate_progress_bar(st["wins"], st["total"])

    embed = discord.Embed(title="📊 Your Trading Statistics", color=discord.Color.blue())
    embed.add_field(name="📝 Total Completed", value=f"`{st['total']}`", inline=True)
    embed.add_field(name="🏆 Win Rate", value=f"`{st['win_rate']}%`\n{progress_bar}", inline=True)
    embed.add_field(name="⚖️ Record", value=f"`{st['wins']}W / {st['losses']}L`", inline=True)
    
    if st["avg_rr_win"] is not None:
        embed.add_field(name="📈 Avg Win R:R", value=f"`1:{st['avg_rr_win']}`", inline=True)
    if st["avg_rr_loss"] is not None:
        embed.add_field(name="📉 Avg Loss R:R", value=f"`1:{st['avg_rr_loss']}`", inline=True)

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="viewlogs", description="View and filter your logged trades with pagination.")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def viewlogs_command(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    if not await is_premium_member(interaction.user.id):
        await interaction.followup.send(embed=build_upgrade_message(), ephemeral=True)
        return

    trades = await db.fetchall(
        "SELECT result, direction, entry, stop_loss, take_profit, note, session, risk_reward, status "
        "FROM trades WHERE user_id = ? ORDER BY id DESC",
        (interaction.user.id,),
    )

    if not trades:
        err_embed = discord.Embed(description="⚠️ You haven't logged any trades yet.", color=discord.Color.orange())
        await interaction.followup.send(embed=err_embed, ephemeral=True)
        return

    chronological_trades = list(reversed(trades))
    paginator = TradeLogPaginator(interaction.user, chronological_trades, page_size=3)
    paginator.update_button_states()
    embed = paginator.build_embed()

    await interaction.followup.send(embed=embed, view=paginator, ephemeral=True)


@bot.tree.command(name="deletelast", description="Delete your most recently logged trade.")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def deletelast_command(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    if not await is_premium_member(interaction.user.id):
        await interaction.followup.send(embed=build_upgrade_message(), ephemeral=True)
        return

    last_trade = await db.fetchone(
        "SELECT id, message_id FROM trades WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (interaction.user.id,)
    )

    if not last_trade:
        err_embed = discord.Embed(description="⚠️ You have no logged trades to delete.", color=discord.Color.orange())
        await interaction.followup.send(embed=err_embed, ephemeral=True)
        return

    trade_id, message_id = last_trade
    await db.execute("DELETE FROM trades WHERE id = ?", (trade_id,))

    deleted_msg_note = ""
    try:
        msg = await interaction.channel.fetch_message(message_id)
        await msg.delete()
        deleted_msg_note = " and deleted its original message."
    except Exception:
        deleted_msg_note = "."

    success_embed = discord.Embed(
        title="🗑️ Trade Deleted",
        description=f"Successfully deleted your most recent trade entry `(ID: {trade_id})`{deleted_msg_note}",
        color=discord.Color.green()
    )
    await interaction.followup.send(embed=success_embed, ephemeral=True)


@bot.tree.command(name="help", description="Show information about TradeSight AI commands and features.")
@app_commands.allowed_contexts(guilds=True, dms=True, private_channels=True)
@app_commands.allowed_installs(guilds=True, users=False)
async def help_command(interaction: discord.Interaction):
    embed = discord.Embed(
        title="🤖 TradeSight AI — Command Guide",
        description="Automated chart logging, edge auditing, and risk protection for serious traders.",
        color=discord.Color.gold()
    )
    embed.add_field(
        name="📷 Automatic Chart Logging",
        value="> Drop any chart screenshot in chat or DM to instantly log direction, entry, stop loss, take profit, and strategy matching.",
        inline=False
    )
    embed.add_field(
        name="`/settimezone`",
        value="> Set your chart timezone (e.g., UTC, UTC+5:30, EST, IST) for accurate session detection.",
        inline=False
    )
    embed.add_field(
        name="`/setstrategy`",
        value="> Set your custom trading strategy rules via an interactive modal dialog box.",
        inline=False
    )
    embed.add_field(
        name="`/findmyedge`",
        value="> Analyze your last completed trades to uncover your core edge, primary leaks, and action plan.",
        inline=False
    )
    embed.add_field(
        name="`/stats`",
        value="> View your overall win rate (with visual win bars), total completed trades, and average risk-to-reward metrics.",
        inline=False
    )
    embed.add_field(
        name="`/viewlogs`",
        value="> Paginate and filter your recent trade history by outcome (Wins, Losses, Breakeven).",
        inline=False
    )
    embed.add_field(
        name="`/deletelast`",
        value="> Remove your most recent trade entry from the database.",
        inline=False
    )
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        err_embed = discord.Embed(description=f"⏳ **Cooldown Active:** Please wait `{error.retry_after:.0f}s` before running this again.", color=discord.Color.orange())
    else:
        logging.error(f"Unhandled app command error: {error}")
        err_embed = discord.Embed(description="❌ **System Error:** Something went wrong while running that command.", color=discord.Color.red())

    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=err_embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=err_embed, ephemeral=True)
    except Exception:
        pass


# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
