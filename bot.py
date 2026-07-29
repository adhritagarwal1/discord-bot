import os
import io
import json
import time
import logging
import threading
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
PREMIUM_ROLE_ID = os.getenv("PREMIUM_ROLE_ID")  # Optional: Role ID for /findmyedge access

if not DISCORD_TOKEN or not GEMINI_API_KEY:
    raise ValueError("Missing critical environment variables (DISCORD_TOKEN or GEMINI_API_KEY).")

genai_client = genai.Client(api_key=GEMINI_API_KEY)

GEMINI_MODEL = "gemini-3.6-flash"

# Per-user cooldown so one trader can't spam paid Gemini calls
CHART_COOLDOWN_SECONDS = 15
_last_chart_analysis_at = {}  # user_id -> unix timestamp

MAX_IMAGE_BYTES = 8 * 1024 * 1024  # 8 MB

MIN_TRADES_FOR_EDGE = 3
EDGE_LOOKBACK = 10  # /findmyedge only ever looks at the last 10 completed trades

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
# DISCORD BOT INITIALIZATION
# -------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)
DB_PATH = "tradesight.db"

# -------------------------------------------------------------------
# ASYNC DATABASE SETUP
# -------------------------------------------------------------------
async def init_db():
    """Initializes SQLite tables asynchronously.

    `trades` intentionally stores only the fields needed to find an edge
    (direction/entry/SL/TP/result) instead of long free-text analysis --
    this is not meant to be a journaling table.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS strategies (
                user_id INTEGER PRIMARY KEY,
                prompt TEXT NOT NULL
            )
        """)
        await db.execute("""
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
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()
    logging.info("Database initialized successfully.")


# -------------------------------------------------------------------
# GEMINI HELPERS
# -------------------------------------------------------------------
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


def format_trade_summary(user_id: int, t: dict) -> str:
    if t["matches_strategy"] is True:
        match_icon = "🟢 **Rules Followed**"
    elif t["matches_strategy"] is False:
        match_icon = "🔴 **Rules Violated**"
    else:
        match_icon = "⚪ **Unclear**"

    return (
        f"### 📊 Trade Logged — <@{user_id}>\n"
        f"> **Direction:** `{t['direction']}`\n"
        f"> **Entry:** `{t['entry']}` | **SL:** `{t['stop_loss']}` | **TP:** `{t['take_profit']}`\n"
        f"> **Strategy Check:** {match_icon}\n"
        f"> *Note:* {t['note'] or 'None'}\n\n"
        f"📌 *React below to log outcome:* 🟢 **WIN** | 🔴 **LOSS**"
    )


# -------------------------------------------------------------------
# BOT EVENTS
# -------------------------------------------------------------------
@bot.event
async def on_ready():
    await init_db()
    try:
        synced = await bot.tree.sync()
        logging.info(f"Synced {len(synced)} slash command(s).")
    except Exception as e:
        logging.error(f"Failed to sync slash commands: {e}")
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

    # Cooldown: protect against spamming paid Gemini calls
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

    processing_msg = await message.channel.send("🔍 **Analyzing chart structure...**")

    try:
        image_bytes = await attachment.read()

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT prompt FROM strategies WHERE user_id = ?", (message.author.id,)
            ) as cursor:
                row = await cursor.fetchone()
                user_strategy = row[0] if row else (
                    "No custom strategy set -- just identify direction, entry, stop-loss "
                    "and take-profit levels visible on the chart."
                )

        with Image.open(io.BytesIO(image_bytes)) as img:
            full_prompt = build_chart_prompt(user_strategy)
            response = await genai_client.aio.models.generate_content(
                model=GEMINI_MODEL,
                contents=[img, full_prompt],
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )

        trade_data = parse_trade_json(response.text)
        del image_bytes

        summary = format_trade_summary(message.author.id, trade_data)
        analysis_msg = await message.channel.send(summary)
        await processing_msg.delete()

        await analysis_msg.add_reaction("🟢")
        await analysis_msg.add_reaction("🔴")

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO trades "
                "(user_id, message_id, status, direction, entry, stop_loss, take_profit, matches_strategy, note) "
                "VALUES (?, ?, 'PENDING', ?, ?, ?, ?, ?, ?)",
                (
                    message.author.id,
                    analysis_msg.id,
                    trade_data["direction"],
                    trade_data["entry"],
                    trade_data["stop_loss"],
                    trade_data["take_profit"],
                    trade_data["matches_strategy"],
                    trade_data["note"],
                ),
            )
            await db.commit()

    except Exception as e:
        logging.error(f"Error during chart analysis: {e}")
        await processing_msg.edit(content=f"❌ Couldn't analyze that chart: `{str(e)}`")

    await bot.process_commands(message)


# -------------------------------------------------------------------
# REACTION EVENT (RESULT LOGGING)
# -------------------------------------------------------------------
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return

    emoji = str(payload.emoji)
    if emoji not in ["🟢", "🔴"]:
        return

    result_type = "WIN" if emoji == "🟢" else "LOSS"

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id, status, result FROM trades WHERE message_id = ?",
            (payload.message_id,)
        ) as cursor:
            trade = await cursor.fetchone()

        if not trade:
            return

        user_id, status, current_result = trade

        # Only the trade owner can log or correct their own result
        if payload.user_id != user_id:
            return

        if status == "PENDING" or (status == "COMPLETED" and current_result != result_type):
            await db.execute(
                "UPDATE trades SET status = 'COMPLETED', result = ? WHERE message_id = ?",
                (result_type, payload.message_id)
            )
            await db.commit()

            channel = bot.get_channel(payload.channel_id)
            if channel:
                status_note = "Logged" if status == "PENDING" else "Updated"
                badge = "🟢 **WIN**" if result_type == "WIN" else "🔴 **LOSS**"
                await channel.send(
                    f"✅ **Trade {status_note}:** Recorded as {badge} for <@{user_id}>!",
                    delete_after=6
                )


# -------------------------------------------------------------------
# SLASH COMMANDS
# -------------------------------------------------------------------
@bot.tree.command(name="setstrategy", description="Set your custom trading strategy for chart analysis.")
@app_commands.describe(strategy="Describe your trading rules, indicators, entry triggers, and risk model.")
async def set_strategy(interaction: discord.Interaction, strategy: str):
    await interaction.response.defer(ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO strategies (user_id, prompt) VALUES (?, ?) "
            "ON CONFLICT(user_id) DO UPDATE SET prompt = excluded.prompt",
            (interaction.user.id, strategy)
        )
        await db.commit()

    await interaction.followup.send("✅ Your trading strategy has been updated successfully!", ephemeral=True)


@bot.tree.command(name="findmyedge", description="Find patterns in your last 10 logged trades.")
async def find_my_edge(interaction: discord.Interaction):
    await interaction.response.defer()

    # Premium role check (defensive parsing -- a malformed env var should never crash the command)
    if PREMIUM_ROLE_ID:
        role_id_int = None
        try:
            role_id_int = int(PREMIUM_ROLE_ID)
        except ValueError:
            logging.error(f"PREMIUM_ROLE_ID is not a valid integer: {PREMIUM_ROLE_ID!r}")

        if role_id_int is not None:
            guild = interaction.guild
            member = guild.get_member(interaction.user.id) if guild else None
            if guild and member is None:
                try:
                    member = await guild.fetch_member(interaction.user.id)
                except Exception as e:
                    logging.error(f"Failed to fetch member: {e}")

            if not member or not any(r.id == role_id_int for r in member.roles):
                await interaction.followup.send(
                    "🔒 This command is reserved for **Premium Members**. Please upgrade your access to run edge audits."
                )
                return

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT result, direction, entry, stop_loss, take_profit, note FROM trades "
            "WHERE user_id = ? AND status = 'COMPLETED' ORDER BY id DESC LIMIT ?",
            (interaction.user.id, EDGE_LOOKBACK)
        ) as cursor:
            trades = await cursor.fetchall()

    if len(trades) < MIN_TRADES_FOR_EDGE:
        await interaction.followup.send(
            f"⚠️ You need at least **{MIN_TRADES_FOR_EDGE} completed trades** (out of your last "
            f"{EDGE_LOOKBACK}) to run an edge analysis. Currently logged: `{len(trades)}`."
        )
        return

    wins = sum(1 for t in trades if t[0] == "WIN")
    losses = len(trades) - wins
    win_rate = round((wins / len(trades)) * 100, 1)

    lines = []
    for idx, (result, direction, entry, sl, tp, note) in enumerate(reversed(trades), 1):
        res_icon = "🟢" if result == "WIN" else "🔴"
        lines.append(
            f"`#{idx}` {res_icon} **{direction or 'Unclear'}** | Entry: `{entry or '-'}` | "
            f"SL: `{sl or '-'}` | TP: `{tp or '-'}` | *{note or ''}*"
        )
    trade_block = "\n".join(lines)

    prompt = (
        "You are a trading performance analyst. Based ONLY on the structured trade data below "
        f"(this trader's last {len(trades)} completed trades), find their edge.\n\n"
        f"Win rate: {win_rate}% ({wins}W / {losses}L)\n\n"
        f"{trade_block}\n\n"
        "Respond with exactly these three short sections and nothing else:\n"
        "1. Core Edge -- the common condition behind the winning trades\n"
        "2. Primary Leak -- the common condition behind the losing trades\n"
        "3. Action Plan -- 2 specific, concrete rules to improve win rate\n"
        "Do not restate the raw data line by line. Do not suggest any new trades."
    )

    try:
        response = await genai_client.aio.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )
        audit_result = response.text or "No analysis returned."
        header = (
            f"### 🧠 Trading Edge Audit — <@{interaction.user.id}>\n"
            f"> **Sample Size:** Last {len(trades)} completed trades\n"
            f"> **Win Rate:** `{win_rate}%` (`{wins}W` / `{losses}L`)\n\n"
            f"---\n\n"
        )
        full_response = header + audit_result
        if len(full_response) > 2000:
            full_response = full_response[:1950] + "\n\n*(truncated)*"

        await interaction.followup.send(full_response)

    except Exception as e:
        logging.error(f"Error generating edge audit: {e}")
        await interaction.followup.send(f"❌ Failed to generate trading audit: `{str(e)}`")


# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
