import os
from dotenv import load_dotenv
import io
import gc
import logging
import threading
import asyncio
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

# Initialize Native Async Gemini Client
genai_client = genai.Client(api_key=GEMINI_API_KEY)

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

# Start Flask in a background thread
threading.Thread(target=run_flask, daemon=True).start()

# -------------------------------------------------------------------
# DISCORD BOT INITIALIZATION
# -------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)
DB_PATH = "trading_journal.db"

# -------------------------------------------------------------------
# ASYNC DATABASE SETUP
# -------------------------------------------------------------------
async def init_db():
    """Initializes SQLite database tables asynchronously."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS strategies (
                user_id INTEGER PRIMARY KEY,
                prompt TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                message_id INTEGER UNIQUE NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                analysis TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()
    logging.info("Asynchronous SQLite database initialized successfully.")

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
    # Ignore bot messages
    if message.author.bot:
        return

    # Check for image attachments
    image_attachments = [
        att for att in message.attachments 
        if att.content_type and att.content_type.startswith("image/")
    ]

    if not image_attachments:
        await bot.process_commands(message)
        return

    attachment = image_attachments[0]
    processing_msg = await message.channel.send("🔍 **Analyzing chart with Gemini AI...**")

    try:
        # Download image bytes
        image_bytes = await attachment.read()

        # Fetch custom user strategy or fallback to default
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT prompt FROM strategies WHERE user_id = ?", (message.author.id,)) as cursor:
                row = await cursor.fetchone()
                user_strategy = row[0] if row else "Analyze this trading chart for key support/resistance levels, trend directions, entry triggers, and risk-to-reward ratio."

        # Strict RAM Management: Use Image context manager
        with Image.open(io.BytesIO(image_bytes)) as img:
            full_prompt = (
    f"You are an objective trade journal assistant. Analyze this chart screenshot STRICTLY keeping in mind the user's defined strategy.\n"
    f"User's Strategy Rules: {user_strategy}\n\n"
    f"CRITICAL RESTRICTION: DO NOT give trade signals or financial advice. "
    f"You are not a signal provider. Your job is to provide a short description,entry prices, stop-loss levels, take-profit targets in the screenshot ONLY.: "
    f"break down what price action occurred, how it interacted with the chart, and whether it matched the user's strategy rules.\n\n"
    f"Provide a structured breakdown:\n"
    f"1. **Price Action Context**: What happened in this chart session.\n"
    f"2. **Strategy Evaluation**: Did the setup align with the user's defined strategy rules?\n"
)
            
            # Native Async Gemini API Call
            response = await genai_client.aio.models.generate_content(
                model="gemini-3.6-flash",
                contents=[img, full_prompt]
            )

        analysis_text = response.text

        # Explicitly release image objects & run garbage collection to prevent RAM spikes
        del image_bytes
        gc.collect()

        # Send analysis reply
        reply_header = f"📊 **Trade Analysis for <@{message.author.id}>**\n"
        full_response = f"{reply_header}\n{analysis_text}\n\n*React with 🟢 for WIN or 🔴 for LOSS to record this trade in your journal.*"
        
        # Handle Discord message length limit (2000 chars)
        if len(full_response) > 2000:
            analysis_msg = await message.channel.send(reply_header + "\n" + analysis_text[:1800] + "...\n\n*React below:*")
        else:
            analysis_msg = await message.channel.send(full_response)

        # Remove temporary processing message
        await processing_msg.delete()

        # Add reactions for trade logging
        await analysis_msg.add_reaction("🟢")
        await analysis_msg.add_reaction("🔴")

        # Save trade to database as PENDING
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO journal (user_id, message_id, status, analysis) VALUES (?, ?, ?, ?)",
                (message.author.id, analysis_msg.id, "PENDING", analysis_text)
            )
            await db.commit()

    except Exception as e:
        logging.error(f"Error during chart analysis: {e}")
        await processing_msg.edit(content=f"❌ An error occurred while analyzing the chart: `{str(e)}`")

    await bot.process_commands(message)

# -------------------------------------------------------------------
# REACTION EVENT (TRADE LOGGING & SWAP FIX)
# -------------------------------------------------------------------
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # Ignore bot's own reactions
    if payload.user_id == bot.user.id:
        return

    emoji = str(payload.emoji)
    if emoji not in ["🟢", "🔴"]:
        return

    result_type = "WIN" if emoji == "🟢" else "LOSS"

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, status, result FROM journal WHERE message_id = ?", (payload.message_id,)) as cursor:
            trade = await cursor.fetchone()

        if not trade:
            return

        user_id, status, current_result = trade

        # Ensure only the trade owner can log or change the reaction
        if payload.user_id != user_id:
            return

        # Update status and handle reaction misclick/swaps
        if status == "PENDING" or (status == "COMPLETED" and current_result != result_type):
            await db.execute(
                "UPDATE journal SET status = 'COMPLETED', result = ? WHERE message_id = ?",
                (result_type, payload.message_id)
            )
            await db.commit()

            channel = bot.get_channel(payload.channel_id)
            if channel:
                msg = await channel.fetch_message(payload.message_id)
                status_note = "Logged" if status == "PENDING" else "Updated"
                await channel.send(
                    f"✅ **Trade {status_note}:** Recorded as **{result_type}** for <@{user_id}>!",
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
            "INSERT INTO strategies (user_id, prompt) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET prompt = excluded.prompt",
            (interaction.user.id, strategy)
        )
        await db.commit()
    
    await interaction.followup.send("✅ Your trading strategy prompt has been updated successfully!", ephemeral=True)

@bot.tree.command(name="findmyedge", description="Run an AI audit on your recent logged trades to identify your trading edge.")
async def find_my_edge(interaction: discord.Interaction):
    await interaction.response.defer()

    # Premium Role Verification with Cold Cache Fallback
    if PREMIUM_ROLE_ID:
        guild = interaction.guild
        if guild:
            member = guild.get_member(interaction.user.id)
            if member is None:
                try:
                    member = await guild.fetch_member(interaction.user.id)
                except Exception as e:
                    logging.error(f"Failed to fetch member directly: {e}")
            
            role_id_int = int(PREMIUM_ROLE_ID)
            if not member or not any(r.id == role_id_int for r in member.roles):
                await interaction.followup.send("🔒 This command is reserved for **Premium Members**. Please upgrade your access to run trading audits.")
                return

    # Fetch last 20 completed trades (Prevents Memory & API Payloads Crashes)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT result, analysis FROM journal WHERE user_id = ? AND status = 'COMPLETED' ORDER BY id DESC LIMIT 20",
            (interaction.user.id,)
        ) as cursor:
            trades = await cursor.fetchall()

    if len(trades) < 3:
        await interaction.followup.send(f"⚠️ You need at least **3 completed trades** to run an edge analysis. Current logged trades: `{len(trades)}`.")
        return

    # Compile trade log summary for Gemini
    formatted_trades = []
    wins = 0
    losses = 0

    for idx, (result, analysis) in enumerate(reversed(trades), 1):
        if result == "WIN":
            wins += 1
        else:
            losses += 1
        formatted_trades.append(f"--- TRADE #{idx} [{result}] ---\n{analysis[:500]}...")

    trade_data_block = "\n\n".join(formatted_trades)
    win_rate = round((wins / len(trades)) * 100, 1)

    prompt = (
        f"You are a elite quantitative trading psychologist and risk manager. "
        f"Analyze these last {len(trades)} trades for this user:\n\n"
        f"SUMMARY STATS: Wins: {wins}, Losses: {losses}, Win Rate: {win_rate}%\n\n"
        f"TRADE LOGS:\n{trade_data_block}\n\n"
        f"Provide a concise, highly actionable audit including:\n"
        f"1. **Core Edge**: What conditions yielded their wins?\n"
        f"2. **Primary Leak**: What common pattern is present in their losses?\n"
        f"3. **Action Plan**: 2 specific rules to improve their win rate and risk execution."
    )

    try:
        # Native Async Gemini Call
        response = await genai_client.aio.models.generate_content(
            model="gemini-3.6-flash",
            contents=prompt
        )

        audit_result = response.text
        header = f"🧠 **AI Trading Edge Audit (<@{interaction.user.id}>)**\n*Analyzed Last {len(trades)} Logged Trades (Win Rate: {win_rate}%)*\n\n"

        if len(header + audit_result) > 2000:
            await interaction.followup.send(header + audit_result[:1800] + "\n\n*(Audit truncated due to Discord length limits)*")
        else:
            await interaction.followup.send(header + audit_result)

    except Exception as e:
        logging.error(f"Error generating edge audit: {e}")
        await interaction.followup.send(f"❌ Failed to generate trading audit: `{str(e)}`")

# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
import os
import io
import gc
import logging
import threading
import asyncio
from flask import Flask
import discord
from discord import app_commands
from discord.ext import commands
from PIL import Image
import aiosqlite
from google import genai
from google.genai import types

# -------------------------------------------------------------------
# LOGGING & CONFIGURATION
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
PREMIUM_ROLE_ID = os.getenv("PREMIUM_ROLE_ID")  # Optional: Role ID for /findmyedge access

if not DISCORD_TOKEN or not GEMINI_API_KEY:
    raise ValueError("Missing critical environment variables (DISCORD_TOKEN or GEMINI_API_KEY).")

# Initialize Native Async Gemini Client
genai_client = genai.Client(api_key=GEMINI_API_KEY)

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

# Start Flask in a background thread
threading.Thread(target=run_flask, daemon=True).start()

# -------------------------------------------------------------------
# DISCORD BOT INITIALIZATION
# -------------------------------------------------------------------
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)
DB_PATH = "trading_journal.db"

# -------------------------------------------------------------------
# ASYNC DATABASE SETUP
# -------------------------------------------------------------------
async def init_db():
    """Initializes SQLite database tables asynchronously."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS strategies (
                user_id INTEGER PRIMARY KEY,
                prompt TEXT NOT NULL
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                message_id INTEGER UNIQUE NOT NULL,
                status TEXT NOT NULL,
                result TEXT,
                analysis TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()
    logging.info("Asynchronous SQLite database initialized successfully.")

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
    # Ignore bot messages
    if message.author.bot:
        return

    # Check for image attachments
    image_attachments = [
        att for att in message.attachments 
        if att.content_type and att.content_type.startswith("image/")
    ]

    if not image_attachments:
        await bot.process_commands(message)
        return

    attachment = image_attachments[0]
    processing_msg = await message.channel.send("🔍 **Analyzing chart with Gemini AI...**")

    try:
        # Download image bytes
        image_bytes = await attachment.read()

        # Fetch custom user strategy or fallback to default
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT prompt FROM strategies WHERE user_id = ?", (message.author.id,)) as cursor:
                row = await cursor.fetchone()
                user_strategy = row[0] if row else "Analyze this trading chart for key support/resistance levels, trend directions, entry triggers, and risk-to-reward ratio."

        # Strict RAM Management: Use Image context manager
        with Image.open(io.BytesIO(image_bytes)) as img:
            full_prompt = (
                f"You are a professional trading analyst. Apply the following strategy to analyze this chart:\n"
                f"Strategy: {user_strategy}\n\n"
                f"Provide a clear, structured breakdown including Trade Bias (Long/Short/Neutral), Key Levels, and Setup Quality."
            )
            
            # Native Async Gemini API Call
            response = await genai_client.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=[img, full_prompt]
            )

        analysis_text = response.text

        # Explicitly release image objects & run garbage collection to prevent RAM spikes
        del image_bytes
        gc.collect()

        # Send analysis reply
        reply_header = f"📊 **Trade Analysis for <@{message.author.id}>**\n"
        full_response = f"{reply_header}\n{analysis_text}\n\n*React with 🟢 for WIN or 🔴 for LOSS to record this trade in your journal.*"
        
        # Handle Discord message length limit (2000 chars)
        if len(full_response) > 2000:
            analysis_msg = await message.channel.send(reply_header + "\n" + analysis_text[:1800] + "...\n\n*React below:*")
        else:
            analysis_msg = await message.channel.send(full_response)

        # Remove temporary processing message
        await processing_msg.delete()

        # Add reactions for trade logging
        await analysis_msg.add_reaction("🟢")
        await analysis_msg.add_reaction("🔴")

        # Save trade to database as PENDING
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO journal (user_id, message_id, status, analysis) VALUES (?, ?, ?, ?)",
                (message.author.id, analysis_msg.id, "PENDING", analysis_text)
            )
            await db.commit()

    except Exception as e:
        logging.error(f"Error during chart analysis: {e}")
        await processing_msg.edit(content=f"❌ An error occurred while analyzing the chart: `{str(e)}`")

    await bot.process_commands(message)

# -------------------------------------------------------------------
# REACTION EVENT (TRADE LOGGING & SWAP FIX)
# -------------------------------------------------------------------
@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    # Ignore bot's own reactions
    if payload.user_id == bot.user.id:
        return

    emoji = str(payload.emoji)
    if emoji not in ["🟢", "🔴"]:
        return

    result_type = "WIN" if emoji == "🟢" else "LOSS"

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, status, result FROM journal WHERE message_id = ?", (payload.message_id,)) as cursor:
            trade = await cursor.fetchone()

        if not trade:
            return

        user_id, status, current_result = trade

        # Ensure only the trade owner can log or change the reaction
        if payload.user_id != user_id:
            return

        # Update status and handle reaction misclick/swaps
        if status == "PENDING" or (status == "COMPLETED" and current_result != result_type):
            await db.execute(
                "UPDATE journal SET status = 'COMPLETED', result = ? WHERE message_id = ?",
                (result_type, payload.message_id)
            )
            await db.commit()

            channel = bot.get_channel(payload.channel_id)
            if channel:
                msg = await channel.fetch_message(payload.message_id)
                status_note = "Logged" if status == "PENDING" else "Updated"
                await channel.send(
                    f"✅ **Trade {status_note}:** Recorded as **{result_type}** for <@{user_id}>!",
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
            "INSERT INTO strategies (user_id, prompt) VALUES (?, ?) ON CONFLICT(user_id) DO UPDATE SET prompt = excluded.prompt",
            (interaction.user.id, strategy)
        )
        await db.commit()
    
    await interaction.followup.send("✅ Your trading strategy prompt has been updated successfully!", ephemeral=True)

@bot.tree.command(name="findmyedge", description="Run an AI audit on your recent logged trades to identify your trading edge.")
async def find_my_edge(interaction: discord.Interaction):
    await interaction.response.defer()

    # Premium Role Verification with Cold Cache Fallback
    if PREMIUM_ROLE_ID:
        guild = interaction.guild
        if guild:
            member = guild.get_member(interaction.user.id)
            if member is None:
                try:
                    member = await guild.fetch_member(interaction.user.id)
                except Exception as e:
                    logging.error(f"Failed to fetch member directly: {e}")
            
            role_id_int = int(PREMIUM_ROLE_ID)
            if not member or not any(r.id == role_id_int for r in member.roles):
                await interaction.followup.send("🔒 This command is reserved for **Premium Members**. Please upgrade your access to run trading audits.")
                return

    # Fetch last 20 completed trades (Prevents Memory & API Payloads Crashes)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT result, analysis FROM journal WHERE user_id = ? AND status = 'COMPLETED' ORDER BY id DESC LIMIT 20",
            (interaction.user.id,)
        ) as cursor:
            trades = await cursor.fetchall()

    if len(trades) < 3:
        await interaction.followup.send(f"⚠️ You need at least **3 completed trades** to run an edge analysis. Current logged trades: `{len(trades)}`.")
        return

    # Compile trade log summary for Gemini
    formatted_trades = []
    wins = 0
    losses = 0

    for idx, (result, analysis) in enumerate(reversed(trades), 1):
        if result == "WIN":
            wins += 1
        else:
            losses += 1
        formatted_trades.append(f"--- TRADE #{idx} [{result}] ---\n{analysis[:500]}...")

    trade_data_block = "\n\n".join(formatted_trades)
    win_rate = round((wins / len(trades)) * 100, 1)

    prompt = (
        f"You are a elite quantitative trading psychologist and risk manager. "
        f"Analyze these last {len(trades)} trades for this user:\n\n"
        f"SUMMARY STATS: Wins: {wins}, Losses: {losses}, Win Rate: {win_rate}%\n\n"
        f"TRADE LOGS:\n{trade_data_block}\n\n"
        f"Provide a concise, highly actionable audit including:\n"
        f"1. **Core Edge**: What conditions yielded their wins?\n"
        f"2. **Primary Leak**: What common pattern is present in their losses?\n"
        f"3. **Action Plan**: 2 specific rules to improve their win rate and risk execution."
    )

    try:
        # Native Async Gemini Call
        response = await genai_client.aio.models.generate_content(
            model="gemini-2.5-flash",
            contents=prompt
        )

        audit_result = response.text
        header = f"🧠 **AI Trading Edge Audit (<@{interaction.user.id}>)**\n*Analyzed Last {len(trades)} Logged Trades (Win Rate: {win_rate}%)*\n\n"

        if len(header + audit_result) > 2000:
            await interaction.followup.send(header + audit_result[:1800] + "\n\n*(Audit truncated due to Discord length limits)*")
        else:
            await interaction.followup.send(header + audit_result)

    except Exception as e:
        logging.error(f"Error generating edge audit: {e}")
        await interaction.followup.send(f"❌ Failed to generate trading audit: `{str(e)}`")

# -------------------------------------------------------------------
# MAIN EXECUTION
# -------------------------------------------------------------------
if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
