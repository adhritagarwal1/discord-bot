import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import os
from flask import Flask
from threading import Thread
from google import genai

# ==========================================
# 1. FLASK SERVER (Keep-Alive for Hosting)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "TradeSight AI is online and monitoring the markets."

def run_flask():
    # Binds to 0.0.0.0 to satisfy Render/cloud hosting port requirements
    app.run(host='0.0.0.0', port=8080)

def keep_alive():
    t = Thread(target=run_flask)
    t.start()

# ==========================================
# 2. DATABASE SETUP (SQLite)
# ==========================================
def setup_db():
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    # Create the trade journal table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (user_id INTEGER, ticker TEXT, result TEXT, pnl REAL, notes TEXT)''')
    conn.commit()
    conn.close()

# ==========================================
# 3. DISCORD BOT & API CLIENTS
# ==========================================
class TradeSightBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=discord.Intents.default())

    async def setup_hook(self):
        setup_db()
        await self.tree.sync()
        print("TradeSight AI is booted up and slash commands are synced.")

bot = TradeSightBot()

# Initialize the Google Gen AI client (reads GEMINI_API_KEY from environment variables)
gemini_client = genai.Client()

# ==========================================
# 4. SLASH COMMANDS
# ==========================================

@bot.tree.command(name="logtrade", description="Log a completed trade to your journal.")
@app_commands.describe(
    ticker="The asset traded (e.g., XAUUSD, ES, MNQ)", 
    result="Win / Loss / Break Even", 
    pnl="Profit or Loss amount (e.g., 500.50)", 
    notes="Setup details, strategy used, or psychological mistakes"
)
async def logtrade(interaction: discord.Interaction, ticker: str, result: str, pnl: float, notes: str):
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("INSERT INTO trades VALUES (?, ?, ?, ?, ?)", 
              (interaction.user.id, ticker.upper(), result, pnl, notes))
    conn.commit()
    conn.close()
    
    await interaction.response.send_message(f"✅ **Trade Logged:** {ticker.upper()} | {result} | ${pnl:.2f}")


@bot.tree.command(name="findmyedge", description="Premium AI audit of your trade history to find your edge.")
async def findmyedge(interaction: discord.Interaction):
    # 1. Gatekeep for the Premium Trader role
    role_names = [role.name for role in interaction.user.roles]
    if "Premium Trader" not in role_names:
        await interaction.response.send_message("❌ You need the **Premium Trader** role to access quantitative audits.", ephemeral=True)
        return

    # 2. Defer the response so Discord doesn't time out while Gemini is thinking
    await interaction.response.defer(thinking=True)

    # 3. Fetch the user's trading history
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("SELECT ticker, result, pnl, notes FROM trades WHERE user_id=?", (interaction.user.id,))
    trades = c.fetchall()
    conn.close()

    # 4. Enforce the minimum trade requirement
    if len(trades) < 3:
        await interaction.followup.send("📊 You need at least **3 completed trades** logged in the database to generate an edge report. Keep trading!")
        return

    # 5. Format the data for the AI
    trade_data = "\n".join([f"- Asset: {t[0]} | Result: {t[1]} (${t[2]}). Notes: {t[3]}" for t in trades])
    
    prompt = f"""
    You are a strict, highly analytical proprietary trading firm risk manager. 
    Analyze the following day trading history. Look for:
    1. Quantitative Edge (Which setups or assets like XAUUSD/Futures are working best?)
    2. Psychological Leaks (Recurring mistakes in the notes)
    3. Actionable adjustments for the next session.
    
    Keep the report concise, highly professional, and data-driven.
    
    Trader's Log:
    {trade_data}
    """

    # 6. Generate the Edge Report using Gemini 3.6 Flash
    try:
        response = gemini_client.models.generate_content(
            model="gemini-3.6-flash",
            contents=prompt,
        )
        edge_report = response.text
        
        # 7. Handle Discord's 2000 character limit by chunking if necessary
        if len(edge_report) > 1900:
            await interaction.followup.send("📈 **Your Edge Report is ready:**")
            for i in range(0, len(edge_report), 1900):
                await interaction.channel.send(edge_report[i:i+1900])
        else:
            await interaction.followup.send(f"📈 **Your Edge Report:**\n\n{edge_report}")

    except Exception as e:
        await interaction.followup.send(f"⚠️ An error occurred while running the quantitative audit: {e}")

# ==========================================
# 5. EXECUTION
# ==========================================
if __name__ == "__main__":
    # Start the Flask web server on a separate thread
    keep_alive() 
    
    # Run the Discord bot
    # Make sure to set your DISCORD_TOKEN environment variable!
    bot.run(os.getenv("DISCORD_TOKEN"))
