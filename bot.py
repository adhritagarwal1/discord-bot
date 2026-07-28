import os
import io
import sqlite3
import discord
from discord.ext import commands
import google.generativeai as genai
from PIL import Image
from dotenv import load_dotenv

# ==========================================
# 1. SETUP & CONFIGURATION
# ==========================================
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GEMINI_KEY = os.getenv('GEMINI_API_KEY')

# Configure Gemini
genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel('gemini-3.6-flash')

# Privileged Intents
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

class TradeBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
    
    async def setup_hook(self):
        await self.tree.sync()
        print("Slash commands synced successfully!")

bot = TradeBot()

# ==========================================
# 2. DATABASE SETUP
# ==========================================
DB_FILE = "bot_database.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            strategy TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            message_id INTEGER,
            status TEXT,
            result TEXT,
            analysis TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

# ==========================================
# 3. HELPER FUNCTIONS
# ==========================================
def get_user_strategy(user_id):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT strategy FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else "No specific strategy set."

def set_user_strategy(user_id, strategy):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO users (user_id, strategy) 
        VALUES (?, ?) 
        ON CONFLICT(user_id) DO UPDATE SET strategy = excluded.strategy
    ''', (user_id, strategy))
    conn.commit()
    conn.close()

# ==========================================
# 4. EVENTS
# ==========================================
@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name} (ID: {bot.user.id})')
    print('Trade bot is online!')

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    
    await bot.process_commands(message)

    if message.attachments:
        for attachment in message.attachments:
            if attachment.content_type and attachment.content_type.startswith("image/"):
                
                async with message.channel.typing():
                    try:
                        image_bytes = await attachment.read()
                        img = Image.open(io.BytesIO(image_bytes))

                        strategy = get_user_strategy(message.author.id)

                        prompt = (
                            f"You are an expert day trader. Analyze this chart based on the user's strategy: '{strategy}'. "
                            f"Provide a brief, structured analysis of the setup, bias, and key levels."
                        )
                        response = await model.generate_content_async([prompt, img])
                        analysis = response.text

                        embed = discord.Embed(
                            title="📈 Trade Setup Logged",
                            description=analysis,
                            color=discord.Color.blue()
                        )
                        embed.set_footer(text="React 🟢 for WIN or 🔴 for LOSS to update your journal.")
                        embed.set_image(url=attachment.url)

                        reply_msg = await message.reply(embed=embed)

                        await reply_msg.add_reaction("🟢")
                        await reply_msg.add_reaction("🔴")

                        conn = sqlite3.connect(DB_FILE)
                        c = conn.cursor()
                        c.execute(
                            "INSERT INTO journal (user_id, message_id, status, analysis) VALUES (?, ?, ?, ?)",
                            (message.author.id, reply_msg.id, "PENDING", analysis)
                        )
                        conn.commit()
                        conn.close()

                    except Exception as e:
                        await message.reply(f"❌ Error analyzing chart: {e}")
                
                return

@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id:
        return

    emoji = str(payload.emoji)
    if emoji not in ["🟢", "🔴"]:
        return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT id, user_id, status FROM journal WHERE message_id = ?", (payload.message_id,))
    row = c.fetchone()

    if row:
        trade_id, db_user_id, status = row
        
        if payload.user_id != db_user_id:
            conn.close()
            return
            
        if status == "PENDING":
            result = "WIN" if emoji == "🟢" else "LOSS"
            
            c.execute("UPDATE journal SET status = ?, result = ? WHERE message_id = ?", ("COMPLETED", result, payload.message_id))
            conn.commit()
            
            channel = bot.get_channel(payload.channel_id)
            if channel:
                try:
                    msg = await channel.fetch_message(payload.message_id)
                    if msg.embeds:
                        embed = msg.embeds[0]
                        if result == "WIN":
                            embed.color = discord.Color.green()
                            embed.title = "✅ Trade Marked as WIN"
                        else:
                            embed.color = discord.Color.red()
                            embed.title = "❌ Trade Marked as LOSS"
                        
                        embed.set_footer(text="Logged permanently in your database.")
                        await msg.edit(embed=embed)
                        await msg.clear_reactions()
                except discord.HTTPException:
                    pass
    conn.close()

# ==========================================
# 5. SLASH COMMANDS
# ==========================================
@bot.tree.command(name="setstrategy", description="Define your trading strategy (e.g. SMC, Price Action, ICT)")
async def set_strategy(interaction: discord.Interaction, strategy: str):
    set_user_strategy(interaction.user.id, strategy)
    await interaction.response.send_message(f"✅ Strategy updated to:\n`{strategy}`", ephemeral=True)

@bot.tree.command(name="findmyedge", description="Premium: Get AI Analysis on your past trades")
async def find_my_edge(interaction: discord.Interaction):
    # 1. DEFER IMMEDIATELY ON LINE 1 (Prevents 3-second timeout / "The application did not respond")
    await interaction.response.defer(thinking=True)

    try:
        # 2. Check for the "Premium Trader" role across ANY mutual server the bot shares with you (works in DMs too)
        has_role = False
        for guild in bot.guilds:
            member = guild.get_member(interaction.user.id)
            if member and any(role.name == "Premium Trader" for role in member.roles):
                has_role = True
                break

        if not has_role:
            await interaction.followup.send("❌ You need the **Premium Trader** role in a shared server to run an AI edge report.", ephemeral=True)
            return

        # 3. Fetch completed trades from SQLite using your global user ID
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT result, analysis FROM journal WHERE user_id = ? AND status = ?", (interaction.user.id, "COMPLETED"))
        trades = c.fetchall()
        conn.close()

        if len(trades) < 3:
            await interaction.followup.send("⚠️ You need at least 3 completed trades (WIN or LOSS) before the AI can find patterns.", ephemeral=True)
            return

        # 4. Format data and call Gemini asynchronously
        trade_data = "\n".join([f"Result: {t[0]} | Analysis: {t[1]}" for t in trades])
        prompt = (
            "You are a quantitative trading psychologist. Analyze the following sequence of the user's trades. "
            "Identify patterns in their wins and losses. What is their edge? What mistakes are they repeating? "
            "Provide a concise, 3-point action plan to improve their strike rate.\n\n"
            f"Trades:\n{trade_data}"
        )

        response = await model.generate_content_async(prompt)
        
        embed = discord.Embed(
            title="🧠 Your AI Edge Report",
            description=response.text,
            color=discord.Color.purple()
        )
        embed.set_footer(text=f"Analyzed based on {len(trades)} completed trades.")

        # 5. Send output back via followup
        await interaction.followup.send(embed=embed)

    except Exception as e:
        await interaction.followup.send(f"❌ An error occurred: `{e}`", ephemeral=True)

if __name__ == "__main__":
    bot.run(TOKEN)
