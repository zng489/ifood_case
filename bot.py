import discord
from discord.ext import commands

# AQUI VOCÊ “HARD‑CODES” O TOKEN
TOKEN = "MTM2Mzg3Nzk1MjAyMTk4NzQ2MA.GRv8ht.pwFcu7PTUYGe0jv85arKDGgv0JObzp3kY5xef8"

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f"✅ Bot conectado como {bot.user}")

@bot.command()
async def limpar_bot(ctx, limite: int = 50):
    """Deleta mensagens de bots (inclusive webhooks)"""
    def eh_bot(msg):
        return msg.author.bot

    deletadas = await ctx.channel.purge(limit=limite, check=eh_bot)
    await ctx.send(f"🧹 Apaguei {len(deletadas)} mensagens de bot.", delete_after=5)

if __name__ == "__main__":
    bot.run(TOKEN)
