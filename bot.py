import os
import logging
import uuid
import json
import asyncio
import time
from typing import Optional, Tuple, Dict, List
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv

from database import Database

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("DISCORD_TOKEN")
ADMIN_IDS_STR = os.getenv("ADMIN_ID")

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set in the environment.")

if not ADMIN_IDS_STR:
    raise RuntimeError("ADMIN_ID is not set in the environment.")

# Parse comma-separated admin IDs
try:
    ADMIN_IDS = set()
    for admin_id_str in ADMIN_IDS_STR.split(','):
        admin_id_str = admin_id_str.strip()
        if admin_id_str:
            ADMIN_IDS.add(int(admin_id_str))

    if not ADMIN_IDS:
        raise RuntimeError("At least one ADMIN_ID must be provided.")
except ValueError:
    raise RuntimeError("ADMIN_ID must contain valid integers (comma-separated for multiple admins).")

WEBHOOKS_PER_CHANNEL = 3

# Max attachment size to forward (8 MB). Larger files are skipped to prevent OOM.
MAX_ATTACHMENT_BYTES = 8 * 1024 * 1024

# How long (seconds) to keep last_author entries before pruning them.
LAST_AUTHOR_TTL = 600  # 10 minutes


class WebhookPoolManager:

    def __init__(self):
        # Key: webhook_url, Value: {'remaining': int, 'reset_at': float}
        self.rate_limits: Dict[str, Dict] = defaultdict(lambda: {'remaining': 5, 'reset_at': 0})

        # Key: channel_id, Value: int (index into webhook list)
        self.rr_index: Dict[int, int] = defaultdict(int)

    def update_rate_limit(self, webhook_url: str, remaining: int, reset_after: float):
        self.rate_limits[webhook_url] = {
            'remaining': remaining,
            'reset_at': time.time() + reset_after
        }

    def is_available(self, webhook_url: str) -> bool:
        info = self.rate_limits[webhook_url]

        # If reset time has passed, webhook is available
        if time.time() >= info['reset_at']:
            return True

        return info['remaining'] > 0

    def get_next_webhook(self, webhooks: list) -> Optional[Dict]:
        if not webhooks:
            return None

        channel_id = webhooks[0]['channel_id']
        num_webhooks = len(webhooks)

        # Try each webhook starting from current index
        for _ in range(num_webhooks):
            idx = self.rr_index[channel_id] % num_webhooks
            webhook = webhooks[idx]

            self.rr_index[channel_id] = (idx + 1) % num_webhooks

            if self.is_available(webhook['webhook_url']):
                return webhook

        return min(webhooks, key=lambda w: self.rate_limits[w['webhook_url']]['reset_at'])

    def mark_rate_limited(self, webhook_url: str, retry_after: float):
        self.rate_limits[webhook_url] = {
            'remaining': 0,
            'reset_at': time.time() + retry_after
        }

    def cleanup_rate_limits(self):
        """Remove expired rate limit entries to prevent unbounded dict growth."""
        now = time.time()
        # Keep entries that are still within their reset window + 60s grace period
        self.rate_limits = {
            k: v for k, v in self.rate_limits.items()
            if now < v['reset_at'] + 60
        }


class BridgeBot(commands.Bot):
    """Custom bot that owns the DB and HTTP session."""

    def __init__(self, *, intents: discord.Intents):
        # Limit the internal message cache to 100 per channel (default is 1000).
        super().__init__(command_prefix="!", intents=intents, max_messages=100)
        self.db = Database()
        self.http_session: Optional[aiohttp.ClientSession] = None
        self.webhook_pool = WebhookPoolManager()
        # channel_id -> {author_id, webhook_url, webhook_id, timestamp}
        self.last_author: Dict[int, Dict] = {}

    def prune_last_author(self):
        """Drop stale last_author entries to prevent unbounded dict growth."""
        cutoff = time.time() - LAST_AUTHOR_TTL
        self.last_author = {
            k: v for k, v in self.last_author.items()
            if v['timestamp'] >= cutoff
        }

    async def setup_hook(self) -> None:
        await self.db.connect()
        self.http_session = aiohttp.ClientSession()

        try:
            synced = await self.tree.sync()
            logger.info(f"Synced {len(synced)} command(s)")
        except Exception as e:
            logger.error(f"Failed to sync commands: {e}", exc_info=True)

    async def close(self) -> None:
        try:
            if self.http_session and not self.http_session.closed:
                await self.http_session.close()
        except Exception as e:
            logger.error(f"Error closing HTTP session: {e}", exc_info=True)

        try:
            await self.db.close()
        except Exception as e:
            logger.error(f"Error closing database: {e}", exc_info=True)

        await super().close()


intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True
intents.messages = True
intents.reactions = True

bot = BridgeBot(intents=intents)


def is_admin():

    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.user.id not in ADMIN_IDS:
            await interaction.response.send_message(
                "Only bot admins can use this command.",
                ephemeral=True,
            )
            return False
        return True

    return app_commands.check(predicate)


async def get_or_create_http_session() -> aiohttp.ClientSession:
    if bot.http_session is None or bot.http_session.closed:
        bot.http_session = aiohttp.ClientSession()
    return bot.http_session


async def create_webhook_pool(channel: discord.TextChannel, count: int = WEBHOOKS_PER_CHANNEL) -> list:
    created = []

    for i in range(count):
        try:
            webhook = await channel.create_webhook(name=f"Bridge Sync {i+1}")

            await bot.db.add_webhook_to_pool(
                channel_id=channel.id,
                webhook_id=str(webhook.id),
                webhook_url=webhook.url,
            )

            created.append({
                'webhook_id': str(webhook.id),
                'webhook_url': webhook.url,
                'channel_id': channel.id,
            })

            logger.info(f"Created webhook {i+1}/{count} for channel {channel.id}")

        except discord.Forbidden:
            logger.error(f"Missing permissions to create webhook {i+1} in channel {channel.id}")
            break
        except discord.HTTPException as e:
            logger.error(f"HTTP error creating webhook {i+1}: {e}")
            break

    return created


async def delete_webhook_pool(channel_id: int, session: aiohttp.ClientSession):
    webhooks = await bot.db.get_webhooks_for_channel(channel_id)

    for wh in webhooks:
        try:
            # Extract webhook ID and token from URL
            parts = wh['webhook_url'].rstrip('/').split('/')
            if len(parts) >= 2:
                webhook_id = parts[-2]
                webhook_token = parts[-1]
                delete_url = f"https://discord.com/api/webhooks/{webhook_id}/{webhook_token}"

                async with session.delete(delete_url) as resp:
                    if resp.status in (200, 204):
                        logger.info(f"Deleted webhook {wh['webhook_id']}")
                    elif resp.status == 404:
                        logger.warning(f"Webhook {wh['webhook_id']} already deleted")
                    else:
                        logger.error(f"Failed to delete webhook {wh['webhook_id']}: {resp.status}")
        except Exception as e:
            logger.error(f"Error deleting webhook {wh['webhook_id']}: {e}")

        # Remove from database regardless
        await bot.db.remove_webhook_from_pool(wh['webhook_id'])


async def webhook_request(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    max_retries: int = 3,
    **kwargs,
) -> Tuple[Optional[int], str, Optional[dict]]:
    for attempt in range(max_retries):
        try:
            async with session.request(method, url, **kwargs) as resp:
                status = resp.status

                # Parse rate limit headers
                remaining = resp.headers.get('X-RateLimit-Remaining')
                reset_after = resp.headers.get('X-RateLimit-Reset-After')

                if remaining is not None and reset_after is not None:
                    try:
                        bot.webhook_pool.update_rate_limit(
                            url.split('?')[0],  # Strip query params
                            int(remaining),
                            float(reset_after)
                        )
                    except (ValueError, TypeError):
                        pass

                if status == 429:
                    # Rate limited - wait and retry
                    try:
                        data = await resp.json()
                        retry_after = data.get("retry_after", 0.5)
                    except Exception:
                        retry_after = float(reset_after) if reset_after else 0.5

                    # Update pool manager
                    bot.webhook_pool.mark_rate_limited(url.split('?')[0], retry_after)

                    logger.warning(
                        f"Rate limited on {method} {url}, "
                        f"waiting {retry_after}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(retry_after + 0.1)  # Small buffer
                    continue

                # Success or other error
                text = await resp.text()

                # Try to parse JSON for successful responses
                json_data = None
                if status in (200, 204) and text:
                    try:
                        json_data = await resp.json()
                    except Exception:
                        pass

                return status, text, json_data

        except asyncio.TimeoutError:
            logger.warning(f"Timeout on {method} {url} (attempt {attempt + 1}/{max_retries})")
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Request error on {method} {url}: {e}")
            return None, str(e), None

    return None, "Max retries exceeded", None


# =======================
# Poll Helper Functions
# =======================

def generate_progress_bar(votes: int, total_votes: int) -> str:
    if total_votes == 0:
        percentage = 0
    else:
        percentage = (votes / total_votes) * 100

    # 10 blocks total
    filled_blocks = int(round(percentage / 10))
    filled_blocks = max(0, min(10, filled_blocks))  # Clamp between 0-10
    empty_blocks = 10 - filled_blocks

    bar = "🟦" * filled_blocks + "⬜" * empty_blocks
    return f"{bar} {percentage:.0f}% ({votes} votes)"


async def build_poll_embed(poll_id: str) -> Optional[discord.Embed]:
    try:
        poll = await bot.db.get_poll(poll_id)
        if not poll:
            return None

        options = await bot.db.get_poll_options(poll_id)
        vote_counts = await bot.db.get_poll_vote_counts(poll_id)
        total_votes = await bot.db.get_total_votes(poll_id)

        embed = discord.Embed(
            title=f"{poll['question']}",
            color=discord.Color.blue() if not poll['is_closed'] else discord.Color.grey(),
        )

        created_by_name = poll['created_by_name']
        embed.add_field(
            name="Created by",
            value=created_by_name,
            inline=True,
        )

        if isinstance(poll['expires_at'], str):
            expires_at = datetime.fromisoformat(poll['expires_at'])
        else:
            expires_at = poll['expires_at']

        expires_timestamp = int(expires_at.replace(tzinfo=timezone.utc).timestamp())

        if poll['is_closed']:
            embed.add_field(
                name="Status",
                value="🔒 Poll Ended",
                inline=True,
            )
        else:
            embed.add_field(
                name="Closes",
                value=f"<t:{expires_timestamp}:R>",
                inline=True,
            )

        embed.add_field(name="\u200b", value="\u200b", inline=False)  # Spacer

        for option in options:
            idx = option['option_index']
            text = option['option_text']
            votes = vote_counts.get(idx, 0)

            progress = generate_progress_bar(votes, total_votes)
            embed.add_field(
                name=text,
                value=progress,
                inline=False,
            )

        embed.set_footer(text=f"Total votes: {total_votes}")

        return embed

    except Exception as e:
        logger.error(f"Error building poll embed: {e}", exc_info=True)
        return None


def build_poll_buttons(poll_id: str, options: List[Dict], disabled: bool = False) -> List[discord.ui.Button]:
    from discord.ui import Button, View

    class PollView(View):
        def __init__(self):
            super().__init__(timeout=None)  # No timeout for persistent polls

            # Add a button for each option
            for option in options:
                button = Button(
                    label=option['option_text'],
                    style=discord.ButtonStyle.primary,
                    custom_id=f"poll_{poll_id}_{option['option_index']}",
                    disabled=disabled,
                )
                button.callback = self.create_vote_callback(poll_id, option['option_index'])
                self.add_item(button)

        def create_vote_callback(self, poll_id: str, option_index: int):
            async def vote_callback(interaction: discord.Interaction):
                await handle_poll_vote(interaction, poll_id, option_index)
            return vote_callback

    return PollView()


async def handle_poll_vote(interaction: discord.Interaction, poll_id: str, option_index: int):
    try:
        # Testing admin ID - can vote multiple times
        TESTING_ADMIN_ID = 55378962364243968

        # Check if poll exists and is open
        poll = await bot.db.get_poll(poll_id)
        if not poll:
            await interaction.response.send_message(
                "❌ This poll no longer exists.",
                ephemeral=True,
            )
            return

        if poll['is_closed']:
            await interaction.response.send_message(
                "❌ This poll has ended.",
                ephemeral=True,
            )
            return

        if isinstance(poll['expires_at'], str):
            expires_at = datetime.fromisoformat(poll['expires_at'])
        else:
            expires_at = poll['expires_at']

        if datetime.now(timezone.utc).replace(tzinfo=None) >= expires_at:
            await interaction.response.send_message(
                "This poll has expired.",
                ephemeral=True,
            )
            return

        vote_info = await bot.db.get_user_vote_info(poll_id, interaction.user.id)

        if interaction.user.id == TESTING_ADMIN_ID:
            try:
                await bot.db.db.execute(
                    """
                    INSERT INTO poll_votes (poll_id, user_id, option_index)
                    VALUES (?, ?, ?)
                    """,
                    (poll_id, interaction.user.id, option_index),
                )
                await bot.db.db.commit()
                success = True
                logger.info(f"Testing admin {interaction.user.id} voted for option {option_index} in poll {poll_id} (multiple votes allowed)")
            except Exception as e:
                logger.error(f"Error recording admin vote: {e}", exc_info=True)
                success = False
        else:
            if vote_info is not None:
                current_option = vote_info['option_index']
                vote_changes = vote_info['vote_changes']

                if current_option == option_index:
                    options = await bot.db.get_poll_options(poll_id)
                    voted_option = next((opt for opt in options if opt['option_index'] == option_index), None)
                    voted_text = voted_option['option_text'] if voted_option else f"Option {option_index + 1}"

                    await interaction.response.send_message(
                        f"✅ You've already voted for: **{voted_text}**",
                        ephemeral=True,
                    )
                    return

                if vote_changes >= 1:
                    options = await bot.db.get_poll_options(poll_id)
                    current_opt_obj = next((opt for opt in options if opt['option_index'] == current_option), None)
                    current_text = current_opt_obj['option_text'] if current_opt_obj else f"Option {current_option + 1}"

                    await interaction.response.send_message(
                        f"❌ You've already changed your vote once!\n"
                        f"Current vote: **{current_text}**\n\n"
                        f"You can only change your vote one time per poll.",
                        ephemeral=True,
                    )
                    return

                success = await bot.db.change_user_vote(poll_id, interaction.user.id, option_index)
                if success:
                    logger.info(f"User {interaction.user.id} changed vote in poll {poll_id} from option {current_option} to {option_index}")
                else:
                    logger.error(f"Failed to change vote for user {interaction.user.id} in poll {poll_id}")
            else:
                success = await bot.db.add_poll_vote(poll_id, interaction.user.id, option_index)
                if not success:
                    logger.error(f"Failed to record vote for user {interaction.user.id} in poll {poll_id}")

        if success:
            options = await bot.db.get_poll_options(poll_id)
            voted_option = next((opt for opt in options if opt['option_index'] == option_index), None)
            voted_text = voted_option['option_text'] if voted_option else f"Option {option_index + 1}"

            await interaction.response.send_message(
                f"Vote recorded for: **{voted_text}**",
                ephemeral=True,
            )

            await update_all_poll_messages(poll_id)
        else:
            await interaction.response.send_message(
                "Failed to record your vote. Please try again.",
                ephemeral=True,
            )

    except Exception as e:
        logger.error(f"Error handling poll vote: {e}", exc_info=True)
        try:
            await interaction.response.send_message(
                "An error occurred while processing your vote.",
                ephemeral=True,
            )
        except Exception:
            pass


async def update_all_poll_messages(poll_id: str):
    try:
        poll = await bot.db.get_poll(poll_id)
        if not poll:
            return

        embed = await build_poll_embed(poll_id)
        if not embed:
            return

        options = await bot.db.get_poll_options(poll_id)
        view = build_poll_buttons(poll_id, options, disabled=poll['is_closed'])

        poll_messages = await bot.db.get_poll_messages(poll_id)

        for msg_data in poll_messages:
            try:
                channel = bot.get_channel(msg_data['channel_id'])
                if not channel:
                    continue

                message = await channel.fetch_message(msg_data['message_id'])
                await message.edit(embed=embed, view=view)

                await asyncio.sleep(0.2)

            except discord.NotFound:
                logger.warning(f"Poll message {msg_data['message_id']} not found in channel {msg_data['channel_id']}")
            except discord.Forbidden:
                logger.warning(f"No permission to edit poll message in channel {msg_data['channel_id']}")
            except Exception as e:
                logger.error(f"Error updating poll message: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Error in update_all_poll_messages: {e}", exc_info=True)


@tasks.loop(hours=12)
async def cleanup_task():
    try:
        deleted = await bot.db.cleanup_old_mappings(max_rows=50000)
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old message mappings")
    except Exception as e:
        logger.error(f"Cleanup task failed: {e}", exc_info=True)


@cleanup_task.before_loop
async def before_cleanup():
    await bot.wait_until_ready()


@tasks.loop(minutes=5)
async def check_poll_expiry():
    try:
        expired_poll_ids = await bot.db.get_expired_polls()

        for poll_id in expired_poll_ids:
            logger.info(f"Closing expired poll: {poll_id}")
            await bot.db.close_poll(poll_id)
            await update_all_poll_messages(poll_id)

    except Exception as e:
        logger.error(f"Poll expiry check failed: {e}", exc_info=True)


@check_poll_expiry.before_loop
async def before_poll_expiry():
    await bot.wait_until_ready()


@tasks.loop(minutes=10)
async def memory_cleanup_task():
    """Periodically prune in-memory caches to prevent unbounded growth."""
    try:
        bot.prune_last_author()
        bot.webhook_pool.cleanup_rate_limits()
        logger.debug(
            f"Memory cleanup: last_author={len(bot.last_author)} entries, "
            f"rate_limits={len(bot.webhook_pool.rate_limits)} entries"
        )
    except Exception as e:
        logger.error(f"Memory cleanup task failed: {e}", exc_info=True)


@memory_cleanup_task.before_loop
async def before_memory_cleanup():
    await bot.wait_until_ready()


@bot.event
async def on_ready():
    """Bot is ready event."""
    logger.info(f"Bot logged in as {bot.user.name} (ID: {bot.user.id})")
    logger.info(f"Connected to {len(bot.guilds)} guild(s)")

    if not cleanup_task.is_running():
        cleanup_task.start()

    if not check_poll_expiry.is_running():
        check_poll_expiry.start()

    if not memory_cleanup_task.is_running():
        memory_cleanup_task.start()


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or message.webhook_id is not None:
        return

    if message.guild is None:
        return

    if await bot.db.is_user_muted(message.author.id):
        logger.info(f"Ignored message from muted user {message.author.id} ({message.author.name})")
        return

    asyncio.create_task(
        bot.db.touch_active_user(
            guild_id=message.guild.id,
            guild_name=message.guild.name,
            user_id=message.author.id,
            display_name=message.author.display_name,
            username=message.author.name,
        )
    )

    if message.content.startswith("!"):
        await bot.process_commands(message)
        return

    group_uid = await bot.db.get_sync_group_by_channel(message.channel.id)
    if not group_uid:
        await bot.process_commands(message)
        return

    channels = await bot.db.get_channels_in_group(group_uid)

    tasks = []
    for channel_data in channels:
        if channel_data["channel_id"] == message.channel.id:
            continue

        tasks.append(forward_message(message, channel_data, group_uid))

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                channel_data = [c for c in channels if c["channel_id"] != message.channel.id][i]
                logger.error(
                    f"Error forwarding to {channel_data.get('guild_name', 'Unknown guild')}: {result}",
                    exc_info=result,
                )

    await bot.process_commands(message)


@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if after.author.bot or after.webhook_id is not None:
        return

    if after.guild is None:
        return

    group_uid = await bot.db.get_sync_group_by_channel(after.channel.id)
    if not group_uid:
        return

    synced_messages = await bot.db.get_all_synced_for_message(
        after.id,
        after.channel.id,
    )

    session = await get_or_create_http_session()

    for entry in synced_messages:
        synced_channel_id = entry["synced_channel_id"]
        synced_msg_id = entry["synced_msg_id"]

        if synced_channel_id == after.channel.id and synced_msg_id == after.id:
            continue

        webhooks = await bot.db.get_webhooks_for_channel(synced_channel_id)
        if not webhooks:
            ch_record = await bot.db.get_channel_by_id(synced_channel_id)
            if not ch_record or not ch_record.get("webhook_url"):
                continue
            webhook_url = ch_record["webhook_url"]
        else:
            webhook_id = entry.get("webhook_id")
            webhook_url = None

            if webhook_id:
                webhook_data = await bot.db.get_webhook_by_id(webhook_id)
                if webhook_data:
                    webhook_url = webhook_data['webhook_url']

            if not webhook_url:
                webhook_url = webhooks[0]['webhook_url']

        edit_url = f"{webhook_url}/messages/{synced_msg_id}"

        status, text, _ = await webhook_request(
            session,
            "PATCH",
            edit_url,
            json={"content": after.content},
        )

        if status not in (200, 204, None):
            logger.error(
                f"Webhook edit failed for channel {synced_channel_id}, "
                f"msg {synced_msg_id}: {status} - {text}"
            )

        await asyncio.sleep(0.1)


@bot.event
async def on_raw_message_delete(payload: discord.RawMessageDeleteEvent):
    channel_id = payload.channel_id
    message_id = payload.message_id

    group_uid = await bot.db.get_sync_group_by_channel(channel_id)
    if not group_uid:
        return

    synced_messages = await bot.db.get_all_synced_for_message(
        message_id,
        channel_id,
    )

    session = await get_or_create_http_session()

    for entry in synced_messages:
        synced_channel_id = entry["synced_channel_id"]
        synced_msg_id = entry["synced_msg_id"]

        if synced_channel_id == channel_id and synced_msg_id == message_id:
            continue

        webhooks = await bot.db.get_webhooks_for_channel(synced_channel_id)
        if not webhooks:
            ch_record = await bot.db.get_channel_by_id(synced_channel_id)
            if not ch_record or not ch_record.get("webhook_url"):
                continue
            webhook_url = ch_record["webhook_url"]
        else:
            webhook_id = entry.get("webhook_id")
            webhook_url = None

            if webhook_id:
                webhook_data = await bot.db.get_webhook_by_id(webhook_id)
                if webhook_data:
                    webhook_url = webhook_data['webhook_url']

            if not webhook_url:
                webhook_url = webhooks[0]['webhook_url']

        delete_url = f"{webhook_url}/messages/{synced_msg_id}"

        status, text, _ = await webhook_request(session, "DELETE", delete_url)

        if status not in (200, 204, 404, None):
            logger.error(
                f"Webhook delete failed for channel {synced_channel_id}, "
                f"msg {synced_msg_id}: {status} - {text}"
            )

        await asyncio.sleep(0.1)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id:
        return

    group_uid = await bot.db.get_sync_group_by_channel(payload.channel_id)
    if not group_uid:
        return

    synced_messages = await bot.db.get_all_synced_for_message(
        payload.message_id,
        payload.channel_id,
    )

    for synced in synced_messages:
        try:
            channel = bot.get_channel(synced["synced_channel_id"])
            if channel is None:
                continue

            message = await channel.fetch_message(synced["synced_msg_id"])
            await message.add_reaction(payload.emoji)

            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error syncing reaction: {e}", exc_info=True)


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if bot.user and payload.user_id == bot.user.id:
        return

    group_uid = await bot.db.get_sync_group_by_channel(payload.channel_id)
    if not group_uid:
        return

    synced_messages = await bot.db.get_all_synced_for_message(
        payload.message_id,
        payload.channel_id,
    )

    for synced in synced_messages:
        try:
            channel = bot.get_channel(synced["synced_channel_id"])
            if channel is None:
                continue

            message = await channel.fetch_message(synced["synced_msg_id"])
            await message.remove_reaction(payload.emoji, bot.user)

            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error removing synced reaction: {e}", exc_info=True)


@bot.event
async def on_guild_remove(guild: discord.Guild):
    logger.info(f"Bot removed from guild: {guild.name} (ID: {guild.id})")
    await bot.db.remove_channel_by_guild(guild.id)


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if isinstance(channel, discord.TextChannel):
        group_uid = await bot.db.get_sync_group_by_channel(channel.id)
        if group_uid:
            logger.info(f"Sync channel deleted: {channel.guild.name} - #{channel.name}")
            await bot.db.remove_channel_from_group(channel.id)


@bot.event
async def on_webhooks_update(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.TextChannel):
        return

    group_uid = await bot.db.get_sync_group_by_channel(channel.id)
    if not group_uid:
        return

    stored_webhooks = await bot.db.get_webhooks_for_channel(channel.id)
    if not stored_webhooks:
        return

    try:
        current_webhooks = await channel.webhooks()
        current_urls = {wh.url for wh in current_webhooks}

        for stored in stored_webhooks:
            if stored['webhook_url'] not in current_urls:
                logger.warning(
                    f"Webhook {stored['webhook_id']} deleted from {channel.guild.name} - #{channel.name}"
                )
                await bot.db.remove_webhook_from_pool(stored['webhook_id'])

        remaining = await bot.db.get_webhook_count_for_channel(channel.id)
        if remaining == 0:
            logger.warning(
                f"All webhooks deleted for {channel.guild.name} - #{channel.name}; "
                f"removing from sync group {group_uid}"
            )
            await bot.db.remove_channel_from_group(channel.id)

    except discord.Forbidden:
        logger.error(
            f"Missing permissions to check webhooks in {channel.guild.name} - #{channel.name}",
        )


@bot.tree.command(
    name="setup",
    description="Set up this channel for multi-server sync",
)
@is_admin()
async def setup(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    channel = interaction.channel
    guild = interaction.guild

    if not isinstance(channel, discord.TextChannel) or guild is None:
        await interaction.followup.send(
            "This command can only be used in a server text channel.",
            ephemeral=True,
        )
        return

    existing_group = await bot.db.get_sync_group_by_channel(channel.id)
    if existing_group:
        await interaction.followup.send(
            f"This channel is already part of sync group: `{existing_group}`",
            ephemeral=True,
        )
        return

    uid = str(uuid.uuid4())[:8].upper()

    try:
        # Clean up any orphaned webhooks from previous failed attempts
        existing_webhooks = await bot.db.get_webhooks_for_channel(channel.id)
        if existing_webhooks:
            logger.warning(f"Found {len(existing_webhooks)} orphaned webhooks for channel {channel.id}, cleaning up")
            session = await get_or_create_http_session()
            await delete_webhook_pool(channel.id, session)

        created_webhooks = await create_webhook_pool(channel, WEBHOOKS_PER_CHANNEL)
        if not created_webhooks:
            await interaction.followup.send(
                "Missing permissions to create webhooks in this channel.",
                ephemeral=True,
            )
            return
    except Exception as e:
        logger.error(f"Error creating webhook pool: {e}", exc_info=True)
        await interaction.followup.send(
            f"Error creating webhooks: {str(e)}",
            ephemeral=True,
        )
        return

    await bot.db.create_sync_group(uid)
    success = await bot.db.add_channel_to_group(
        uid,
        channel.id,
        guild.id,
        guild.name,
        webhook_url=None,  # Using webhook pool now
    )

    if success:
        await interaction.followup.send(
            "Sync group created!\n\n"
            f"**Sync UID:** `{uid}`\n"
            f"**Webhooks created:** {len(created_webhooks)}\n\n"
            f"Use `/sync {uid}` in other channels to connect them to this sync group.",
            ephemeral=True,
        )
        logger.info(f"Created sync group {uid} in {guild.name} - #{channel.name} with {len(created_webhooks)} webhooks")
    else:
        await interaction.followup.send(
            "Failed to create sync group. Please try again.",
            ephemeral=True,
        )


@bot.tree.command(
    name="links",
    description="Show all existing sync groups and their linked channels",
)
@is_admin()
async def links(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    try:
        async with bot.db.db.execute("SELECT uid FROM sync_groups ORDER BY created_at") as cursor:
            groups = await cursor.fetchall()

        if not groups:
            await interaction.followup.send(
                "No sync groups exist yet.\n\n"
                "Use `/setup` to create your first sync group!",
                ephemeral=True,
            )
            return

        response_parts = ["**🔗 All Sync Groups:**\n"]

        for group_row in groups:
            group_uid = group_row[0]
            channels = await bot.db.get_channels_in_group(group_uid)

            response_parts.append(f"\n**Sync Group:** `{group_uid}`")
            response_parts.append(f"├─ **Channels ({len(channels)}):**")

            for i, ch in enumerate(channels):
                guild_name = ch["guild_name"]
                channel_id = ch["channel_id"]
                webhook_count = await bot.db.get_webhook_count_for_channel(channel_id)
                # Show custom name if set
                custom_name = await bot.db.get_guild_custom_name(ch["guild_id"])
                display_name = f"{guild_name} (as **{custom_name}**)" if custom_name else guild_name
                is_last = (i == len(channels) - 1)
                prefix = "└─" if is_last else "├─"
                response_parts.append(f"{prefix} {display_name} - <#{channel_id}> ({webhook_count} webhooks)")

        response = "\n".join(response_parts)

        # Split into multiple messages if too long (Discord has 2000 char limit)
        if len(response) > 1900:
            chunks = []
            current_chunk = ""
            for line in response_parts:
                if len(current_chunk) + len(line) + 1 > 1900:
                    chunks.append(current_chunk)
                    current_chunk = line + "\n"
                else:
                    current_chunk += line + "\n"
            if current_chunk:
                chunks.append(current_chunk)

            for i, chunk in enumerate(chunks):
                if i == 0:
                    await interaction.followup.send(chunk, ephemeral=True)
                else:
                    await interaction.followup.send(chunk, ephemeral=True)
        else:
            await interaction.followup.send(response, ephemeral=True)

    except Exception as e:
        logger.error(f"Error in /links command: {e}", exc_info=True)
        await interaction.followup.send(
            "An error occurred while fetching sync groups.",
            ephemeral=True,
        )


@bot.tree.command(
    name="unlink",
    description="Remove this channel from its sync group",
)
@is_admin()
async def unlink(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    channel = interaction.channel
    guild = interaction.guild

    if not isinstance(channel, discord.TextChannel) or guild is None:
        await interaction.followup.send(
            "This command can only be used in a server text channel.",
            ephemeral=True,
        )
        return

    group_uid = await bot.db.get_sync_group_by_channel(channel.id)
    if not group_uid:
        await interaction.followup.send(
            "This channel is not part of any sync group.",
            ephemeral=True,
        )
        return

    session = await get_or_create_http_session()

    await delete_webhook_pool(channel.id, session)

    channel_data = await bot.db.get_channel_by_id(channel.id)
    if channel_data and channel_data.get("webhook_url"):
        try:
            parts = channel_data["webhook_url"].rstrip('/').split('/')
            if len(parts) >= 2:
                webhook_id = parts[-2]
                webhook_token = parts[-1]
                delete_url = f"https://discord.com/api/webhooks/{webhook_id}/{webhook_token}"
                async with session.delete(delete_url) as resp:
                    pass  # Ignore result
        except Exception:
            pass

    await bot.db.remove_channel_from_group(channel.id)

    await interaction.followup.send(
        f"Channel removed from sync group `{group_uid}`.\n\n"
        "This channel is no longer syncing messages.",
        ephemeral=True,
    )
    logger.info(f"Removed {guild.name} - #{channel.name} from sync group {group_uid}")


@bot.tree.command(
    name="sync",
    description="Connect this channel to an existing sync group",
)
@app_commands.describe(uid="The sync group UID to join")
@is_admin()
async def sync(interaction: discord.Interaction, uid: str):
    await interaction.response.defer(ephemeral=True)

    channel = interaction.channel
    guild = interaction.guild

    if not isinstance(channel, discord.TextChannel) or guild is None:
        await interaction.followup.send(
            "This command can only be used in a server text channel.",
            ephemeral=True,
        )
        return

    uid = uid.upper().strip()

    # Check if sync group exists
    if not await bot.db.sync_group_exists(uid):
        await interaction.followup.send(
            f"Sync group `{uid}` does not exist.",
            ephemeral=True,
        )
        return

    # Check if channel is already in a sync group
    existing_group = await bot.db.get_sync_group_by_channel(channel.id)
    if existing_group:
        await interaction.followup.send(
            "This channel is already part of sync group: "
            f"`{existing_group}`\n"
            "Remove it first before joining a new group.",
            ephemeral=True,
        )
        return

    # Create webhook pool for this channel
    try:
        # Clean up any orphaned webhooks from previous failed attempts
        existing_webhooks = await bot.db.get_webhooks_for_channel(channel.id)
        if existing_webhooks:
            logger.warning(f"Found {len(existing_webhooks)} orphaned webhooks for channel {channel.id}, cleaning up")
            session = await get_or_create_http_session()
            await delete_webhook_pool(channel.id, session)

        created_webhooks = await create_webhook_pool(channel, WEBHOOKS_PER_CHANNEL)
        if not created_webhooks:
            await interaction.followup.send(
                "Missing permissions to create webhooks in this channel.",
                ephemeral=True,
            )
            return
    except Exception as e:
        logger.error(f"Error creating webhook pool: {e}", exc_info=True)
        await interaction.followup.send(
            f"Error creating webhooks: {str(e)}",
            ephemeral=True,
        )
        return

    success = await bot.db.add_channel_to_group(
        uid,
        channel.id,
        guild.id,
        guild.name,
        webhook_url=None,  # Using webhook pool now
    )

    if success:
        channels = await bot.db.get_channels_in_group(uid)
        await interaction.followup.send(
            f"Channel connected to sync group `{uid}`!\n\n"
            f"**Webhooks created:** {len(created_webhooks)}\n"
            f"Now syncing with {len(channels)} channel(s).",
            ephemeral=True,
        )
        logger.info(f"Added {guild.name} - #{channel.name} to sync group {uid} with {len(created_webhooks)} webhooks")
    else:
        existing_channel = await bot.db.get_channel_by_id(channel.id)
        group_exists = await bot.db.sync_group_exists(uid)
        logger.error(
            f"add_channel_to_group failed for channel {channel.id} in group {uid}. "
            f"Channel in DB: {existing_channel}, Group exists: {group_exists}"
        )

        # Clean up webhooks if adding to group failed
        session = await get_or_create_http_session()
        await delete_webhook_pool(channel.id, session)

        await interaction.followup.send(
            "❌ Failed to join sync group. The channel may already be registered. "
            "Try `/unlink` first, then try again.",
            ephemeral=True,
        )


@bot.tree.command(
    name="guildname",
    description="Set a custom display name for this server in bridged messages",
)
@app_commands.describe(
    name="The short name to display instead of the full server name (leave empty to reset)",
)
@is_admin()
async def guildname(interaction: discord.Interaction, name: str = None):
    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send(
            "This command can only be used inside a server.",
            ephemeral=True,
        )
        return

    # --- Reset / clear ---
    if not name or not name.strip():
        cleared = await bot.db.clear_guild_custom_name(guild.id)
        if cleared:
            await interaction.followup.send(
                f"✅ Custom name cleared. Bridged messages will now show **{guild.name}** again.",
                ephemeral=True,
            )
            logger.info(f"Guild custom name cleared for {guild.name} ({guild.id}) by {interaction.user}")
        else:
            await interaction.followup.send(
                "No custom name was set for this server.",
                ephemeral=True,
            )
        return

    name = name.strip()

    # Enforce a sensible length limit so the webhook username stays under Discord's 80-char cap.
    MAX_LEN = 40
    if len(name) > MAX_LEN:
        await interaction.followup.send(
            f"❌ Custom name is too long ({len(name)} chars). Please keep it under {MAX_LEN} characters.",
            ephemeral=True,
        )
        return

    success = await bot.db.set_guild_custom_name(guild.id, name, interaction.user.id)
    if success:
        await interaction.followup.send(
            f"✅ Done! Bridged messages from **{guild.name}** will now appear as **[{name}]**.",
            ephemeral=True,
        )
        logger.info(f"Guild custom name set to '{name}' for {guild.name} ({guild.id}) by {interaction.user}")
    else:
        await interaction.followup.send(
            "❌ Failed to save the custom name. Please try again.",
            ephemeral=True,
        )


@bot.tree.command(
    name="mute",
    description="Mute a user by ID - their messages won't be forwarded to other servers",
)
@app_commands.describe(
    user_id="The Discord user ID to mute",
    reason="Optional reason for muting this user"
)
@is_admin()
async def mute(interaction: discord.Interaction, user_id: str, reason: str = None):
    await interaction.response.defer(ephemeral=True)

    try:
        user_id_int = int(user_id)
    except ValueError:
        await interaction.followup.send(
            f"Invalid user ID: `{user_id}` is not a valid number.",
            ephemeral=True,
        )
        return

    if await bot.db.is_user_muted(user_id_int):
        await interaction.followup.send(
            f"User <@{user_id_int}> (`{user_id_int}`) is already muted.",
            ephemeral=True,
        )
        return

    success = await bot.db.mute_user(user_id_int, interaction.user.id, reason)

    if success:
        reason_text = f"\n**Reason:** {reason}" if reason else ""
        await interaction.followup.send(
            f"✅ Successfully muted user <@{user_id_int}> (`{user_id_int}`).{reason_text}\n\n"
            "Their messages will no longer be forwarded to other servers.",
            ephemeral=True,
        )
        logger.info(f"User {user_id_int} muted by {interaction.user.id} ({interaction.user.name})")
    else:
        await interaction.followup.send(
            "Failed to mute user. Please try again.",
            ephemeral=True,
        )


@bot.tree.command(
    name="unmute",
    description="Unmute a user by ID - their messages will be forwarded again",
)
@app_commands.describe(user_id="The Discord user ID to unmute")
@is_admin()
async def unmute(interaction: discord.Interaction, user_id: str):
    await interaction.response.defer(ephemeral=True)

    try:
        user_id_int = int(user_id)
    except ValueError:
        await interaction.followup.send(
            f"Invalid user ID: `{user_id}` is not a valid number.",
            ephemeral=True,
        )
        return

    if not await bot.db.is_user_muted(user_id_int):
        await interaction.followup.send(
            f"User <@{user_id_int}> (`{user_id_int}`) is not muted.",
            ephemeral=True,
        )
        return

    success = await bot.db.unmute_user(user_id_int)

    if success:
        await interaction.followup.send(
            f"✅ Successfully unmuted user <@{user_id_int}> (`{user_id_int}`).\n\n"
            "Their messages will now be forwarded to other servers again.",
            ephemeral=True,
        )
        logger.info(f"User {user_id_int} unmuted by {interaction.user.id} ({interaction.user.name})")
    else:
        await interaction.followup.send(
            "Failed to unmute user. Please try again.",
            ephemeral=True,
        )


@bot.tree.command(
    name="listmuted",
    description="Show all currently muted users",
)
@is_admin()
async def listmuted(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    try:
        muted_users = await bot.db.get_muted_users()

        if not muted_users:
            await interaction.followup.send(
                "No users are currently muted.",
                ephemeral=True,
            )
            return

        response_parts = [f"**🔇 Muted Users ({len(muted_users)}):**\n"]

        for user in muted_users:
            user_id = user["user_id"]
            muted_by = user["muted_by"]
            muted_at = user["muted_at"]
            reason = user.get("reason")

            response_parts.append(f"**User:** <@{user_id}> (`{user_id}`)")
            response_parts.append(f"├─ **Muted by:** <@{muted_by}>")
            response_parts.append(f"├─ **Muted at:** {muted_at}")

            if reason:
                response_parts.append(f"└─ **Reason:** {reason}")
            else:
                response_parts.append(f"└─ **Reason:** *No reason provided*")
            response_parts.append("")  # Empty line between entries

        response = "\n".join(response_parts)

        if len(response) > 1900:
            chunks = []
            current_chunk = ""
            for line in response_parts:
                if len(current_chunk) + len(line) + 1 > 1900:
                    chunks.append(current_chunk)
                    current_chunk = line + "\n"
                else:
                    current_chunk += line + "\n"
            if current_chunk:
                chunks.append(current_chunk)

            for i, chunk in enumerate(chunks):
                if i == 0:
                    await interaction.followup.send(chunk, ephemeral=True)
                else:
                    await interaction.followup.send(chunk, ephemeral=True)
        else:
            await interaction.followup.send(response, ephemeral=True)

    except Exception as e:
        logger.error(f"Error in /listmuted command: {e}", exc_info=True)
        await interaction.followup.send(
            "An error occurred while fetching muted users.",
            ephemeral=True,
        )


@bot.tree.command(
    name="poll",
    description="Create a poll that syncs across all servers in this sync group",
)
@app_commands.describe(
    question="The poll question",
    option1="First option",
    option2="Second option",
    option3="Third option (optional)",
    option4="Fourth option (optional)",
    option5="Fifth option (optional)",
    duration_hours="How many hours until the poll expires (default: 24)",
)
async def poll(
    interaction: discord.Interaction,
    question: str,
    option1: str,
    option2: str,
    option3: str = None,
    option4: str = None,
    option5: str = None,
    duration_hours: int = 24,
):
    await interaction.response.defer(ephemeral=True)

    channel = interaction.channel
    guild = interaction.guild

    if not isinstance(channel, discord.TextChannel) or guild is None:
        await interaction.followup.send(
            "This command can only be used in a server text channel.",
            ephemeral=True,
        )
        return

    group_uid = await bot.db.get_sync_group_by_channel(channel.id)
    if not group_uid:
        await interaction.followup.send(
            "This channel is not part of any sync group.\n"
            "Use `/setup` to create a sync group first.",
            ephemeral=True,
        )
        return

    options = [option1, option2]
    if option3:
        options.append(option3)
    if option4:
        options.append(option4)
    if option5:
        options.append(option5)

    if len(options) < 2:
        await interaction.followup.send(
            "❌ Polls must have at least 2 options.",
            ephemeral=True,
        )
        return

    if len(options) > 5:
        await interaction.followup.send(
            "❌ Polls can have a maximum of 5 options.",
            ephemeral=True,
        )
        return

    if duration_hours < 1 or duration_hours > 168:
        await interaction.followup.send(
            "❌ Poll duration must be between 1 and 168 hours (1 week).",
            ephemeral=True,
        )
        return

    try:
        poll_id = str(uuid.uuid4())[:8].upper()

        expires_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=duration_hours)

        success = await bot.db.create_poll(
            poll_id=poll_id,
            group_uid=group_uid,
            question=question,
            created_by=interaction.user.id,
            created_by_name=interaction.user.display_name,
            expires_at=expires_at,
            options=options,
        )

        if not success:
            await interaction.followup.send(
                "❌ Failed to create poll. Please try again.",
                ephemeral=True,
            )
            return

        embed = await build_poll_embed(poll_id)
        if not embed:
            await interaction.followup.send(
                "❌ Failed to build poll embed. Please try again.",
                ephemeral=True,
            )
            return

        poll_options = await bot.db.get_poll_options(poll_id)
        view = build_poll_buttons(poll_id, poll_options, disabled=False)

        channels = await bot.db.get_channels_in_group(group_uid)

        sent_count = 0
        for channel_data in channels:
            try:
                target_channel = bot.get_channel(channel_data['channel_id'])
                if not target_channel:
                    continue

                poll_message = await target_channel.send(embed=embed, view=view)

                await bot.db.add_poll_message(
                    poll_id=poll_id,
                    channel_id=target_channel.id,
                    message_id=poll_message.id,
                    guild_id=channel_data['guild_id'],
                )

                sent_count += 1
                logger.info(
                    f"Posted poll {poll_id} to {channel_data['guild_name']} - "
                    f"#{target_channel.name}"
                )

            except discord.Forbidden:
                logger.error(
                    f"Missing permissions to send poll to channel {channel_data['channel_id']}"
                )
            except Exception as e:
                logger.error(f"Error sending poll to channel: {e}", exc_info=True)

        if sent_count > 0:
            await interaction.followup.send(
                f"✅ Poll created successfully!\n\n"
                f"**Poll ID:** `{poll_id}`\n"
                f"**Question:** {question}\n"
                f"**Options:** {len(options)}\n"
                f"**Expires:** <t:{int(expires_at.timestamp())}:R>\n"
                f"**Posted to:** {sent_count} channel(s)",
                ephemeral=True,
            )
        else:
            await interaction.followup.send(
                "❌ Failed to post poll to any channels.",
                ephemeral=True,
            )

    except Exception as e:
        logger.error(f"Error creating poll: {e}", exc_info=True)
        await interaction.followup.send(
            "❌ An error occurred while creating the poll.",
            ephemeral=True,
        )


@bot.tree.command(
    name="bing",
    description="Search other synced servers for a user and get a mention snippet",
)
@app_commands.describe(query="Part of their name or nickname (in other servers)")
async def bing(interaction: discord.Interaction, query: str):
    channel = interaction.channel
    guild = interaction.guild

    if not isinstance(channel, discord.TextChannel) or guild is None:
        await interaction.response.send_message(
            "This command can only be used in a server text channel.",
            ephemeral=True,
        )
        return

    group_uid = await bot.db.get_sync_group_by_channel(channel.id)
    if not group_uid:
        await interaction.response.send_message(
            "This channel is not part of any sync group, so I can't look across servers.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)

    try:
        matches = await bot.db.search_active_users_in_group(
            group_uid=group_uid,
            exclude_guild_id=guild.id,
            query=query,
            max_age_days=20,
            limit=10,
        )
    except Exception as e:
        logger.error(f"/bing DB error: {e}", exc_info=True)
        await interaction.followup.send(
            "Something went wrong while searching. Please try again later.",
            ephemeral=True,
        )
        return

    if not matches:
        await interaction.followup.send(
            f"No recent speakers found matching `{query}` in *other* servers in this sync group.\n"
            f"(Only users who have talked in the last 20 days are remembered.)",
            ephemeral=True,
        )
        return

    lines = []
    for row in matches:
        display_name = row["display_name"]
        username = row["username"]
        guild_name = row["guild_name"]
        user_id = row["user_id"]
        lines.append(
            f"{display_name} ({username}) @ {guild_name} → <@{user_id}>"
        )

    body = "\n".join(lines)

    await interaction.followup.send(
        "Here are the closest matches **from other servers (recent speakers)**:\n\n"
        "```text\n"
        f"{body}\n"
        "```\n"
        "Copy the `<@id>` for the person you want, and paste it into your message so it looks like:\n"
        "```text\n"
        "<@id> your message here\n"
        "```",
        ephemeral=True,
    )


async def forward_message(
    message: discord.Message,
    target_channel_data: dict,
    group_uid: str,
) -> None:
    session = await get_or_create_http_session()
    target_channel_id = target_channel_data["channel_id"]

    webhooks = await bot.db.get_webhooks_for_channel(target_channel_id)

    used_webhook_id = None  # Track which webhook we use

    if not webhooks:
        webhook_url = target_channel_data.get("webhook_url")
        if not webhook_url:
            logger.warning(f"No webhooks available for channel {target_channel_id}")
            return
        parts = webhook_url.rstrip('/').split('/')
        if len(parts) >= 2:
            used_webhook_id = parts[-2]
    else:
        # Check if we should reuse the same webhook for visual message grouping
        last_author_info = bot.last_author.get(target_channel_id)
        should_reuse_webhook = (
            last_author_info is not None
            and last_author_info["author_id"] == message.author.id
            and (time.time() - last_author_info["timestamp"]) < LAST_AUTHOR_TTL
        )

        if should_reuse_webhook:
            webhook_url = last_author_info["webhook_url"]
            used_webhook_id = last_author_info["webhook_id"]
        else:
            webhook = bot.webhook_pool.get_next_webhook(webhooks)
            webhook_url = webhook['webhook_url']
            used_webhook_id = webhook['webhook_id']

    # Resolve display name
    display_name = message.author.display_name
    guild_id = message.guild.id if message.guild else None
    real_guild_name = message.guild.name if message.guild else "Unknown Server"

    if guild_id:
        custom_name = await bot.db.get_guild_custom_name(guild_id)
        server_label = custom_name if custom_name else real_guild_name
    else:
        server_label = real_guild_name

    username = f'[{server_label}] {display_name}'
    avatar_url = message.author.display_avatar.url
    content = message.content

    # DO NOT forward auto-generated embeds (so rich previews re-generate)
    embeds = []

    # Download attachments, skipping files that exceed the size cap.
    files_data = []
    for attachment in message.attachments:
        if attachment.size > MAX_ATTACHMENT_BYTES:
            logger.warning(
                f"Skipping large attachment '{attachment.filename}' "
                f"({attachment.size / 1024 / 1024:.1f} MB > "
                f"{MAX_ATTACHMENT_BYTES / 1024 / 1024:.0f} MB limit)"
            )
            # Append a note in the message so recipients know a file was skipped.
            size_mb = attachment.size / 1024 / 1024
            content = (content or "") + f"\n-# 📎 *[{attachment.filename} ({size_mb:.1f} MB) not forwarded — too large]*"
            continue

        try:
            async with session.get(attachment.url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    files_data.append(
                        {
                            "name": attachment.filename,
                            "data": data,
                        }
                    )
                    # Explicitly release reference after appending; the list
                    # holds the only remaining reference until we POST and clear.
                    del data
                else:
                    logger.error(
                        f"Failed to download attachment {attachment.filename}: HTTP {resp.status}"
                    )
        except Exception as e:
            logger.error(f"Error downloading attachment: {e}", exc_info=True)

    # Download stickers as images
    for sticker in message.stickers:
        try:
            sticker_url = f"https://media.discordapp.net/stickers/{sticker.id}.png?size=160"
            logger.info(f"Downloading sticker: {sticker.name} (ID: {sticker.id}) from {sticker_url}")

            async with session.get(sticker_url) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    files_data.append({
                        "name": f"{sticker.name}.png",
                        "data": data,
                    })
                    del data
                    logger.info(f"Successfully downloaded sticker {sticker.name}")
                else:
                    logger.warning(f"Failed to download sticker {sticker.name}: HTTP {resp.status}")
        except Exception as e:
            logger.error(f"Error downloading sticker {sticker.name}: {e}", exc_info=True)

    components = None

    if message.reference and message.reference.message_id:
        try:
            logger.info(f"Processing reply to message {message.reference.message_id}")

            original_channel = bot.get_channel(message.reference.channel_id)
            if original_channel is None:
                logger.warning("Original channel not found for reply reference")
            else:
                try:
                    original_message = await original_channel.fetch_message(
                        message.reference.message_id
                    )
                except discord.NotFound:
                    logger.warning(f"Original message {message.reference.message_id} was deleted")
                    original_message = None

                if original_message:
                    author_name = original_message.author.display_name

                    preview_raw = (original_message.content or "").replace("\n", " ").strip()
                    max_preview_len = 75
                    preview_trimmed = preview_raw[:max_preview_len]
                    if len(preview_raw) > max_preview_len:
                        preview_trimmed += "..."

                    logger.info(f"Reply to {author_name}: {preview_trimmed or '[no text]'}")

                    synced_messages = await bot.db.get_all_synced_for_message(
                        message.reference.message_id,
                        message.reference.channel_id,
                    )
                    logger.info(
                        f"Found {len(synced_messages)} synced messages for reply lookup"
                    )

                    target_msg_id = None
                    for synced in synced_messages:
                        if synced["synced_channel_id"] == target_channel_data["channel_id"]:
                            target_msg_id = synced["synced_msg_id"]
                            break

                    if target_msg_id is not None:
                        guild_id_str = str(target_channel_data["guild_id"])
                        channel_id_str = str(target_channel_data["channel_id"])
                        msg_id_str = str(target_msg_id)

                        discord_url = (
                            f"https://discord.com/channels/"
                            f"{guild_id_str}/{channel_id_str}/{msg_id_str}"
                        )
                        logger.info(f"Reply button URL: {discord_url}")

                        buttons = []

                        buttons.append(
                            {
                                "type": 2,
                                "style": 2,
                                "label": f"↩️ {author_name[:70]}",
                                "disabled": True,
                                "custom_id": "reply_author_disabled",
                            }
                        )

                        if preview_trimmed:
                            buttons.append(
                                {
                                    "type": 2,
                                    "style": 5,
                                    "label": preview_trimmed,
                                    "url": discord_url,
                                }
                            )
                        else:
                            buttons.append(
                                {
                                    "type": 2,
                                    "style": 5,
                                    "label": "View message",
                                    "url": discord_url,
                                }
                            )

                        components = [
                            {
                                "type": 1,
                                "components": buttons,
                            }
                        ]
                    else:
                        logger.warning(
                            f"No synced message found for reply in target channel {target_channel_data['channel_id']} "
                            f"(guild: {target_channel_data['guild_name']}). "
                            f"Available synced channels: {[s['synced_channel_id'] for s in synced_messages]}"
                        )
                        content = f"-# ↩️ *replying to an older message*\n{content}"

        except Exception as e:
            logger.error(f"Error creating reply buttons: {e}", exc_info=True)

    payload = {
        "username": username,
        "avatar_url": avatar_url,
        "allowed_mentions": {
            "parse": ["users", "roles"]
        }
    }
    if content:
        payload["content"] = content
    if embeds:
        payload["embeds"] = embeds
    if components:
        payload["components"] = components

    if not content and not files_data and not embeds:
        logger.warning(f"Skipping empty message from {message.author} - nothing to forward")
        return

    try:
        if files_data:
            form = aiohttp.FormData()
            form.add_field("payload_json", json.dumps(payload))

            for i, f in enumerate(files_data):
                form.add_field(
                    f"file{i}",
                    f["data"],
                    filename=f["name"],
                )

            status, text, response_data = await webhook_request(
                session,
                "POST",
                f"{webhook_url}?wait=true",
                data=form,
            )

            if status not in (200, 204):
                logger.error(f"Webhook error (multipart): {status} - {text}")
                return
        else:
            status, text, response_data = await webhook_request(
                session,
                "POST",
                f"{webhook_url}?wait=true",
                json=payload,
                headers={"Content-Type": "application/json"},
            )

            if status not in (200, 204):
                logger.error(f"Webhook error: {status} - {text}")
                return

        if response_data:
            sent_msg_id = int(response_data["id"])

            # Update last author tracking for message grouping
            bot.last_author[target_channel_id] = {
                "author_id": message.author.id,
                "webhook_url": webhook_url,
                "webhook_id": used_webhook_id,
                "timestamp": time.time(),
            }

            await bot.db.add_message_mapping(
                message.id,
                message.channel.id,
                sent_msg_id,
                target_channel_data["channel_id"],
                group_uid,
                webhook_id=used_webhook_id,
            )

    except Exception as e:
        logger.error(f"Error sending webhook message: {e}", exc_info=True)
    finally:
        # Explicitly free attachment data after the POST regardless of outcome.
        files_data.clear()


async def main():
    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())