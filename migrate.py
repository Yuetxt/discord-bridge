"""
Migration script to upgrade existing sync channels to use webhook pools.

This script:
1. Finds all channels with a single webhook_url in sync_channels
2. Creates 2 additional webhooks for each (bringing total to 3)
3. Migrates existing webhook to the new sync_webhooks table

Run this once after upgrading to the webhook pool version.

Usage:
    python migrate_to_webhook_pool.py
"""

import asyncio
import os
import logging
import aiohttp
import discord
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("DISCORD_TOKEN")
WEBHOOKS_PER_CHANNEL = 3


async def migrate():
    """Run the migration."""
    
    # Import after setting up logging
    from database import Database
    
    db = Database()
    await db.connect()
    
    # Create Discord client for webhook creation
    intents = discord.Intents.default()
    client = discord.Client(intents=intents)
    
    @client.event
    async def on_ready():
        logger.info(f"Logged in as {client.user}")
        
        try:
            # Get all channels with legacy webhook_url
            async with db.db.execute(
                "SELECT channel_id, guild_id, guild_name, webhook_url FROM sync_channels WHERE webhook_url IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
            
            if not rows:
                logger.info("No channels with legacy webhooks found. Nothing to migrate.")
                await client.close()
                return
            
            logger.info(f"Found {len(rows)} channels to migrate")
            
            for row in rows:
                channel_id = row[0]
                guild_id = row[1]
                guild_name = row[2]
                legacy_webhook_url = row[3]
                
                logger.info(f"Migrating channel {channel_id} in {guild_name}...")
                
                # Check if already migrated
                existing_webhooks = await db.get_webhooks_for_channel(channel_id)
                if existing_webhooks:
                    logger.info(f"  Channel already has {len(existing_webhooks)} webhooks in pool, skipping")
                    continue
                
                # Get the Discord channel
                channel = client.get_channel(channel_id)
                if channel is None:
                    try:
                        channel = await client.fetch_channel(channel_id)
                    except discord.NotFound:
                        logger.warning(f"  Channel {channel_id} not found, skipping")
                        continue
                    except discord.Forbidden:
                        logger.warning(f"  No access to channel {channel_id}, skipping")
                        continue
                
                if not isinstance(channel, discord.TextChannel):
                    logger.warning(f"  Channel {channel_id} is not a text channel, skipping")
                    continue
                
                # Extract webhook ID from legacy URL
                parts = legacy_webhook_url.rstrip('/').split('/')
                if len(parts) >= 2:
                    legacy_webhook_id = parts[-2]
                    
                    # Add legacy webhook to pool
                    await db.add_webhook_to_pool(
                        channel_id=channel_id,
                        webhook_id=legacy_webhook_id,
                        webhook_url=legacy_webhook_url,
                    )
                    logger.info(f"  Added legacy webhook {legacy_webhook_id} to pool")
                
                # Create additional webhooks
                webhooks_to_create = WEBHOOKS_PER_CHANNEL - 1  # -1 because we already have one
                
                for i in range(webhooks_to_create):
                    try:
                        webhook = await channel.create_webhook(name=f"Bridge Sync {i+2}")
                        await db.add_webhook_to_pool(
                            channel_id=channel_id,
                            webhook_id=str(webhook.id),
                            webhook_url=webhook.url,
                        )
                        logger.info(f"  Created webhook {i+2}/{WEBHOOKS_PER_CHANNEL}")
                    except discord.Forbidden:
                        logger.error(f"  Missing permissions to create webhook in channel {channel_id}")
                        break
                    except discord.HTTPException as e:
                        logger.error(f"  HTTP error creating webhook: {e}")
                        break
                
                # Clear legacy webhook_url from sync_channels (optional, for cleanliness)
                # We keep it for backward compatibility, but you can uncomment to clear:
                # await db.db.execute(
                #     "UPDATE sync_channels SET webhook_url = NULL WHERE channel_id = ?",
                #     (channel_id,)
                # )
                # await db.db.commit()
                
                final_count = await db.get_webhook_count_for_channel(channel_id)
                logger.info(f"  Migration complete: {final_count} webhooks in pool")
                
                # Small delay to avoid rate limits
                await asyncio.sleep(1)
            
            logger.info("Migration complete!")
            
        except Exception as e:
            logger.error(f"Migration failed: {e}", exc_info=True)
        
        finally:
            await db.close()
            await client.close()
    
    await client.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(migrate())