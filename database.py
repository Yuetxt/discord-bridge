import aiosqlite
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path: str = "sync_data.db"):
        self.db_path = db_path
        self.db: Optional[aiosqlite.Connection] = None

    async def connect(self):
        """Initialize database connection and create tables"""
        self.db = await aiosqlite.connect(self.db_path)
        self.db.row_factory = aiosqlite.Row
        await self.create_tables()

    async def create_tables(self):
        """Create database tables if they don't exist"""
        # Sync groups
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS sync_groups (
                uid TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Channels in each sync group
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS sync_channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_uid TEXT NOT NULL,
                channel_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                guild_name TEXT NOT NULL,
                webhook_url TEXT,
                FOREIGN KEY (group_uid) REFERENCES sync_groups(uid) ON DELETE CASCADE,
                UNIQUE(channel_id)
            )
        """)

        # Webhook pool for each channel (NEW)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS sync_webhooks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                webhook_id TEXT NOT NULL,
                webhook_url TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(webhook_id)
            )
        """)

        # Message mappings (for replies/reactions/edits/deletes)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS message_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_msg_id INTEGER NOT NULL,
                original_channel_id INTEGER NOT NULL,
                synced_msg_id INTEGER NOT NULL,
                synced_channel_id INTEGER NOT NULL,
                group_uid TEXT NOT NULL,
                webhook_id TEXT,
                FOREIGN KEY (group_uid) REFERENCES sync_groups(uid) ON DELETE CASCADE
            )
        """)

        # Recent active users across guilds (for /bing)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS active_users (
                guild_id     INTEGER NOT NULL,
                guild_name   TEXT    NOT NULL,
                user_id      INTEGER NOT NULL,
                display_name TEXT    NOT NULL,
                username     TEXT    NOT NULL,
                last_seen    INTEGER NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            )
        """)

        # Muted users (messages from these users won't be forwarded)
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS muted_users (
                user_id INTEGER PRIMARY KEY,
                muted_by INTEGER NOT NULL,
                muted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                reason TEXT
            )
        """)

        # Custom guild display names for bridge messages
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS guild_custom_names (
                guild_id   INTEGER PRIMARY KEY,
                custom_name TEXT NOT NULL,
                set_by     INTEGER NOT NULL,
                set_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Polls - stores poll metadata
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS polls (
                poll_id TEXT PRIMARY KEY,
                group_uid TEXT NOT NULL,
                question TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                created_by_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP NOT NULL,
                is_closed BOOLEAN DEFAULT 0,
                FOREIGN KEY (group_uid) REFERENCES sync_groups(uid) ON DELETE CASCADE
            )
        """)

        # Poll options - stores answer choices
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS poll_options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_id TEXT NOT NULL,
                option_text TEXT NOT NULL,
                option_index INTEGER NOT NULL,
                FOREIGN KEY (poll_id) REFERENCES polls(poll_id) ON DELETE CASCADE,
                UNIQUE(poll_id, option_index)
            )
        """)

        # Poll votes - tracks who voted for what
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS poll_votes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                option_index INTEGER NOT NULL,
                voted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                vote_changes INTEGER DEFAULT 0,
                FOREIGN KEY (poll_id) REFERENCES polls(poll_id) ON DELETE CASCADE
            )
        """)
        
        # Create index for faster lookups (but no unique constraint)
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_poll_votes_user 
            ON poll_votes(poll_id, user_id)
        """)
        
        # Migration: Add vote_changes column if it doesn't exist
        try:
            await self.db.execute("ALTER TABLE poll_votes ADD COLUMN vote_changes INTEGER DEFAULT 0")
            logger.info("Added vote_changes column to poll_votes table")
        except Exception:
            pass  # Column already exists

        # Poll messages - tracks poll instances across servers
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS poll_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                poll_id TEXT NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                FOREIGN KEY (poll_id) REFERENCES polls(poll_id) ON DELETE CASCADE
            )
        """)

        # Create indexes for better query performance
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_message_mappings_original 
            ON message_mappings(original_msg_id, original_channel_id)
        """)
        
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_message_mappings_synced 
            ON message_mappings(synced_msg_id, synced_channel_id)
        """)
        
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sync_channels_channel 
            ON sync_channels(channel_id)
        """)
        
        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_active_users_last_seen 
            ON active_users(last_seen)
        """)

        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_sync_webhooks_channel 
            ON sync_webhooks(channel_id)
        """)

        # Migration: Add webhook_id column if it doesn't exist (for existing databases)
        try:
            await self.db.execute("ALTER TABLE message_mappings ADD COLUMN webhook_id TEXT")
            logger.info("Added webhook_id column to message_mappings table")
        except Exception:
            pass  # Column already exists

        # Migration: Make webhook_url nullable (for webhook pool support)
        # SQLite doesn't support ALTER COLUMN, so we need to recreate the table
        try:
            # Check if webhook_url has NOT NULL constraint by trying to insert NULL
            await self.db.execute("""
                CREATE TABLE IF NOT EXISTS sync_channels_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_uid TEXT NOT NULL,
                    channel_id INTEGER NOT NULL,
                    guild_id INTEGER NOT NULL,
                    guild_name TEXT NOT NULL,
                    webhook_url TEXT,
                    FOREIGN KEY (group_uid) REFERENCES sync_groups(uid) ON DELETE CASCADE,
                    UNIQUE(channel_id)
                )
            """)
            
            # Check if old table exists and has data
            async with self.db.execute("SELECT COUNT(*) FROM sync_channels") as cursor:
                row = await cursor.fetchone()
                if row and row[0] > 0:
                    # Copy data
                    await self.db.execute("INSERT OR IGNORE INTO sync_channels_new SELECT * FROM sync_channels")
                    await self.db.execute("DROP TABLE sync_channels")
                    await self.db.execute("ALTER TABLE sync_channels_new RENAME TO sync_channels")
                    await self.db.execute("CREATE INDEX IF NOT EXISTS idx_sync_channels_channel ON sync_channels(channel_id)")
                    logger.info("Migrated sync_channels table to allow NULL webhook_url")
                else:
                    # No data, just drop the new table - original create will handle it
                    await self.db.execute("DROP TABLE IF EXISTS sync_channels_new")
        except Exception as e:
            # Table might not exist yet or migration already done
            try:
                await self.db.execute("DROP TABLE IF EXISTS sync_channels_new")
            except Exception:
                pass

        await self.db.commit()
        logger.info("Database tables created successfully")

    # =======================
    # Sync groups / channels
    # =======================

    async def create_sync_group(self, uid: str) -> bool:
        """Create a new sync group"""
        try:
            await self.db.execute(
                "INSERT INTO sync_groups (uid) VALUES (?)",
                (uid,)
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def add_channel_to_group(
        self,
        uid: str,
        channel_id: int,
        guild_id: int,
        guild_name: str,
        webhook_url: str = None,  # Now optional, kept for backward compatibility
    ) -> bool:
        """Add a channel to a sync group"""
        try:
            await self.db.execute(
                """
                INSERT INTO sync_channels 
                    (group_uid, channel_id, guild_id, guild_name, webhook_url) 
                VALUES (?, ?, ?, ?, ?)
                """,
                (uid, channel_id, guild_id, guild_name, webhook_url),
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError as e:
            logger.error(f"IntegrityError adding channel {channel_id} to group {uid}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error adding channel {channel_id} to group {uid}: {e}")
            return False

    async def get_sync_group_by_channel(self, channel_id: int) -> Optional[str]:
        """Get the sync group UID for a channel"""
        async with self.db.execute(
            "SELECT group_uid FROM sync_channels WHERE channel_id = ?",
            (channel_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def get_channels_in_group(self, uid: str) -> List[Dict]:
        """Get all channels in a sync group"""
        async with self.db.execute(
            """
            SELECT channel_id, guild_id, guild_name, webhook_url 
            FROM sync_channels WHERE group_uid = ?
            """,
            (uid,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_channel_by_id(self, channel_id: int) -> Optional[Dict]:
        """Get a sync_channels row by channel_id (for webhook URL, etc.)"""
        async with self.db.execute(
            """
            SELECT group_uid, channel_id, guild_id, guild_name, webhook_url
            FROM sync_channels
            WHERE channel_id = ?
            """,
            (channel_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def remove_channel_from_group(self, channel_id: int):
        """Remove a channel from its sync group and its webhooks"""
        # First remove all webhooks for this channel
        await self.db.execute(
            "DELETE FROM sync_webhooks WHERE channel_id = ?",
            (channel_id,),
        )
        # Then remove the channel
        await self.db.execute(
            "DELETE FROM sync_channels WHERE channel_id = ?",
            (channel_id,),
        )
        await self.db.commit()

    async def remove_channel_by_guild(self, guild_id: int):
        """Remove all channels from a guild (when bot is kicked)"""
        # First get all channel IDs for this guild
        async with self.db.execute(
            "SELECT channel_id FROM sync_channels WHERE guild_id = ?",
            (guild_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            channel_ids = [row[0] for row in rows]

        # Remove webhooks for all these channels
        for channel_id in channel_ids:
            await self.db.execute(
                "DELETE FROM sync_webhooks WHERE channel_id = ?",
                (channel_id,),
            )

        # Remove the channels
        await self.db.execute(
            "DELETE FROM sync_channels WHERE guild_id = ?",
            (guild_id,),
        )
        await self.db.commit()

    async def sync_group_exists(self, uid: str) -> bool:
        """Check if a sync group exists"""
        async with self.db.execute(
            "SELECT 1 FROM sync_groups WHERE uid = ?",
            (uid,),
        ) as cursor:
            row = await cursor.fetchone()
            exists = row is not None
            logger.debug(f"sync_group_exists({uid}) = {exists}")
            return exists

    # =======================
    # Webhook pool management
    # =======================

    async def add_webhook_to_pool(
        self,
        channel_id: int,
        webhook_id: str,
        webhook_url: str,
    ) -> bool:
        """Add a webhook to a channel's pool"""
        try:
            await self.db.execute(
                """
                INSERT INTO sync_webhooks (channel_id, webhook_id, webhook_url)
                VALUES (?, ?, ?)
                """,
                (channel_id, webhook_id, webhook_url),
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def get_webhooks_for_channel(self, channel_id: int) -> List[Dict]:
        """Get all webhooks in a channel's pool"""
        async with self.db.execute(
            """
            SELECT id, channel_id, webhook_id, webhook_url
            FROM sync_webhooks
            WHERE channel_id = ?
            ORDER BY id
            """,
            (channel_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def remove_webhook_from_pool(self, webhook_id: str):
        """Remove a specific webhook from the pool"""
        await self.db.execute(
            "DELETE FROM sync_webhooks WHERE webhook_id = ?",
            (webhook_id,),
        )
        await self.db.commit()

    async def remove_webhook_by_url(self, webhook_url: str):
        """Remove a webhook by its URL"""
        await self.db.execute(
            "DELETE FROM sync_webhooks WHERE webhook_url = ?",
            (webhook_url,),
        )
        await self.db.commit()

    async def get_webhook_count_for_channel(self, channel_id: int) -> int:
        """Get the number of webhooks in a channel's pool"""
        async with self.db.execute(
            "SELECT COUNT(*) FROM sync_webhooks WHERE channel_id = ?",
            (channel_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_webhook_by_id(self, webhook_id: str) -> Optional[Dict]:
        """Get a webhook by its ID"""
        async with self.db.execute(
            """
            SELECT id, channel_id, webhook_id, webhook_url
            FROM sync_webhooks
            WHERE webhook_id = ?
            """,
            (webhook_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    # =======================
    # Guild custom names
    # =======================

    async def set_guild_custom_name(
        self, guild_id: int, custom_name: str, set_by: int
    ) -> bool:
        """Insert or update the custom display name for a guild."""
        try:
            await self.db.execute(
                """
                INSERT INTO guild_custom_names (guild_id, custom_name, set_by)
                VALUES (?, ?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    custom_name = excluded.custom_name,
                    set_by      = excluded.set_by,
                    set_at      = CURRENT_TIMESTAMP
                """,
                (guild_id, custom_name, set_by),
            )
            await self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Error setting guild custom name: {e}")
            return False

    async def get_guild_custom_name(self, guild_id: int) -> Optional[str]:
        """Return the custom display name for a guild, or None if not set."""
        async with self.db.execute(
            "SELECT custom_name FROM guild_custom_names WHERE guild_id = ?",
            (guild_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None

    async def clear_guild_custom_name(self, guild_id: int) -> bool:
        """Remove the custom display name for a guild (revert to real name)."""
        cursor = await self.db.execute(
            "DELETE FROM guild_custom_names WHERE guild_id = ?",
            (guild_id,),
        )
        await self.db.commit()
        return cursor.rowcount > 0

    # =======================
    # Message mappings
    # =======================

    async def add_message_mapping(
        self,
        original_msg_id: int,
        original_channel_id: int,
        synced_msg_id: int,
        synced_channel_id: int,
        group_uid: str,
        webhook_id: str = None,
    ):
        """Store a message mapping for replies and reactions"""
        await self.db.execute(
            """
            INSERT INTO message_mappings 
                (original_msg_id, original_channel_id, synced_msg_id, synced_channel_id, group_uid, webhook_id)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (original_msg_id, original_channel_id, synced_msg_id, synced_channel_id, group_uid, webhook_id),
        )
        await self.db.commit()

    async def get_synced_messages(
        self,
        original_msg_id: int,
        original_channel_id: int,
    ) -> List[Dict]:
        """Get all synced copies of a message"""
        async with self.db.execute(
            """
            SELECT synced_msg_id, synced_channel_id, webhook_id 
            FROM message_mappings 
            WHERE original_msg_id = ? AND original_channel_id = ?
            """,
            (original_msg_id, original_channel_id),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def get_original_message(
        self,
        synced_msg_id: int,
        synced_channel_id: int,
    ) -> Optional[Dict]:
        """Get the original message for a synced message"""
        async with self.db.execute(
            """
            SELECT original_msg_id, original_channel_id 
            FROM message_mappings 
            WHERE synced_msg_id = ? AND synced_channel_id = ?
            """,
            (synced_msg_id, synced_channel_id),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_all_synced_for_message(
        self,
        msg_id: int,
        channel_id: int,
    ) -> List[Dict]:
        """
        Get all related messages (original + all synced copies) for a given message.

        The input (msg_id, channel_id) can be either:
        - the original message, or
        - any synced copy.

        Returns a list of dicts with keys:
        - 'synced_msg_id'
        - 'synced_channel_id'
        where one of the entries will always be the original itself.
        """
        # First, see if this message is a synced copy.
        original = await self.get_original_message(msg_id, channel_id)

        if original:
            # This message is a synced copy; base everything off the original.
            base_msg_id = original["original_msg_id"]
            base_channel_id = original["original_channel_id"]
        else:
            # Treat this as the original message.
            base_msg_id = msg_id
            base_channel_id = channel_id

        # Get all synced copies of the base/original message.
        synced = await self.get_synced_messages(base_msg_id, base_channel_id)

        # Start with all synced copies.
        results = list(synced)

        # Also include the base/original itself as a "synced" entry so callers
        # can find it by channel_id just like the others.
        results.append(
            {
                "synced_msg_id": base_msg_id,
                "synced_channel_id": base_channel_id,
            }
        )

        return results

    async def cleanup_old_mappings(self, max_rows: int = 50000) -> int:
        """
        Delete oldest mappings if table exceeds max_rows.
        Returns the number of rows deleted.
        """
        async with self.db.execute("SELECT COUNT(*) FROM message_mappings") as cursor:
            row = await cursor.fetchone()
            count = row[0]

        if count <= max_rows:
            return 0

        to_delete = count - max_rows
        
        # Delete the oldest rows (lowest IDs)
        await self.db.execute(
            """
            DELETE FROM message_mappings 
            WHERE id IN (
                SELECT id FROM message_mappings 
                ORDER BY id ASC 
                LIMIT ?
            )
            """,
            (to_delete,),
        )
        await self.db.commit()
        
        logger.info(f"Cleaned up {to_delete} old message mappings (was {count}, now {max_rows})")
        return to_delete

    # =======================
    # Active users (for /bing)
    # =======================

    async def touch_active_user(
        self,
        guild_id: int,
        guild_name: str,
        user_id: int,
        display_name: str,
        username: str,
    ) -> None:
        """
        Insert or update this user as 'recently active' in this guild.
        """
        now_ts = int(time.time())
        await self.db.execute(
            """
            INSERT INTO active_users (guild_id, guild_name, user_id, display_name, username, last_seen)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, user_id) DO UPDATE SET
                guild_name   = excluded.guild_name,
                display_name = excluded.display_name,
                username     = excluded.username,
                last_seen    = excluded.last_seen
            """,
            (guild_id, guild_name, user_id, display_name, username, now_ts),
        )
        await self.db.commit()

    async def search_active_users_in_group(
        self,
        group_uid: str,
        exclude_guild_id: int,
        query: str,
        max_age_days: int = 20,
        limit: int = 10,
    ) -> List[Dict]:
        """
        Return up to `limit` recent active users in the given sync group,
        excluding `exclude_guild_id`, whose name matches `query`.
        """
        channels = await self.get_channels_in_group(group_uid)
        guild_ids = {ch["guild_id"] for ch in channels if ch["guild_id"] != exclude_guild_id}

        if not guild_ids:
            return []

        # Delete entries older than max_age_days
        cutoff_dt = datetime.utcnow() - timedelta(days=max_age_days)
        cutoff_ts = int(cutoff_dt.timestamp())
        await self.db.execute(
            "DELETE FROM active_users WHERE last_seen < ?",
            (cutoff_ts,),
        )
        await self.db.commit()

        like = f"%{query.lower()}%"
        placeholders = ",".join("?" for _ in guild_ids)
        params: List = list(guild_ids)
        params.extend([like, like, limit])

        async with self.db.execute(
            f"""
            SELECT guild_id, guild_name, user_id, display_name, username, last_seen
            FROM active_users
            WHERE guild_id IN ({placeholders})
              AND (
                  LOWER(display_name) LIKE ?
                  OR LOWER(username) LIKE ?
              )
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            params,
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    # =======================
    # Muted users
    # =======================

    async def mute_user(self, user_id: int, muted_by: int, reason: str = None) -> bool:
        """Mute a user from being forwarded across bridges"""
        try:
            await self.db.execute(
                """
                INSERT INTO muted_users (user_id, muted_by, reason)
                VALUES (?, ?, ?)
                """,
                (user_id, muted_by, reason),
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False  # User already muted

    async def unmute_user(self, user_id: int) -> bool:
        """Unmute a user"""
        cursor = await self.db.execute(
            "DELETE FROM muted_users WHERE user_id = ?",
            (user_id,),
        )
        await self.db.commit()
        return cursor.rowcount > 0

    async def is_user_muted(self, user_id: int) -> bool:
        """Check if a user is muted"""
        async with self.db.execute(
            "SELECT 1 FROM muted_users WHERE user_id = ?",
            (user_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def get_muted_users(self) -> List[Dict]:
        """Get all muted users"""
        async with self.db.execute(
            """
            SELECT user_id, muted_by, muted_at, reason
            FROM muted_users
            ORDER BY muted_at DESC
            """,
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    # =======================
    # Polls
    # =======================

    async def create_poll(
        self,
        poll_id: str,
        group_uid: str,
        question: str,
        created_by: int,
        created_by_name: str,
        expires_at: datetime,
        options: List[str],
    ) -> bool:
        """Create a new poll with options"""
        try:
            await self.db.execute(
                """
                INSERT INTO polls (poll_id, group_uid, question, created_by, created_by_name, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (poll_id, group_uid, question, created_by, created_by_name, expires_at),
            )

            # Add poll options
            for idx, option_text in enumerate(options):
                await self.db.execute(
                    """
                    INSERT INTO poll_options (poll_id, option_text, option_index)
                    VALUES (?, ?, ?)
                    """,
                    (poll_id, option_text, idx),
                )

            await self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Error creating poll: {e}")
            return False

    async def get_poll(self, poll_id: str) -> Optional[Dict]:
        """Get poll metadata"""
        async with self.db.execute(
            """
            SELECT poll_id, group_uid, question, created_by, created_by_name, 
                   created_at, expires_at, is_closed
            FROM polls
            WHERE poll_id = ?
            """,
            (poll_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def get_poll_options(self, poll_id: str) -> List[Dict]:
        """Get all options for a poll"""
        async with self.db.execute(
            """
            SELECT option_text, option_index
            FROM poll_options
            WHERE poll_id = ?
            ORDER BY option_index
            """,
            (poll_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def add_poll_vote(self, poll_id: str, user_id: int, option_index: int) -> bool:
        """Record a vote for a poll (one vote per user globally)"""
        try:
            await self.db.execute(
                """
                INSERT INTO poll_votes (poll_id, user_id, option_index)
                VALUES (?, ?, ?)
                """,
                (poll_id, user_id, option_index),
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            # User already voted
            return False

    async def get_user_vote(self, poll_id: str, user_id: int) -> Optional[int]:
        """Get the option index a user voted for (if any)"""
        async with self.db.execute(
            """
            SELECT option_index
            FROM poll_votes
            WHERE poll_id = ? AND user_id = ?
            """,
            (poll_id, user_id),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None
    
    async def get_user_vote_info(self, poll_id: str, user_id: int) -> Optional[Dict]:
        """Get the vote info including option index and vote changes count"""
        async with self.db.execute(
            """
            SELECT option_index, vote_changes
            FROM poll_votes
            WHERE poll_id = ? AND user_id = ?
            """,
            (poll_id, user_id),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def change_user_vote(self, poll_id: str, user_id: int, new_option_index: int) -> bool:
        """Change a user's vote and increment vote_changes counter"""
        try:
            await self.db.execute(
                """
                UPDATE poll_votes 
                SET option_index = ?, vote_changes = vote_changes + 1
                WHERE poll_id = ? AND user_id = ?
                """,
                (new_option_index, poll_id, user_id),
            )
            await self.db.commit()
            return True
        except Exception as e:
            logger.error(f"Error changing vote: {e}")
            return False

    async def get_poll_vote_counts(self, poll_id: str) -> Dict[int, int]:
        """Get vote counts for each option"""
        async with self.db.execute(
            """
            SELECT option_index, COUNT(*) as count
            FROM poll_votes
            WHERE poll_id = ?
            GROUP BY option_index
            """,
            (poll_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return {row[0]: row[1] for row in rows}

    async def get_total_votes(self, poll_id: str) -> int:
        """Get total number of votes for a poll"""
        async with self.db.execute(
            """
            SELECT COUNT(*) FROM poll_votes WHERE poll_id = ?
            """,
            (poll_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def add_poll_message(
        self,
        poll_id: str,
        channel_id: int,
        message_id: int,
        guild_id: int,
    ):
        """Store a poll message instance"""
        await self.db.execute(
            """
            INSERT INTO poll_messages (poll_id, channel_id, message_id, guild_id)
            VALUES (?, ?, ?, ?)
            """,
            (poll_id, channel_id, message_id, guild_id),
        )
        await self.db.commit()

    async def get_poll_messages(self, poll_id: str) -> List[Dict]:
        """Get all message instances for a poll"""
        async with self.db.execute(
            """
            SELECT channel_id, message_id, guild_id
            FROM poll_messages
            WHERE poll_id = ?
            """,
            (poll_id,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def close_poll(self, poll_id: str):
        """Mark a poll as closed"""
        await self.db.execute(
            """
            UPDATE polls SET is_closed = 1 WHERE poll_id = ?
            """,
            (poll_id,),
        )
        await self.db.commit()

    async def get_expired_polls(self) -> List[str]:
        """Get all polls that have expired but aren't closed yet"""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        async with self.db.execute(
            """
            SELECT poll_id
            FROM polls
            WHERE expires_at < ? AND is_closed = 0
            """,
            (now,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]

    async def close(self):
        """Close database connection"""
        if self.db:
            await self.db.close()