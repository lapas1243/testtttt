"""
TgCF Pro - Smart Bump Service Engine
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Enterprise-grade automated advertising and campaign management system.
Provides intelligent scheduling, multi-target broadcasting, and comprehensive
performance analytics for business communication automation.

Features:
- Advanced campaign scheduling with multiple patterns
- Multi-account campaign management
- Real-time performance tracking and analytics
- Intelligent retry mechanisms and error handling
- Professional campaign templates and A/B testing

Author: TgCF Pro Team
License: MIT
Version: 1.0.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import schedule
import time
import os
import random
import queue
import psutil  # For resource monitoring
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from telethon import TelegramClient
from telethon.tl.custom import Button
from telethon.tl.types import ReplyKeyboardMarkup, KeyboardButton, KeyboardButtonUrl, KeyboardButtonRow
from telethon import errors
from telethon.errors import FloodWaitError
from forwarder_database import Database
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telethon_manager import telethon_manager
import json
import threading
import traceback

# Configure structured logging
logger = logging.getLogger(__name__)

class StructuredLogger:
    """Enhanced logging with structured data and context"""
    
    @staticmethod
    def log_operation(operation: str, user_id: int = None, campaign_id: int = None, 
                     account_id: int = None, success: bool = None, details: str = None):
        """Log operation with structured context"""
        context = {
            'operation': operation,
            'user_id': user_id,
            'campaign_id': campaign_id,
            'account_id': account_id,
            'success': success,
            'timestamp': datetime.now().isoformat(),
            'details': details
        }
        
        if success is True:
            logger.info(f"✅ {operation} completed successfully", extra=context)
        elif success is False:
            logger.error(f"❌ {operation} failed", extra=context)
        else:
            logger.info(f"🔄 {operation} in progress", extra=context)
    
    @staticmethod
    def log_error(operation: str, error: Exception, user_id: int = None, 
                 campaign_id: int = None, account_id: int = None):
        """Log error with full context and stack trace"""
        context = {
            'operation': operation,
            'user_id': user_id,
            'campaign_id': campaign_id,
            'account_id': account_id,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'timestamp': datetime.now().isoformat(),
            'stack_trace': traceback.format_exc()
        }
        
        logger.error(f"💥 {operation} failed: {error}", extra=context, exc_info=True)
    
    @staticmethod
    def log_performance(operation: str, duration: float, user_id: int = None, 
                       campaign_id: int = None, details: str = None):
        """Log performance metrics"""
        context = {
            'operation': operation,
            'duration_seconds': duration,
            'user_id': user_id,
            'campaign_id': campaign_id,
            'timestamp': datetime.now().isoformat(),
            'details': details
        }
        
        if duration > 10:
            logger.warning(f"⚠️ {operation} took {duration:.2f}s (slow)", extra=context)
        else:
            logger.info(f"⏱️ {operation} completed in {duration:.2f}s", extra=context)

@dataclass
class AdCampaign:
    """Represents an advertising campaign"""
    id: int
    user_id: int
    account_id: int
    campaign_name: str
    ad_content: str
    target_chats: List[str]
    schedule_type: str  # 'once', 'daily', 'weekly', 'custom'
    schedule_time: str  # Format: "HH:MM" or cron-like
    is_active: bool
    created_at: str
    last_run: Optional[str] = None
    total_sends: int = 0

class BumpService:
    """Service for managing automated ad bumping/posting - Optimized for 50+ accounts"""
    
    def __init__(self, bot_instance=None):
        self.db = Database()
        self.active_campaigns = {}
        self.scheduler_thread = None
        self.is_running = True  # Set to True so workers can run immediately
        self.telegram_clients = {}
        self.client_init_semaphore = threading.Semaphore(1)  # Thread-safe semaphore
        self.temp_files = set()  # Track temporary files for cleanup
        self.bot_instance = bot_instance  # Store bot instance for ReplyKeyboardMarkup
        
        # SCALING OPTIMIZATIONS for 50+ accounts (configurable via Config)
        from forwarder_config import Config
        self.execution_queue = queue.Queue(maxsize=Config.EXECUTION_QUEUE_SIZE)  # Queue for campaign executions
        self.execution_semaphore = threading.Semaphore(Config.MAX_CONCURRENT_CAMPAIGNS)  # Max concurrent
        self.client_last_used = {}  # Track when each client was last used
        self.client_cleanup_interval = Config.CLIENT_IDLE_TIMEOUT  # Close clients idle for X seconds
        self.max_execution_workers = Config.EXECUTION_WORKER_THREADS  # Worker threads
        
        # Start execution worker threads
        self.execution_workers = []
        for i in range(self.max_execution_workers):
            worker = threading.Thread(target=self._execution_worker, daemon=True, name=f"CampaignWorker-{i+1}")
            worker.start()
            self.execution_workers.append(worker)
            logger.info(f"✅ Started execution worker thread {i+1}/{self.max_execution_workers}")
        
        # Start client cleanup thread (if enabled)
        if Config.ENABLE_CLIENT_CLEANUP:
            self.cleanup_thread = threading.Thread(target=self._client_cleanup_worker, daemon=True, name="ClientCleanup")
            self.cleanup_thread.start()
            logger.info("✅ Started client cleanup thread")
        
        logger.info(f"🚀 SCALING MODE: {Config.MAX_CONCURRENT_CAMPAIGNS} concurrent campaigns, {Config.EXECUTION_WORKER_THREADS} workers")
        
        self.init_bump_database()
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🛡️ ANTI-BAN SYSTEM Functions
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    def _init_account_tracking(self, account_id: int, account_created_date=None):
        """Initialize tracking for an account"""
        from forwarder_config import Config
        from datetime import datetime, timedelta
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Check if tracking already exists
            cursor.execute('SELECT id FROM account_usage_tracking WHERE account_id = ?', (account_id,))
            if cursor.fetchone():
                return  # Already exists
            
            # Determine account age and daily limit
            if account_created_date:
                created_date = datetime.fromisoformat(account_created_date) if isinstance(account_created_date, str) else account_created_date
            else:
                created_date = datetime.now()
            
            account_age_days = (datetime.now() - created_date).days
            
            if account_age_days < Config.ACCOUNT_WARM_UP_DAYS:
                daily_limit = Config.MAX_MESSAGES_PER_DAY_NEW_ACCOUNT
            elif account_age_days < Config.ACCOUNT_MATURE_DAYS:
                daily_limit = Config.MAX_MESSAGES_PER_DAY_WARMED_ACCOUNT
            else:
                daily_limit = Config.MAX_MESSAGES_PER_DAY_MATURE_ACCOUNT
            
            cursor.execute('''
                INSERT INTO account_usage_tracking 
                (account_id, account_created_date, daily_limit)
                VALUES (?, ?, ?)
            ''', (account_id, created_date, daily_limit))
            conn.commit()
            
            logger.info(f"🛡️ ANTI-BAN: Initialized tracking for account {account_id} (age: {account_age_days} days, limit: {daily_limit}/day)")
    
    def _check_account_can_send(self, account_id: int, messages_to_send: int) -> tuple[bool, str]:
        """Check if account can send messages without hitting limits"""
        from forwarder_config import Config
        from datetime import datetime, timedelta
        
        self._init_account_tracking(account_id)
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT messages_sent_today, daily_limit, last_message_time, 
                       last_campaign_time, is_restricted, restriction_reason,
                       last_reset_date
                FROM account_usage_tracking 
                WHERE account_id = ?
            ''', (account_id,))
            
            row = cursor.fetchone()
            if not row:
                return False, "Account tracking not initialized"
            
            messages_today, daily_limit, last_message_time, last_campaign_time, is_restricted, restriction_reason, last_reset_date = row
            
            # Check if restricted
            if is_restricted:
                return False, f"Account restricted: {restriction_reason}"
            
            # Reset daily counter if new day
            today = datetime.now().date()
            last_reset = datetime.fromisoformat(last_reset_date).date() if last_reset_date else today
            if last_reset < today:
                messages_today = 0
                cursor.execute('''
                    UPDATE account_usage_tracking 
                    SET messages_sent_today = 0, last_reset_date = ? 
                    WHERE account_id = ?
                ''', (today, account_id))
                conn.commit()
                logger.info(f"🛡️ ANTI-BAN: Reset daily counter for account {account_id}")
            
            # Check daily limit (SKIP for mature accounts if disabled)
            account_age_days = (datetime.now() - datetime.fromisoformat(str(last_reset_date))).days if last_reset_date else 0
            is_mature = account_age_days >= Config.ACCOUNT_MATURE_DAYS
            
            if Config.DISABLE_DAILY_LIMITS_FOR_MATURE and is_mature:
                # No daily limits for mature accounts (2023+)
                logger.debug(f"🛡️ ANTI-BAN: Mature account - daily limits disabled")
            else:
                # Enforce daily limits for new/warmed accounts
                if messages_today + messages_to_send > daily_limit:
                    remaining = max(0, daily_limit - messages_today)
                    return False, f"Daily limit reached ({messages_today}/{daily_limit}). {remaining} messages remaining today."
            
            # Check cooldown between campaigns (randomized 1.0-1.4 hours)
            if last_campaign_time:
                last_campaign = datetime.fromisoformat(last_campaign_time)
                # Use random cooldown between MIN and MAX for unpredictable timing
                import random
                cooldown_minutes = random.uniform(
                    Config.MIN_COOLDOWN_BETWEEN_CAMPAIGNS_MINUTES,
                    Config.MAX_COOLDOWN_BETWEEN_CAMPAIGNS_MINUTES
                )
                time_since_last = (datetime.now() - last_campaign).total_seconds() / 60
                
                if time_since_last < cooldown_minutes:
                    remaining_minutes = cooldown_minutes - time_since_last
                    return False, f"Cooldown period active. Wait {remaining_minutes:.1f} more minutes."
            
            # Check minimum delay between messages
            if last_message_time:
                last_message = datetime.fromisoformat(last_message_time)
                min_delay_seconds = Config.MIN_DELAY_BETWEEN_MESSAGES
                time_since_last = (datetime.now() - last_message).total_seconds()
                
                if time_since_last < min_delay_seconds:
                    remaining_seconds = min_delay_seconds - time_since_last
                    return False, f"Message delay active. Wait {remaining_seconds:.0f} more seconds."
            
            return True, "OK"
    
    def _record_message_sent(self, account_id: int):
        """Record that a message was sent"""
        from datetime import datetime
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE account_usage_tracking 
                SET messages_sent_today = messages_sent_today + 1,
                    total_messages_sent = total_messages_sent + 1,
                    last_message_time = ?
                WHERE account_id = ?
            ''', (datetime.now(), account_id))
            conn.commit()
    
    def _record_campaign_start(self, account_id: int):
        """Record that a campaign started"""
        from datetime import datetime
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE account_usage_tracking 
                SET last_campaign_time = ?
                WHERE account_id = ?
            ''', (datetime.now(), account_id))
            conn.commit()
    
    def _get_safe_delay(self) -> float:
        """Get a safe random delay between messages"""
        from forwarder_config import Config
        import random
        
        min_delay = Config.MIN_DELAY_BETWEEN_MESSAGES
        max_delay = Config.MAX_DELAY_BETWEEN_MESSAGES
        
        # Use exponential distribution for more human-like delays
        base_delay = random.uniform(min_delay, max_delay)
        
        # Add occasional longer pauses (10% chance of 2x delay)
        if random.random() < 0.1:
            base_delay *= 2
            logger.info(f"🛡️ ANTI-BAN: Extended delay for natural behavior")
        
        return base_delay
    
    def _should_take_break(self) -> tuple[bool, float]:
        """Determine if account should take a break (ONLY during night hours 3-6 AM Lithuanian time)"""
        from forwarder_config import Config
        import random
        from datetime import datetime
        import pytz
        
        if not Config.ENABLE_RANDOM_BREAKS:
            return False, 0
        
        # Check if it's currently night time in Lithuanian timezone
        try:
            lithuania_tz = pytz.timezone(Config.NIGHT_BREAK_TIMEZONE)
            current_time_lithuania = datetime.now(lithuania_tz)
            current_hour = current_time_lithuania.hour
            
            # Check if within night hours (3:00 AM - 6:00 AM)
            is_night_time = Config.NIGHT_BREAK_START_HOUR <= current_hour < Config.NIGHT_BREAK_END_HOUR
            
            if not is_night_time:
                # Not night time - no breaks during day
                return False, 0
            
            # It's night time - take sleep break
            break_minutes = random.uniform(
                Config.MIN_BREAK_DURATION_MINUTES,
                Config.MAX_BREAK_DURATION_MINUTES
            )
            logger.info(f"😴 NIGHT TIME ({current_hour}:00 Lithuanian time): Taking sleep break")
            return True, break_minutes * 60  # Convert to seconds
            
        except Exception as e:
            logger.warning(f"⚠️ Timezone check failed: {e}. Skipping break.")
            return False, 0
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🆕 WARM-UP MODE Functions (for account recovery/new accounts)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    def enable_warmup_mode(self, account_id: int, duration_days: int = None):
        """Enable warm-up mode for an account (for recovery after ban or new accounts)"""
        from forwarder_config import Config
        from datetime import datetime, timedelta
        
        if duration_days is None:
            duration_days = Config.WARMUP_DURATION_DAYS
        
        self._init_account_tracking(account_id)
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            
            start_date = datetime.now()
            end_date = start_date + timedelta(days=duration_days)
            
            cursor.execute('''
                UPDATE account_usage_tracking 
                SET warmup_mode_enabled = 1,
                    warmup_start_date = ?,
                    warmup_end_date = ?,
                    daily_limit = ?
                WHERE account_id = ?
            ''', (start_date, end_date, Config.WARMUP_MAX_MESSAGES_PER_DAY, account_id))
            conn.commit()
            
            logger.info(f"🆕 WARM-UP MODE ENABLED for account {account_id}")
            logger.info(f"   Duration: {duration_days} days (until {end_date.strftime('%Y-%m-%d')})")
            logger.info(f"   Daily limit: {Config.WARMUP_MAX_MESSAGES_PER_DAY} messages")
            logger.info(f"   Min delay: {Config.WARMUP_MIN_DELAY_MINUTES} minutes")
    
    def disable_warmup_mode(self, account_id: int):
        """Disable warm-up mode for an account"""
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE account_usage_tracking 
                SET warmup_mode_enabled = 0,
                    warmup_start_date = NULL,
                    warmup_end_date = NULL
                WHERE account_id = ?
            ''', (account_id,))
            conn.commit()
            
            logger.info(f"✅ WARM-UP MODE DISABLED for account {account_id}")
    
    def _is_account_in_warmup(self, account_id: int) -> tuple[bool, dict]:
        """Check if account is in warm-up mode and return settings"""
        from datetime import datetime
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT warmup_mode_enabled, warmup_start_date, warmup_end_date
                FROM account_usage_tracking
                WHERE account_id = ?
            ''', (account_id,))
            
            row = cursor.fetchone()
            if not row or not row[0]:
                return False, {}
            
            warmup_enabled, start_date, end_date = row
            
            # Check if warm-up period has ended
            if end_date:
                end_datetime = datetime.fromisoformat(end_date) if isinstance(end_date, str) else end_date
                if datetime.now() > end_datetime:
                    # Warm-up period over - auto-disable
                    self.disable_warmup_mode(account_id)
                    logger.info(f"🎉 WARM-UP COMPLETE for account {account_id}")
                    return False, {}
            
            # Still in warm-up period
            days_remaining = (end_datetime - datetime.now()).days if end_date else 0
            
            return True, {
                'start_date': start_date,
                'end_date': end_date,
                'days_remaining': days_remaining
            }
    
    def _get_warmup_delay(self) -> float:
        """Get delay for warm-up mode (much longer, safer delays)"""
        from forwarder_config import Config
        import random
        
        # Warm-up mode: 30+ minute delays between messages
        min_delay_seconds = Config.WARMUP_MIN_DELAY_MINUTES * 60
        max_delay_seconds = min_delay_seconds * 1.5  # 30-45 min range
        
        return random.uniform(min_delay_seconds, max_delay_seconds)
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 🎭 ADVANCED ANTI-BAN FEATURES
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    def _vary_message_content(self, original_text: str) -> str:
        """
        Add random variations to message text to avoid spam detection.
        Adds random blank lines and/or random ending phrases.
        """
        from forwarder_config import Config
        import random
        
        if not Config.ENABLE_MESSAGE_VARIATION or not original_text:
            return original_text
        
        varied_text = original_text
        
        # Add 1-3 random blank lines within the message
        lines = varied_text.split('\n')
        if len(lines) > 2:
            # Insert blank lines at random positions (not at start/end)
            num_blanks = random.randint(Config.MIN_BLANK_LINES, Config.MAX_BLANK_LINES)
            for _ in range(num_blanks):
                insert_pos = random.randint(1, len(lines) - 1)
                lines.insert(insert_pos, '')
            varied_text = '\n'.join(lines)
        
        # Add random ending phrase (50% chance of adding something)
        ending = random.choice(Config.MESSAGE_ENDING_PHRASES)
        varied_text += ending
        
        return varied_text
    
    async def _simulate_typing(self, client, chat_entity, text_length: int):
        """
        Simulate typing action before sending message.
        Duration based on message length (more realistic).
        """
        from forwarder_config import Config
        import random
        import asyncio
        
        if not Config.ENABLE_TYPING_SIMULATION:
            return
        
        try:
            # Send typing action
            await client.send_typing_action(chat_entity)
            
            # Calculate typing duration (longer for longer messages)
            base_duration = random.uniform(
                Config.MIN_TYPING_DURATION_SECONDS,
                Config.MAX_TYPING_DURATION_SECONDS
            )
            
            # Add time based on text length (realistic typing speed)
            length_factor = min(text_length / 200, 3)  # Max 3x multiplier
            typing_duration = base_duration * (1 + length_factor * 0.5)
            
            logger.info(f"⌨️ TYPING: Simulating {typing_duration:.1f}s typing action")
            await asyncio.sleep(typing_duration)
            
        except Exception as e:
            logger.debug(f"Typing simulation error (non-critical): {e}")
    
    async def _simulate_read_receipts(self, client, account_id: int, target_chat=None):
        """
        Simulate reading messages in groups to appear human-like.
        Reads from both target groups and random public groups.
        """
        from forwarder_config import Config
        import random
        import asyncio
        
        if not Config.ENABLE_READ_RECEIPTS:
            return
        
        try:
            chats_to_read = []
            
            # Read target group if provided (30% chance)
            if target_chat and random.random() < Config.READ_RECEIPTS_PROBABILITY:
                chats_to_read.append(target_chat)
            
            # Also read random groups (simulate browsing)
            try:
                dialogs = await client.get_dialogs(limit=50)
                public_groups = [d for d in dialogs if d.is_group or d.is_channel]
                
                if public_groups:
                    random_groups = random.sample(
                        public_groups,
                        min(Config.RANDOM_GROUPS_TO_READ, len(public_groups))
                    )
                    chats_to_read.extend([g.entity for g in random_groups])
            except Exception as e:
                logger.debug(f"Could not fetch dialogs for reading: {e}")
            
            # Read messages from selected chats
            for chat in chats_to_read:
                try:
                    # Mark messages as read
                    await client.send_read_acknowledge(chat)
                    
                    chat_name = getattr(chat, 'title', getattr(chat, 'username', 'Unknown'))
                    logger.info(f"👀 READ RECEIPTS: Marked messages as read in '{chat_name}'")
                    
                    # Small delay between reads
                    await asyncio.sleep(random.uniform(1, 3))
                    
                except Exception as e:
                    logger.debug(f"Read receipt error for {chat}: {e}")
            
            # Update last online simulation time
            conn = self._get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE account_usage_tracking 
                SET last_online_simulation = CURRENT_TIMESTAMP
                WHERE account_id = ?
            """, (account_id,))
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.debug(f"Read receipt simulation error (non-critical): {e}")
    
    def _handle_peer_flood(self, account_id: int, account_name: str):
        """
        Handle PeerFlood error - this is a pre-ban warning from Telegram.
        Auto-pause account and enable warm-up mode.
        """
        from forwarder_config import Config
        
        logger.error(f"🚨 PEER FLOOD DETECTED for account '{account_name}' (ID: {account_id})")
        logger.error(f"⚠️ This is a PRE-BAN WARNING from Telegram!")
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Mark peer flood detected
            cursor.execute("""
                UPDATE account_usage_tracking 
                SET peer_flood_detected = 1,
                    peer_flood_time = CURRENT_TIMESTAMP,
                    is_restricted = 1,
                    restriction_reason = 'PeerFlood - Too many messages'
                WHERE account_id = ?
            """, (account_id,))
            
            conn.commit()
            conn.close()
            
            # Auto-enable warm-up mode if configured
            if Config.AUTO_ENABLE_WARMUP_ON_PEER_FLOOD:
                self.enable_warmup_mode(account_id, duration_days=7)
                logger.warning(f"🆕 AUTO-RECOVERY: Enabled 7-day warm-up mode for account {account_id}")
                logger.warning(f"⏸️ Account will be paused for {Config.PEER_FLOOD_COOLDOWN_HOURS} hours")
            else:
                logger.warning(f"⏸️ Account paused for {Config.PEER_FLOOD_COOLDOWN_HOURS} hours")
                logger.warning(f"💡 Consider enabling warm-up mode: python check_account_safety.py")
            
        except Exception as e:
            logger.error(f"Error handling peer flood: {e}")
    
    def _record_flood_wait(self, account_id: int, wait_seconds: int):
        """
        Record FloodWait error for an account.
        This helps track which accounts are being rate-limited.
        """
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            # Update account tracking with flood wait info
            cursor.execute("""
                UPDATE account_usage_tracking 
                SET is_restricted = 1,
                    restriction_reason = ?,
                    last_campaign_time = CURRENT_TIMESTAMP
                WHERE account_id = ?
            """, (f"FloodWait {wait_seconds}s", account_id))
            
            conn.commit()
            conn.close()
            
            logger.warning(f"📝 Recorded FloodWait for account {account_id}: {wait_seconds}s cooldown")
            
        except Exception as e:
            logger.error(f"Error recording flood wait: {e}")
    
    def _check_peer_flood_status(self, account_id: int) -> tuple[bool, str]:
        """
        Check if account is in peer flood cooldown.
        Returns (is_blocked, reason).
        """
        from forwarder_config import Config
        from datetime import datetime, timedelta
        
        try:
            conn = self._get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT peer_flood_detected, peer_flood_time
                FROM account_usage_tracking
                WHERE account_id = ?
            """, (account_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if not row or not row[0]:
                return False, ""
            
            peer_flood_detected, peer_flood_time_str = row
            
            if peer_flood_detected and peer_flood_time_str:
                peer_flood_time = datetime.fromisoformat(peer_flood_time_str)
                cooldown_end = peer_flood_time + timedelta(hours=Config.PEER_FLOOD_COOLDOWN_HOURS)
                
                if datetime.now() < cooldown_end:
                    remaining = (cooldown_end - datetime.now()).total_seconds() / 3600
                    return True, f"PeerFlood cooldown active (wait {remaining:.1f} more hours)"
                else:
                    # Cooldown expired, clear flag
                    conn = self._get_db_connection()
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE account_usage_tracking 
                        SET peer_flood_detected = 0,
                            is_restricted = 0,
                            restriction_reason = NULL
                        WHERE account_id = ?
                    """, (account_id,))
                    conn.commit()
                    conn.close()
                    
                    logger.info(f"✅ PeerFlood cooldown expired for account {account_id}")
                    return False, ""
            
            return False, ""
            
        except Exception as e:
            logger.error(f"Error checking peer flood status: {e}")
            return False, ""
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    def _execution_worker(self):
        """Worker thread that processes campaign executions from the queue"""
        worker_name = threading.current_thread().name
        logger.info(f"🔧 {worker_name} started and ready")
        
        while self.is_running:
            try:
                # Get campaign from queue (wait max 1 second)
                try:
                    campaign_id = self.execution_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # 🛡️ ANTI-BAN: Check if we should take a random break
                should_break, break_duration = self._should_take_break()
                if should_break:
                    logger.info(f"☕ ANTI-BAN: {worker_name} taking a {break_duration/60:.1f} minute break to appear more human")
                    time.sleep(break_duration)
                
                # Acquire semaphore to limit concurrent executions
                with self.execution_semaphore:
                    logger.info(f"🚀 {worker_name} executing campaign {campaign_id}")
                    start_time = time.time()
                    
                    try:
                        # Execute the campaign
                        self.send_ad(campaign_id)
                        duration = time.time() - start_time
                        logger.info(f"✅ {worker_name} completed campaign {campaign_id} in {duration:.2f}s")
                    except Exception as e:
                        logger.error(f"❌ {worker_name} failed campaign {campaign_id}: {e}")
                        logger.error(f"Stack trace: {traceback.format_exc()}")
                    finally:
                        self.execution_queue.task_done()
                
            except Exception as e:
                logger.error(f"❌ Error in {worker_name}: {e}")
                time.sleep(5)
        
        logger.info(f"🔧 {worker_name} stopped")
    
    def _client_cleanup_worker(self):
        """Worker thread that closes idle Telegram clients to save memory"""
        logger.info("🧹 Client cleanup worker started")
        
        while self.is_running:
            try:
                current_time = time.time()
                clients_to_close = []
                
                # Find idle clients
                with self.client_init_semaphore:
                    for account_id, last_used in list(self.client_last_used.items()):
                        if current_time - last_used > self.client_cleanup_interval:
                            if account_id in self.telegram_clients:
                                clients_to_close.append(account_id)
                
                # Close idle clients
                for account_id in clients_to_close:
                    try:
                        client = self.telegram_clients.get(account_id)
                        if client and client.is_connected():
                            # Run disconnect in async
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                loop.run_until_complete(client.disconnect())
                            finally:
                                loop.close()
                            
                            # Remove from cache
                            with self.client_init_semaphore:
                                if account_id in self.telegram_clients:
                                    del self.telegram_clients[account_id]
                                if account_id in self.client_last_used:
                                    del self.client_last_used[account_id]
                            
                            logger.info(f"🧹 Closed idle client for account {account_id} (idle for {self.client_cleanup_interval}s)")
                    except Exception as e:
                        logger.warning(f"⚠️ Error closing idle client {account_id}: {e}")
                
                # Log memory usage every cleanup cycle
                try:
                    process = psutil.Process()
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    cpu_percent = process.cpu_percent(interval=1)
                    logger.info(f"📊 Resource usage: {memory_mb:.1f} MB RAM, {cpu_percent:.1f}% CPU, {len(self.telegram_clients)} active clients")
                except ImportError:
                    logger.debug("psutil not available - resource monitoring disabled")
                except Exception as e:
                    logger.debug(f"Resource monitoring error: {e}")
                
                # Sleep for cleanup interval
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"❌ Error in client cleanup worker: {e}")
                time.sleep(60)
        
        logger.info("🧹 Client cleanup worker stopped")
    
    def _get_db_connection(self):
        """Get database connection with proper configuration"""
        return self.db._get_connection()
    
    def _register_temp_file(self, file_path: str):
        """Register a temporary file for cleanup"""
        self.temp_files.add(file_path)
    
    def _cleanup_temp_file(self, file_path: str):
        """Clean up a temporary file"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.debug(f"Cleaned up temporary file: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to clean up temporary file {file_path}: {e}")
        finally:
            self.temp_files.discard(file_path)
    
    async def _process_bridge_channel_message(self, client, chat_entity, ad_content, telethon_reply_markup):
        """Process bridge channel message with premium emoji preservation"""
        try:
            bridge_channel_entity = ad_content.get('bridge_channel_entity')
            bridge_message_id = ad_content.get('bridge_message_id')
            
            logger.info(f"🔗 Bridge channel: {bridge_channel_entity}, Message ID: {bridge_message_id}")
            
            # Step 1: Get the bridge channel entity (join if needed)
            try:
                bridge_entity = await client.get_entity(bridge_channel_entity)
                logger.info(f"✅ Bridge channel entity resolved: {getattr(bridge_entity, 'title', bridge_channel_entity)}")
                
                # Try to join the channel (if it's public and we're not already in it)
                try:
                    from telethon.tl.functions.channels import JoinChannelRequest
                    await client(JoinChannelRequest(bridge_entity))
                    logger.info(f"✅ Joined bridge channel {bridge_channel_entity}")
                except Exception as join_error:
                    logger.info(f"Already in bridge channel or can't join: {join_error}")
                
            except Exception as entity_error:
                logger.error(f"❌ Could not resolve bridge channel {bridge_channel_entity}: {entity_error}")
                return
            
            # Step 2: Get the original message from bridge channel (preserves all entities)
            try:
                original_message = await client.get_messages(bridge_entity, ids=bridge_message_id)
                if not original_message:
                    logger.error(f"❌ Message {bridge_message_id} not found in {bridge_channel_entity}")
                    return
                
                logger.info(f"✅ Retrieved original message from bridge channel with all entities intact")
                logger.info(f"Message has media: {bool(original_message.media)}")
                logger.info(f"Message text length: {len(original_message.message or '')}")
                
                # Step 3: Forward the message with all entities preserved + add buttons
                if original_message.media:
                    # Forward media with preserved entities and add buttons
                    message = await client.send_file(
                        chat_entity,
                        original_message.media,
                        caption=original_message.message,
                        reply_markup=telethon_reply_markup
                    )
                    logger.info(f"✅ Bridge channel media forwarded with PREMIUM EMOJIS and buttons to {chat_entity.title}")
                else:
                    # Forward text with preserved entities and add buttons
                    message = await client.send_message(
                        chat_entity,
                        original_message.message,
                        reply_markup=telethon_reply_markup
                    )
                    logger.info(f"✅ Bridge channel text forwarded with PREMIUM EMOJIS and buttons to {chat_entity.title}")
                
            except Exception as message_error:
                logger.error(f"❌ Could not retrieve/forward message from bridge channel: {message_error}")
                return
                
        except Exception as e:
            logger.error(f"❌ Bridge channel processing failed: {e}")
            return
    
    def cleanup_all_resources(self):
        """Clean up all resources (clients, temp files, etc.)"""
        logger.info("Starting comprehensive resource cleanup...")
        
        # Clean up all Telegram clients
        for account_id, client in list(self.telegram_clients.items()):
            try:
                if hasattr(client, 'disconnect'):
                    # Run disconnect in a separate thread to avoid blocking
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._sync_disconnect_client, client)
                        future.result(timeout=5)  # 5 second timeout
                logger.info(f"Disconnected client for account {account_id}")
            except Exception as e:
                logger.error(f"Error disconnecting client {account_id}: {e}")
            finally:
                del self.telegram_clients[account_id]
        
        # Clean up all temporary files
        for temp_file in list(self.temp_files):
            self._cleanup_temp_file(temp_file)
        
        # Clean up any remaining session files
        self._cleanup_session_files()
        
        logger.info("Resource cleanup completed")
    
    def _sync_disconnect_client(self, client):
        """Synchronously disconnect a client"""
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(client.disconnect())
            finally:
                loop.close()
        except Exception as e:
            logger.error(f"Failed to disconnect client: {e}")
    
    def _cleanup_session_files(self):
        """Clean up all session files"""
        import glob
        try:
            # Find all session files
            session_files = glob.glob("bump_session_*.session")
            for session_file in session_files:
                try:
                    if os.path.exists(session_file):
                        os.remove(session_file)
                        logger.debug(f"Cleaned up session file: {session_file}")
                except Exception as e:
                    logger.warning(f"Failed to clean up session file {session_file}: {e}")
        except Exception as e:
            logger.error(f"Error during session file cleanup: {e}")
    
    # Removed _format_buttons_as_text - now using inline buttons only
    
    def _reconstruct_text_with_entities(self, text, entities):
        """Reconstruct text with custom emojis using entity data"""
        if not text or not entities:
            return text or ""
        
        logger.info(f"Reconstructing text with {len(entities)} entities")
        
        # Sort entities by offset to process them in order
        sorted_entities = sorted(entities, key=lambda x: x.get('offset', 0))
        
        reconstructed = ""
        last_offset = 0
        
        for entity in sorted_entities:
            entity_type = entity.get('type', '')
            offset = entity.get('offset', 0)
            length = entity.get('length', 0)
            
            # Add text before this entity
            if offset > last_offset:
                reconstructed += text[last_offset:offset]
            
            # Get the entity text
            entity_text = text[offset:offset + length]
            
            if entity_type == 'custom_emoji' and entity.get('custom_emoji_id'):
                # For custom emojis, we'll use a special format that Telethon can understand
                custom_emoji_id = entity.get('custom_emoji_id')
                # Use the original text but mark it for custom emoji
                reconstructed += entity_text  # Keep original emoji text
                logger.info(f"Preserved custom emoji: {entity_text} (ID: {custom_emoji_id})")
            else:
                # For other entities, just add the text
                reconstructed += entity_text
            
            last_offset = offset + length
        
        # Add remaining text
        if last_offset < len(text):
            reconstructed += text[last_offset:]
        
        logger.info(f"Text reconstruction complete: {len(reconstructed)} chars")
        return reconstructed
    
    def _convert_to_telethon_entities(self, entities, text):
        """Convert Bot API entities to Telethon entities for premium emoji support"""
        if not entities:
            return []
        
        try:
            from telethon.tl.types import (
                MessageEntityCustomEmoji, MessageEntityBold, MessageEntityItalic,
                MessageEntityTextUrl, MessageEntityHashtag
            )
            
            telethon_entities = []
            
            for entity in entities:
                entity_type = entity.get('type', '')
                offset = entity.get('offset', 0)
                length = entity.get('length', 0)
                
                # Skip if offset/length would be out of bounds
                if offset + length > len(text):
                    continue
                
                if entity_type == 'custom_emoji' and entity.get('custom_emoji_id'):
                    # This is the key for premium emojis!
                    custom_emoji_id = int(entity.get('custom_emoji_id'))
                    telethon_entity = MessageEntityCustomEmoji(
                        offset=offset,
                        length=length,
                        document_id=custom_emoji_id
                    )
                    telethon_entities.append(telethon_entity)
                    # Custom emoji converted successfully
                
                elif entity_type == 'bold':
                    telethon_entities.append(MessageEntityBold(offset=offset, length=length))
                
                elif entity_type == 'italic':
                    telethon_entities.append(MessageEntityItalic(offset=offset, length=length))
                
                elif entity_type == 'text_link' and entity.get('url'):
                    telethon_entities.append(MessageEntityTextUrl(
                        offset=offset, length=length, url=entity.get('url')
                    ))
                
                elif entity_type == 'hashtag':
                    telethon_entities.append(MessageEntityHashtag(offset=offset, length=length))
            
            logger.info(f"Converted {len(telethon_entities)} entities for Telethon")
            return telethon_entities
            
        except Exception as e:
            logger.error(f"Failed to convert entities to Telethon format: {e}")
            return []
    
    # Removed _add_buttons_to_text - now using inline buttons directly
    
    def init_bump_database(self):
        """Initialize bump service database tables"""
        import sqlite3
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            # 🛡️ ANTI-BAN SYSTEM: Account Usage Tracking Table
            # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS account_usage_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER UNIQUE,
                    account_created_date TIMESTAMP,
                    messages_sent_today INTEGER DEFAULT 0,
                    last_message_time TIMESTAMP,
                    last_campaign_time TIMESTAMP,
                    daily_limit INTEGER,
                    is_restricted BOOLEAN DEFAULT 0,
                    restriction_reason TEXT,
                    total_messages_sent INTEGER DEFAULT 0,
                    last_reset_date DATE DEFAULT CURRENT_DATE,
                    warmup_mode_enabled BOOLEAN DEFAULT 0,
                    warmup_start_date TIMESTAMP,
                    warmup_end_date TIMESTAMP,
                    peer_flood_detected BOOLEAN DEFAULT 0,
                    peer_flood_time TIMESTAMP,
                    last_online_simulation TIMESTAMP,
                    FOREIGN KEY (account_id) REFERENCES telegram_accounts (id)
                )
            ''')
            
            # Add warmup columns to existing table if they don't exist
            cursor.execute("PRAGMA table_info(account_usage_tracking)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'warmup_mode_enabled' not in columns:
                cursor.execute('ALTER TABLE account_usage_tracking ADD COLUMN warmup_mode_enabled BOOLEAN DEFAULT 0')
                cursor.execute('ALTER TABLE account_usage_tracking ADD COLUMN warmup_start_date TIMESTAMP')
                cursor.execute('ALTER TABLE account_usage_tracking ADD COLUMN warmup_end_date TIMESTAMP')
                logger.info("Added warmup mode columns to account_usage_tracking table")
            
            # Add peer flood detection columns to existing table if they don't exist
            if 'peer_flood_detected' not in columns:
                cursor.execute('ALTER TABLE account_usage_tracking ADD COLUMN peer_flood_detected BOOLEAN DEFAULT 0')
                cursor.execute('ALTER TABLE account_usage_tracking ADD COLUMN peer_flood_time TIMESTAMP')
                cursor.execute('ALTER TABLE account_usage_tracking ADD COLUMN last_online_simulation TIMESTAMP')
                logger.info("Added peer flood detection columns to account_usage_tracking table")
            
            # Ad campaigns table - Enhanced for multi-userbot support
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ad_campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    account_id INTEGER,
                    campaign_name TEXT,
                    ad_content TEXT,
                    target_chats TEXT,
                    schedule_type TEXT,
                    schedule_time TEXT,
                    buttons TEXT,
                    target_mode TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    immediate_start BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_run TIMESTAMP,
                    total_sends INTEGER DEFAULT 0,
                    additional_accounts TEXT,  -- JSON array of {account_id, delay_minutes, content_variation}
                    spam_avoidance_enabled BOOLEAN DEFAULT 1,
                    timing_variation_minutes INTEGER DEFAULT 5,
                    content_variations TEXT,  -- JSON array of message variations
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    FOREIGN KEY (account_id) REFERENCES telegram_accounts (id)
                )
            ''')
            
            # Campaign execution logs for spam avoidance
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS campaign_execution_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER,
                    account_id INTEGER,
                    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    content_variation_used INTEGER DEFAULT 0,
                    groups_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    delay_applied_minutes INTEGER DEFAULT 0,
                    FOREIGN KEY (campaign_id) REFERENCES ad_campaigns (id),
                    FOREIGN KEY (account_id) REFERENCES telegram_accounts (id)
                )
            ''')
            
            # Add missing columns to existing tables if they don't exist
            cursor.execute("PRAGMA table_info(ad_campaigns)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'buttons' not in columns:
                cursor.execute('ALTER TABLE ad_campaigns ADD COLUMN buttons TEXT')
                logger.info("Added buttons column to ad_campaigns table")
            if 'target_mode' not in columns:
                cursor.execute('ALTER TABLE ad_campaigns ADD COLUMN target_mode TEXT')
                logger.info("Added target_mode column to ad_campaigns table")
            if 'immediate_start' not in columns:
                cursor.execute('ALTER TABLE ad_campaigns ADD COLUMN immediate_start BOOLEAN DEFAULT 0')
                logger.info("Added immediate_start column to ad_campaigns table")
            if 'additional_accounts' not in columns:
                cursor.execute('ALTER TABLE ad_campaigns ADD COLUMN additional_accounts TEXT')
                logger.info("Added additional_accounts column to ad_campaigns table")
            if 'spam_avoidance_enabled' not in columns:
                cursor.execute('ALTER TABLE ad_campaigns ADD COLUMN spam_avoidance_enabled BOOLEAN DEFAULT 1')
                logger.info("Added spam_avoidance_enabled column to ad_campaigns table")
            if 'timing_variation_minutes' not in columns:
                cursor.execute('ALTER TABLE ad_campaigns ADD COLUMN timing_variation_minutes INTEGER DEFAULT 5')
                logger.info("Added timing_variation_minutes column to ad_campaigns table")
            if 'content_variations' not in columns:
                cursor.execute('ALTER TABLE ad_campaigns ADD COLUMN content_variations TEXT')
                logger.info("Added content_variations column to ad_campaigns table")
            
            # Update existing campaigns with default values and ensure they're active
            cursor.execute("UPDATE ad_campaigns SET buttons = ? WHERE buttons IS NULL", (json.dumps([{"text": "Shop Now", "url": "https://t.me/testukassdfdds"}]),))
            cursor.execute("UPDATE ad_campaigns SET target_mode = 'all_groups' WHERE target_mode IS NULL")
            cursor.execute("UPDATE ad_campaigns SET immediate_start = 0 WHERE immediate_start IS NULL")
            cursor.execute("UPDATE ad_campaigns SET is_active = 1 WHERE is_active IS NULL OR is_active = 0")
            cursor.execute("UPDATE ad_campaigns SET spam_avoidance_enabled = 1 WHERE spam_avoidance_enabled IS NULL")
            cursor.execute("UPDATE ad_campaigns SET timing_variation_minutes = 5 WHERE timing_variation_minutes IS NULL")
            
            updated_count = cursor.rowcount
            if updated_count > 0:
                logger.info(f"Updated {updated_count} existing campaigns with default button data and activated them")
            
            # Ad performance tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ad_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER,
                    user_id INTEGER,
                    target_chat TEXT,
                    message_id INTEGER,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'sent',
                    FOREIGN KEY (campaign_id) REFERENCES ad_campaigns (id),
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            conn.commit()
    
    def add_campaign(self, user_id: int, account_id: int, campaign_name: str, 
                    ad_content, target_chats: List[str], schedule_type: str, 
                    schedule_time: str, buttons=None, target_mode='specific', immediate_start=False) -> int:
        """Add new ad campaign with support for complex content types and buttons"""
        import sqlite3
        start_time = time.time()
        
        try:
            StructuredLogger.log_operation(
                "add_campaign", 
                user_id=user_id, 
                campaign_id=None, 
                account_id=account_id,
                success=None,
                details=f"Creating campaign '{campaign_name}' with {len(target_chats)} targets"
            )
            
            # Convert ad_content to JSON string if it's a list or dict
            if isinstance(ad_content, (list, dict)):
                ad_content_str = json.dumps(ad_content)
            else:
                ad_content_str = str(ad_content)
            
            # Convert target_chats to JSON string
            target_chats_str = json.dumps(target_chats) if isinstance(target_chats, list) else str(target_chats)
            
            # Convert buttons to JSON string
            buttons_str = json.dumps(buttons) if buttons else None
            
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO ad_campaigns 
                    (user_id, account_id, campaign_name, ad_content, target_chats, schedule_type, schedule_time, buttons, target_mode, immediate_start)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (user_id, account_id, campaign_name, ad_content_str, 
                     target_chats_str, schedule_type, schedule_time, buttons_str, target_mode, immediate_start))
                conn.commit()
                campaign_id = cursor.lastrowid
                
                # Schedule the campaign
                self.schedule_campaign(campaign_id)
                
                duration = time.time() - start_time
                StructuredLogger.log_performance(
                    "add_campaign", 
                    duration, 
                    user_id=user_id, 
                    campaign_id=campaign_id,
                    details=f"Campaign '{campaign_name}' created and scheduled"
                )
                
                StructuredLogger.log_operation(
                    "add_campaign", 
                    user_id=user_id, 
                    campaign_id=campaign_id, 
                    account_id=account_id,
                    success=True,
                    details=f"Campaign '{campaign_name}' successfully created"
                )
                
                # Execute immediately if requested
                if immediate_start:
                    logger.info(f"🚀 Running campaign {campaign_id} immediately on creation")
                    # Run the campaign execution in a separate thread to not block
                    threading.Thread(
                        target=self._run_campaign_immediately, 
                        args=(campaign_id,),
                        daemon=True
                    ).start()
                
                return campaign_id
                
        except Exception as e:
            StructuredLogger.log_error(
                "add_campaign", 
                e, 
                user_id=user_id, 
                account_id=account_id,
                details=f"Failed to create campaign '{campaign_name}'"
            )
            raise
    
    def _run_campaign_immediately(self, campaign_id: int):
        """Run campaign immediately in a separate thread"""
        try:
            logger.info(f"🚀 Starting immediate execution of campaign {campaign_id}")
            # Create new event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            try:
                # Run the campaign execution
                loop.run_until_complete(self._execute_campaign_async(campaign_id))
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"❌ Immediate campaign execution failed for {campaign_id}: {e}")
    
    async def _execute_campaign_async(self, campaign_id: int):
        """Execute campaign asynchronously - same logic as scheduled execution"""
        try:
            # Get campaign data
            campaign = self.db.get_campaign(campaign_id)
            if not campaign:
                logger.error(f"Campaign {campaign_id} not found")
                return
            
            logger.info(f"🚀 Executing immediate campaign {campaign_id}: {campaign['campaign_name']}")
            
            # Use the existing campaign execution logic
            await self._async_send_ad(campaign_id)
            
            logger.info(f"✅ Immediate campaign {campaign_id} executed successfully")
            
        except Exception as e:
            logger.error(f"❌ Immediate campaign execution failed for {campaign_id}: {e}")
    
    def get_user_campaigns(self, user_id: int) -> List[Dict]:
        """Get all campaigns for a user"""
        import sqlite3
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ac.id, ac.user_id, ac.account_id, ac.campaign_name, ac.ad_content, 
                       ac.target_chats, ac.schedule_type, ac.schedule_time, ac.buttons, 
                       ac.target_mode, ac.is_active, ac.created_at, ac.last_run, 
                       ac.total_sends, ta.account_name
                FROM ad_campaigns ac
                LEFT JOIN telegram_accounts ta ON ac.account_id = ta.id
                WHERE ac.user_id = ?
                ORDER BY ac.created_at DESC
            ''', (user_id,))
            rows = cursor.fetchall()
            
            campaigns = []
            for row in rows:
                # Parse ad_content (could be JSON string or plain string) - safer parsing
                try:
                    if row[4] and isinstance(row[4], str) and row[4].startswith(('[', '{')):
                        ad_content = json.loads(row[4])
                    else:
                        ad_content = str(row[4]) if row[4] else ""
                except (json.JSONDecodeError, AttributeError, TypeError):
                    ad_content = str(row[4]) if row[4] else ""
                
                # Parse target_chats (should be JSON string) - safer parsing
                try:
                    if row[5] and isinstance(row[5], str):
                        target_chats = json.loads(row[5])
                    elif isinstance(row[5], list):
                        target_chats = row[5]
                    else:
                        target_chats = [str(row[5])] if row[5] else []
                except (json.JSONDecodeError, TypeError):
                    target_chats = [str(row[5])] if row[5] else []
                
                # Parse buttons if they exist - much safer parsing
                buttons = []
                try:
                    if len(row) > 8 and row[8] is not None:
                        if isinstance(row[8], str) and row[8]:
                            buttons = json.loads(row[8])
                        elif isinstance(row[8], list):
                            buttons = row[8]
                except (json.JSONDecodeError, IndexError, TypeError):
                    buttons = []
                
                # Parse target_mode if it exists - safer parsing
                try:
                    target_mode = str(row[9]) if len(row) > 9 and row[9] else 'specific'
                except (IndexError, TypeError):
                    target_mode = 'specific'
                
                campaigns.append({
                    'id': row[0],                    # ac.id
                    'user_id': row[1],               # ac.user_id  
                    'account_id': row[2],            # ac.account_id
                    'campaign_name': row[3],         # ac.campaign_name
                    'ad_content': ad_content,        # ac.ad_content (parsed)
                    'target_chats': target_chats,    # ac.target_chats (parsed)
                    'schedule_type': row[6],         # ac.schedule_type
                    'schedule_time': row[7],         # ac.schedule_time
                    'buttons': buttons,              # ac.buttons (parsed)
                    'target_mode': target_mode,      # ac.target_mode (parsed)
                    'is_active': bool(row[10]),      # ac.is_active
                    'created_at': row[11],           # ac.created_at
                    'last_run': row[12],             # ac.last_run
                    'total_sends': row[13] or 0,     # ac.total_sends
                    'account_name': row[14]          # ta.account_name
                })
            return campaigns
    
    def get_campaign(self, campaign_id: int) -> Optional[Dict]:
        """Get specific campaign by ID"""
        import sqlite3
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT ac.id, ac.user_id, ac.account_id, ac.campaign_name, ac.ad_content, 
                       ac.target_chats, ac.schedule_type, ac.schedule_time, ac.buttons, 
                       ac.target_mode, ac.is_active, ac.immediate_start, ac.created_at, ac.last_run, 
                       ac.total_sends, ta.account_name
                FROM ad_campaigns ac
                LEFT JOIN telegram_accounts ta ON ac.account_id = ta.id
                WHERE ac.id = ?
            ''', (campaign_id,))
            row = cursor.fetchone()
            
            if row:
                # Parse ad_content (could be JSON string or plain string) - safer parsing
                try:
                    if row[4] and isinstance(row[4], str) and row[4].startswith(('[', '{')):
                        ad_content = json.loads(row[4])
                    else:
                        ad_content = str(row[4]) if row[4] else ""
                except (json.JSONDecodeError, AttributeError, TypeError):
                    ad_content = str(row[4]) if row[4] else ""
                
                # Parse target_chats (should be JSON string) - safer parsing
                try:
                    if row[5] and isinstance(row[5], str):
                        target_chats = json.loads(row[5])
                    elif isinstance(row[5], list):
                        target_chats = row[5]
                    else:
                        target_chats = [str(row[5])] if row[5] else []
                except (json.JSONDecodeError, TypeError):
                    target_chats = [str(row[5])] if row[5] else []
                
                # Parse buttons if they exist - much safer parsing
                buttons = []
                try:
                    if len(row) > 8 and row[8] is not None:
                        if isinstance(row[8], str) and row[8]:
                            buttons = json.loads(row[8])
                        elif isinstance(row[8], list):
                            buttons = row[8]
                except (json.JSONDecodeError, IndexError, TypeError):
                    buttons = []
                
                # Parse target_mode if it exists - safer parsing
                try:
                    target_mode = str(row[9]) if len(row) > 9 and row[9] else 'specific'
                except (IndexError, TypeError):
                    target_mode = 'specific'
                
                return {
                    'id': row[0],                    # ac.id
                    'user_id': row[1],               # ac.user_id  
                    'account_id': row[2],            # ac.account_id
                    'campaign_name': row[3],         # ac.campaign_name
                    'ad_content': ad_content,        # ac.ad_content (parsed)
                    'target_chats': target_chats,    # ac.target_chats (parsed)
                    'schedule_type': row[6],         # ac.schedule_type
                    'schedule_time': row[7],         # ac.schedule_time
                    'buttons': buttons,              # ac.buttons (parsed)
                    'target_mode': target_mode,      # ac.target_mode (parsed)
                    'is_active': bool(row[10]),      # ac.is_active
                    'immediate_start': bool(row[11]), # ac.immediate_start
                    'created_at': row[12],           # ac.created_at
                    'last_run': row[13],             # ac.last_run
                    'total_sends': row[14] or 0,     # ac.total_sends
                    'account_name': row[15]          # ta.account_name
                }
            return None
    
    def update_campaign(self, campaign_id: int, **kwargs):
        """Update campaign details with SQL injection protection"""
        import sqlite3
        
        # Strictly validate allowed fields to prevent SQL injection
        allowed_fields = {
            'campaign_name': str,
            'ad_content': (str, dict, list),
            'target_chats': (str, list),
            'schedule_type': str,
            'schedule_time': str,
            'is_active': bool
        }
        
        updates = []
        values = []
        
        for field, value in kwargs.items():
            # Validate field name
            if field not in allowed_fields:
                logger.warning(f"Attempted to update invalid field '{field}' for campaign {campaign_id}")
                continue
            
            # Validate field type
            expected_type = allowed_fields[field]
            if not isinstance(value, expected_type):
                logger.warning(f"Invalid type for field '{field}': expected {expected_type}, got {type(value)}")
                continue
            
            # Sanitize and prepare value
            if field == 'target_chats' and isinstance(value, list):
                value = json.dumps(value)
            elif field == 'ad_content' and isinstance(value, (dict, list)):
                value = json.dumps(value)
            elif field == 'is_active' and not isinstance(value, bool):
                value = bool(value)
            
            updates.append(f"{field} = ?")
            values.append(value)
        
        if not updates:
            logger.warning(f"No valid updates provided for campaign {campaign_id}")
            return False
        
        try:
            values.append(campaign_id)
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                # Use parameterized query to prevent SQL injection
                cursor.execute(f'''
                    UPDATE ad_campaigns 
                    SET {', '.join(updates)}
                    WHERE id = ?
                ''', values)
                conn.commit()
                
                if cursor.rowcount == 0:
                    logger.warning(f"No campaign found with ID {campaign_id}")
                    return False
                
                logger.info(f"Successfully updated campaign {campaign_id} with fields: {', '.join([u.split(' = ')[0] for u in updates])}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to update campaign {campaign_id}: {e}")
            return False
    
    def delete_campaign(self, campaign_id: int):
        """Permanently delete campaign from database and clean up scheduler"""
        import sqlite3
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Delete from ad_performance table first (foreign key constraint)
            cursor.execute('DELETE FROM ad_performance WHERE campaign_id = ?', (campaign_id,))
            
            # Delete from ad_campaigns table
            cursor.execute('DELETE FROM ad_campaigns WHERE id = ?', (campaign_id,))
            
            conn.commit()
            logger.info(f"Permanently deleted campaign {campaign_id} from database")
            
        # Remove from active campaigns
        if campaign_id in self.active_campaigns:
            del self.active_campaigns[campaign_id]
            logger.info(f"Removed campaign {campaign_id} from active campaigns")
        
        # Clean up scheduled jobs for this campaign
        import schedule
        jobs_to_remove = []
        for job in schedule.jobs:
            if hasattr(job, 'job_func') and hasattr(job.job_func, 'args') and job.job_func.args and job.job_func.args[0] == campaign_id:
                jobs_to_remove.append(job)
        
        for job in jobs_to_remove:
            schedule.cancel_job(job)
            logger.info(f"Cancelled scheduled job for campaign {campaign_id}")
        
        logger.info(f"Campaign {campaign_id} completely cleaned up")
    
    def initialize_telegram_client(self, account_id: int, cache_client: bool = False) -> Optional[TelegramClient]:
        """Initialize Telegram client - Thread-safe version for scheduler"""
        # Use thread-safe semaphore to prevent simultaneous client initialization
        with self.client_init_semaphore:
            try:
                # Always run in a separate thread to avoid event loop conflicts
                import concurrent.futures
                import threading
                
                # Check if we're in the main thread (where the bot runs)
                current_thread = threading.current_thread()
                is_main_thread = current_thread == threading.main_thread()
                
                if is_main_thread:
                    # We're in the main thread - use ThreadPoolExecutor to avoid blocking
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._sync_initialize_client, account_id, cache_client)
                        return future.result(timeout=30)  # 30 second timeout
                else:
                    # We're already in a background thread - run directly
                    return self._sync_initialize_client(account_id, cache_client)
                    
            except Exception as e:
                logger.error(f"Failed to initialize client for account {account_id}: {e}")
                return None
    
    def _sync_initialize_client(self, account_id: int, cache_client: bool = False) -> Optional[TelegramClient]:
        """Synchronous wrapper for client initialization in a new thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self._async_initialize_client(account_id, cache_client))
        finally:
            loop.close()
    
    async def _async_initialize_client(self, account_id: int, cache_client: bool = False) -> Optional[TelegramClient]:
        """Async helper for client initialization using telethon_manager (no interactive auth)"""
        # For scheduled executions, always create fresh client to avoid asyncio loop issues
        if cache_client and account_id in self.telegram_clients:
            # Update last used time for client memory management
            self.client_last_used[account_id] = time.time()
            return self.telegram_clients[account_id]
        
        account = self.db.get_account(account_id)
        if not account:
            logger.error(f"Account {account_id} not found")
            return None
        
        # 🎯 USE TELETHON_MANAGER: No more interactive authentication issues!
        logger.info(f"🔄 BUMP SERVICE: Using telethon_manager for account {account_id}")
        
        try:
            # Use the unified telethon manager (handles sessions properly)
            client = await telethon_manager.get_client(account)
            
            if not client:
                logger.error(f"❌ Failed to get client from telethon_manager for account {account_id}")
                return None
                
            # Only cache client if requested (not for scheduled executions)
            if cache_client:
                self.telegram_clients[account_id] = client
                # Track client usage for memory management
                self.client_last_used[account_id] = time.time()
                
            logger.info(f"✅ Telegram client initialized via telethon_manager (Account: {account_id})")
            
            # 🎯 AUTO-JOIN STORAGE CHANNEL: Ensure worker account can access storage channel
            try:
                from forwarder_config import Config
                storage_channel_id = Config.STORAGE_CHANNEL_ID
                
                if storage_channel_id:
                    logger.info(f"🔄 AUTO-JOIN: Ensuring worker account has access to storage channel {storage_channel_id}")
                    
                    # Convert string ID to integer for Telethon
                    try:
                        if isinstance(storage_channel_id, str):
                            if storage_channel_id.startswith('-100'):
                                # Full channel ID format: -1001234567890
                                channel_id_int = int(storage_channel_id)
                            elif storage_channel_id.startswith('-'):
                                # Short format: -1234567890, convert to full format
                                channel_id_int = int('-100' + storage_channel_id[1:])
                            else:
                                # Positive number, convert to negative channel ID
                                channel_id_int = int('-100' + storage_channel_id)
                        else:
                            channel_id_int = int(storage_channel_id)
                        
                        logger.info(f"🔄 Using channel ID: {channel_id_int}")
                        
                        # Try to get channel info with proper error handling
                        try:
                            storage_channel = await client.get_entity(channel_id_int)
                            logger.info(f"✅ Storage channel access confirmed: {storage_channel.title}")
                        except Exception as entity_error:
                            # Handle asyncio event loop issues
                            if "asyncio event loop" in str(entity_error):
                                logger.warning(f"⚠️ Event loop issue detected, retrying with fresh client...")
                                # Recreate client to avoid event loop conflicts
                                client = await telethon_manager.get_client(account)
                                if client:
                                    storage_channel = await client.get_entity(channel_id_int)
                                    logger.info(f"✅ Storage channel access confirmed after retry: {storage_channel.title}")
                                else:
                                    raise entity_error
                            else:
                                raise entity_error
                        
                    except Exception as access_error:
                        logger.warning(f"⚠️ Cannot access storage channel with ID {channel_id_int}: {access_error}")
                        
                        # 🔄 TELETHON SESSION REFRESH: If worker is member but Telethon can't find channel, refresh session
                        if "Cannot find any entity" in str(access_error):
                            logger.warning(f"🔄 TELETHON SESSION ISSUE: Worker is member but session cache is stale")
                            logger.warning(f"💡 SOLUTION: Force session refresh by getting dialogs")
                            
                            try:
                                # Force Telethon to refresh its entity cache by getting dialogs
                                logger.info(f"🔄 Refreshing Telethon session cache...")
                                dialogs = await client.get_dialogs(limit=50)
                                logger.info(f"✅ Session refreshed: Found {len(dialogs)} dialogs")
                                
                                # Try accessing storage channel again after refresh
                                storage_channel = await client.get_entity(channel_id_int)
                                logger.info(f"✅ Storage channel access confirmed after session refresh: {storage_channel.title}")
                                
                            except Exception as refresh_error:
                                logger.warning(f"❌ Session refresh failed: {refresh_error}")
                                
                                # Try alternative ID formats as fallback
                                alternative_ids = []
                                if isinstance(storage_channel_id, str) and storage_channel_id.startswith('-100'):
                                    # Try without -100 prefix
                                    alt_id = int(storage_channel_id[4:])  # Remove -100 prefix
                                    alternative_ids.append(alt_id)
                                    alternative_ids.append(-alt_id)  # Try negative version
                                
                                for alt_id in alternative_ids:
                                    try:
                                        logger.info(f"🔄 Trying alternative channel ID after refresh: {alt_id}")
                                        storage_channel = await client.get_entity(alt_id)
                                        logger.info(f"✅ Storage channel access confirmed with alternative ID {alt_id}: {storage_channel.title}")
                                        break
                                    except Exception as alt_error:
                                        logger.warning(f"❌ Alternative ID {alt_id} failed: {alt_error}")
                                else:
                                    logger.warning(f"❌ All channel access methods failed")
                                    logger.warning(f"💡 If worker account is a member, this is a Telethon session cache issue")
                                    logger.warning(f"💡 Consider restarting the service to refresh session files")
                        else:
                            logger.warning(f"❌ Channel access failed with non-entity error: {access_error}")
                else:
                    logger.info(f"⚠️ STORAGE_CHANNEL_ID not configured - skipping auto-join")
                    
            except Exception as storage_setup_error:
                logger.error(f"❌ Storage channel setup failed: {storage_setup_error}")
                # Continue anyway - this is not critical for basic functionality
            
            return client
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize client via telethon_manager for account {account_id}: {e}")
            return None
    
    def send_ad(self, campaign_id: int, wait_for_completion=False):
        """Send ad for a specific campaign with button support - Non-blocking by default"""
        try:
            import threading
            
            # Check if we're in the main thread
            current_thread = threading.current_thread()
            is_main_thread = current_thread == threading.main_thread()
            
            if is_main_thread:
                # We're in the main thread - use ThreadPoolExecutor to avoid blocking
                import concurrent.futures
                executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                future = executor.submit(self._sync_send_ad, campaign_id)
                
                if wait_for_completion:
                    # Only wait if explicitly requested (old behavior)
                    return future.result(timeout=60)
                else:
                    # Non-blocking: return immediately, campaign runs in background
                    logger.info(f"🚀 Campaign {campaign_id} started in background (non-blocking)")
                    return True
            else:
                # We're already in a background thread - run directly
                return self._sync_send_ad(campaign_id)
                
        except Exception as e:
            logger.error(f"Failed to send ad for campaign {campaign_id}: {e}")
            return False
    
    def _sync_send_ad(self, campaign_id: int):
        """Synchronous wrapper for send_ad in a new thread"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self._async_send_ad(campaign_id))
        finally:
            loop.close()
    
    async def _async_send_ad(self, campaign_id: int):
        """Async helper for send_ad"""
        logger.info(f"🚀 Starting _async_send_ad for campaign {campaign_id}")
        
        try:
            campaign = self.get_campaign(campaign_id)
            if not campaign:
                logger.error(f"❌ Campaign {campaign_id} not found!")
                return False
            
            logger.info(f"📋 Campaign found: {campaign['campaign_name']}")
            logger.info(f"👤 Account ID: {campaign['account_id']}")
            logger.info(f"🎯 Target chats: {campaign.get('target_chats', [])}")
            logger.info(f"🔘 Buttons: {len(campaign.get('buttons', []))} buttons")
            if not campaign or not campaign['is_active']:
                logger.warning(f"Campaign {campaign_id} not found or inactive")
                return
        except Exception as e:
            logger.error(f"🚨 Failed to get campaign {campaign_id}: {e}")
            return
        
        # Get account info for logging
        account = self.db.get_account(campaign['account_id'])
        account_name = account['account_name'] if account else f"Account_{campaign['account_id']}"
        account_id = campaign['account_id']
        
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info(f"🚀 CAMPAIGN START: {campaign['campaign_name']}")
        logger.info(f"👤 Using Account: {account_name} (ID: {account_id})")
        logger.info(f"🎯 Target Mode: {campaign.get('target_mode', 'unknown')}")
        logger.info(f"🔘 Buttons: {len(campaign.get('buttons', []))}")
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # 🛡️ ANTI-BAN SYSTEM: Pre-flight Checks
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        # Initialize tracking for this account
        self._init_account_tracking(account_id, account.get('created_at'))
        
        # 🆕 Check if account is in warm-up mode
        is_warmup, warmup_info = self._is_account_in_warmup(account_id)
        if is_warmup:
            days_remaining = warmup_info.get('days_remaining', 0)
            logger.warning(f"🆕 WARM-UP MODE ACTIVE for account {account_id}")
            logger.warning(f"   {days_remaining} days remaining")
            logger.warning(f"   Using conservative settings (slower delays, lower limits)")
        
        # Estimate number of messages to send
        target_chats = campaign['target_chats']
        if campaign.get('target_mode') == 'all_groups' or target_chats == ['ALL_WORKER_GROUPS']:
            # We'll check this after getting group list
            estimated_messages = 0
        else:
            estimated_messages = len(target_chats)
        
        # Check if account can send (if we have estimate)
        if estimated_messages > 0:
            can_send, reason = self._check_account_can_send(account_id, estimated_messages)
            if not can_send:
                logger.error(f"🛡️ ANTI-BAN BLOCK: {reason}")
                logger.error(f"❌ Campaign {campaign_id} aborted to protect account from ban")
                return False
        
        # Record campaign start
        self._record_campaign_start(account_id)
        logger.info(f"🛡️ ANTI-BAN: Campaign {campaign_id} passed pre-flight checks")
        
        # 🚨 Check peer flood status (pre-ban warning)
        is_blocked, flood_reason = self._check_peer_flood_status(account_id)
        if is_blocked:
            logger.error(f"⛔ PEER FLOOD BLOCK: {flood_reason}")
            logger.error(f"❌ Campaign {campaign_id} aborted - account in cooldown after peer flood")
            return False
        
        # YOLO MODE: Use fresh client for scheduled execution with aggressive retries
        # Maximum performance configuration with no compromises
        from forwarder_config import Config
        max_client_retries = getattr(Config, 'MAX_RETRY_ATTEMPTS', 5)  # YOLO MODE: 5 retries
        client = None
        
        for client_attempt in range(max_client_retries):
            try:
                client = await self._async_initialize_client(campaign['account_id'], cache_client=False)
                if client:
                    # Test client with a simple API call
                    await client.get_me()
                    logger.info(f"✅ Client initialized and tested successfully for {account_name}")
                    break
                else:
                    logger.warning(f"⚠️ Client initialization returned None (attempt {client_attempt + 1})")
            except Exception as client_error:
                logger.warning(f"⚠️ Client test failed (attempt {client_attempt + 1}): {client_error}")
                if client:
                    try:
                        await client.disconnect()
                    except:
                        pass
                client = None
            
            if client_attempt < max_client_retries - 1:
                await asyncio.sleep(3 * (client_attempt + 1))  # Progressive delay
        
        if not client:
            logger.error(f"❌ Failed to initialize {account_name} for campaign {campaign_id} after {max_client_retries} attempts")
            logger.error(f"💡 Solution: Re-add {account_name} with API credentials instead of uploaded session")
            return False
        
        # Get storage channel for forwarding
        storage_channel = None
        try:
            from forwarder_config import Config
            storage_channel_id = Config.STORAGE_CHANNEL_ID
            if storage_channel_id:
                storage_channel = await client.get_entity(int(storage_channel_id))
                logger.info(f"✅ Storage channel ready for forwarding: {storage_channel.title}")
        except Exception as e:
            logger.warning(f"⚠️ Could not get storage channel: {e}")
            storage_channel = None
        
        ad_content = campaign['ad_content']
        target_chats = campaign['target_chats']
        buttons = campaign.get('buttons', [])
        sent_count = 0
        
        # Button info for logging
        if buttons and len(buttons) > 0:
            logger.info(f"🔘 Campaign has {len(buttons)} button(s) configured")
            for btn in buttons:
                logger.info(f"   📎 {btn.get('text', '?')} -> {btn.get('url', '?')}")
        
        # Get all groups if target_mode is all_groups
        if campaign.get('target_mode') == 'all_groups' or target_chats == ['ALL_WORKER_GROUPS']:
            logger.info(f"🔍 DISCOVERY: Getting all groups for scheduled campaign {campaign_id}")
            logger.info(f"🔍 DISCOVERY: Account {campaign['account_id']} - fetching dialogs...")
            dialogs = await client.get_dialogs()
            logger.info(f"🔍 DISCOVERY: Retrieved {len(dialogs)} total dialogs from account")
            
            target_entities = []
            group_count = 0
            for dialog in dialogs:
                if dialog.is_group:
                    target_entities.append(dialog.entity)
                    group_count += 1
                    logger.info(f"✅ FOUND GROUP #{group_count}: {dialog.name} (ID: {dialog.id})")
            
            logger.info(f"🎯 DISCOVERY COMPLETE: Found {len(target_entities)} groups total for campaign {campaign_id}")
        else:
            # Convert chat IDs to entities
            target_entities = []
            for chat_id in target_chats:
                try:
                    entity = await client.get_entity(chat_id)
                    target_entities.append(entity)
                except Exception as e:
                    logger.error(f"Failed to get entity for {chat_id}: {e}")
        
        # HUMAN-LIKE BEHAVIOR: Slightly randomize group order to avoid patterns
        # Shuffle in small chunks to maintain some order but add variance
        if len(target_entities) > 10:
            logger.info(f"🎲 ANTI-DETECTION: Randomizing send order to appear more natural")
            # Shuffle groups in chunks of 5-10 to add randomness while keeping some locality
            chunk_size = random.randint(5, 10)
            for i in range(0, len(target_entities), chunk_size):
                chunk_end = min(i + chunk_size, len(target_entities))
                chunk = target_entities[i:chunk_end]
                random.shuffle(chunk)
                target_entities[i:chunk_end] = chunk
        
        # Create template message ONCE before processing all chats
        template_message_id = None
        
        # No template creation needed - send directly to target groups
        template_message_id = None
        
        # Initialize button tracking
        buttons_sent_count = 0
        failed_count = 0
        flood_retry_queue = []  # Groups that need retry after flood wait
        
        logger.info(f"📤 SENDING: About to send campaign {campaign_id} to {len(target_entities)} target groups")
        logger.info(f"🚀 HUMAN-LIKE FORWARDING: Sending to all groups")
        
        # ═══════════════════════════════════════════════════════════════════════════
        # EXACT COPY FROM ORIGINAL FORWARDER - CREATE BUTTONS FIRST
        # ═══════════════════════════════════════════════════════════════════════════
        telethon_buttons = None
        if buttons and len(buttons) > 0:
            try:
                button_rows = []
                current_row = []
                
                for i, btn in enumerate(buttons):
                    if btn.get('url'):
                        url = btn['url']
                        # Fix malformed URLs like "https:/example.com"
                        url = url.replace('https:/', 'https://').replace('http:/', 'http://')
                        # Remove duplicate protocols
                        url = url.replace('https://https://', 'https://').replace('http://http://', 'http://')
                        url = url.replace('https://http://', 'http://').replace('http://https://', 'https://')
                        # Add protocol if missing
                        if not url.startswith('http://') and not url.startswith('https://'):
                            url = 'https://' + url
                        telethon_button = Button.url(btn['text'], url)
                    else:
                        telethon_button = Button.inline(btn['text'], f"btn_{i}")
                    
                    current_row.append(telethon_button)
                    
                    if len(current_row) == 2 or i == len(buttons) - 1:
                        button_rows.append(current_row)
                        current_row = []
                
                telethon_buttons = button_rows
                logger.info(f"✅ Created {len(buttons)} buttons in {len(button_rows)} rows")
            except Exception as e:
                logger.error(f"❌ Error creating buttons: {e}")
                telethon_buttons = [[Button.url("Shop Now", "https://t.me/testukassdfdds")]]
        else:
            telethon_buttons = [[Button.url("Shop Now", "https://t.me/testukassdfdds")]]
            logger.info("Using default Shop Now button")
        
        # ═══════════════════════════════════════════════════════════════════════════
        # EXACT COPY FROM ORIGINAL FORWARDER bot.py - SEND MESSAGES
        # ═══════════════════════════════════════════════════════════════════════════
        for idx, chat_entity in enumerate(target_entities, 1):
            message = None
            try:
                logger.info(f"🚀 Sending to {chat_entity.title} ({idx}/{len(target_entities)})")
                
                # Check if ad_content is bridge channel format (like original forwarder)
                if isinstance(ad_content, dict) and ad_content.get('bridge_channel'):
                    # ORIGINAL FORWARDER FORMAT - bridge channel
                    bridge_channel_entity_id = ad_content.get('bridge_channel_entity')
                    bridge_message_id = ad_content.get('bridge_message_id')
                    
                    logger.info(f"🔗 Bridge channel: {bridge_channel_entity_id}, Message ID: {bridge_message_id}")
                    
                    try:
                        # Use storage_channel if it matches, otherwise fetch the entity
                        if storage_channel and str(bridge_channel_entity_id) == str(storage_channel_id):
                            bridge_entity = storage_channel
                            logger.info(f"✅ Using cached storage channel: {bridge_entity.title}")
                        else:
                            # Try to get entity - convert to int if string
                            try:
                                entity_id = int(bridge_channel_entity_id) if isinstance(bridge_channel_entity_id, str) else bridge_channel_entity_id
                                bridge_entity = await client.get_entity(entity_id)
                            except Exception as entity_err:
                                # Fallback: refresh dialogs and try again
                                logger.warning(f"⚠️ Entity not found, refreshing dialogs...")
                                await client.get_dialogs(limit=50)
                                entity_id = int(bridge_channel_entity_id) if isinstance(bridge_channel_entity_id, str) else bridge_channel_entity_id
                                bridge_entity = await client.get_entity(entity_id)
                            logger.info(f"✅ Bridge channel resolved: {getattr(bridge_entity, 'title', bridge_channel_entity_id)}")
                        
                        # Get original message
                        original_message = await client.get_messages(bridge_entity, ids=bridge_message_id)
                        if not original_message:
                            logger.error(f"❌ Message {bridge_message_id} not found")
                            failed_count += 1
                            continue
                        
                        # ═══════════════════════════════════════════════════════════════════════════
                        # EXACT COPY FROM ORIGINAL FORWARDER bump_service.py line 2560-2561
                        # "FORWARD the storage message to preserve InlineKeyboardMarkup buttons!"
                        # "This is how user accounts can send InlineKeyboardMarkup - by forwarding!"
                        # ═══════════════════════════════════════════════════════════════════════════
                        sent_msg = None
                        
                        # Check if this message was created by bot with inline buttons
                        is_bot_created = ad_content.get('bot_created_with_buttons', False)
                        
                        if is_bot_created:
                            # FORWARD the bot-created message - this preserves inline buttons!
                            logger.info(f"🔄 FORWARDING bot message with inline buttons to {chat_entity.title}")
                            try:
                                sent_msg = await client.forward_messages(
                                    entity=chat_entity,
                                    messages=bridge_message_id,
                                    from_peer=bridge_entity
                                )
                                logger.info(f"✅ Forwarded message WITH INLINE BUTTONS to {chat_entity.title}")
                            except Exception as fwd_err:
                                logger.warning(f"⚠️ Forward failed: {fwd_err}, falling back to send")
                                is_bot_created = False  # Fall through to send method
                        
                        if not is_bot_created:
                            # Fallback: Send with text-based buttons
                            message_text = original_message.message or ''
                            button_text = ""
                            for button_row in telethon_buttons:
                                for button in button_row:
                                    if hasattr(button, 'url'):
                                        button_text += f"\n\n🔗 {button.text}: {button.url}"
                            
                            final_message = (message_text or "") + button_text
                            
                            logger.info(f"📤 SENDING message with text buttons to {chat_entity.title}")
                            try:
                                if original_message.media:
                                    sent_msg = await client.send_file(
                                        chat_entity,
                                        original_message.media,
                                        caption=final_message,
                                        buttons=telethon_buttons
                                    )
                                else:
                                    sent_msg = await client.send_message(
                                        chat_entity,
                                        final_message,
                                        buttons=telethon_buttons
                                    )
                                logger.info(f"✅ Sent message with buttons to {chat_entity.title}")
                            except Exception as send_error:
                                logger.warning(f"⚠️ Send failed: {send_error}")
                                try:
                                    if original_message.media:
                                        sent_msg = await client.send_file(chat_entity, original_message.media, caption=final_message)
                                    else:
                                        sent_msg = await client.send_message(chat_entity, final_message)
                                    logger.info(f"✅ Sent with text buttons to {chat_entity.title}")
                                except Exception as fallback_error:
                                    logger.error(f"❌ Failed to send to {chat_entity.title}: {fallback_error}")
                        
                        if sent_msg:
                            sent_count += 1
                            buttons_sent_count += 1
                            msg_id = sent_msg[0].id if isinstance(sent_msg, list) else sent_msg.id
                            self.log_ad_performance(campaign_id, campaign['user_id'], str(chat_entity.id), msg_id)
                            self._record_message_sent(account_id)
                        else:
                            failed_count += 1
                            
                    except Exception as bridge_err:
                        logger.error(f"❌ Bridge channel error: {bridge_err}")
                        failed_count += 1
                    
                    # Short delay
                    await asyncio.sleep(random.uniform(0.5, 2.0))
                    continue
                
                # OLD FORMAT - list with linked_message (backwards compatibility)
                elif isinstance(ad_content, list) and ad_content:
                    for message_data in ad_content:
                        if message_data.get('type') == 'linked_message':
                            storage_chat = message_data.get('storage_chat_id')
                            storage_msg_id = message_data.get('storage_message_id')
                            
                            try:
                                storage_entity = await client.get_entity(int(storage_chat))
                                original_message = await client.get_messages(storage_entity, ids=int(storage_msg_id))
                                
                                if original_message:
                                    # Build button text (clickable links) from campaign buttons
                                    button_text = ""
                                    if buttons and len(buttons) > 0:
                                        button_text = "\n\n━━━━━━━━━━━━━━━━━"
                                        for btn in buttons:
                                            btn_text = btn.get('text', 'Click Here')
                                            btn_url = btn.get('url', '')
                                            if btn_url:
                                                if not btn_url.startswith('http://') and not btn_url.startswith('https://'):
                                                    btn_url = 'https://' + btn_url
                                                button_text += f"\n🔗 {btn_text}: {btn_url}"
                                    
                                    final_caption = (original_message.message or '') + button_text
                                    
                                    if original_message.media:
                                        sent_msg = await client.send_file(
                                            chat_entity, original_message.media,
                                            caption=final_caption, buttons=telethon_buttons)
                                    else:
                                        sent_msg = await client.send_message(
                                            chat_entity, final_caption, buttons=telethon_buttons)
                                    
                                    if sent_msg:
                                        sent_count += 1
                                        buttons_sent_count += 1
                                        logger.info(f"✅ Sent with buttons to {chat_entity.title}")
                            except Exception as e:
                                logger.error(f"❌ Error: {e}")
                                failed_count += 1
                            
                            await asyncio.sleep(random.uniform(0.5, 2.0))
                
            except Exception as send_error:
                logger.error(f"❌ Error sending to {chat_entity.title}: {send_error}")
                failed_count += 1
                await asyncio.sleep(random.uniform(1, 3))
                continue
        
        # Log completion - scheduler handles when to run next (no blocking delay here)
        if sent_count > 0:
            logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            logger.info(f"✅ Sent to {sent_count} groups successfully!")
            logger.info(f"⏰ Next run will be according to campaign schedule")
            logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        # Update campaign statistics
        self.update_campaign_stats(campaign_id, sent_count)
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        logger.info(f"✅ CAMPAIGN COMPLETE: {campaign['campaign_name']}")
        logger.info(f"📊 Results: {sent_count} sent successfully, {failed_count} failed out of {len(target_entities)} total groups")
        logger.info(f"📈 Success rate: {(sent_count/len(target_entities)*100) if len(target_entities) > 0 else 0:.1f}%")
        if len(flood_retry_queue) > 0:
            logger.info(f"♻️ All rate-limited groups were retried after waiting")
        logger.info(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        # Disconnect client after scheduled execution to prevent asyncio loop issues
        try:
            await client.disconnect()
            logger.info(f"Disconnected client for scheduled campaign {campaign_id}")
        except Exception as e:
            logger.warning(f"Failed to disconnect client for campaign {campaign_id}: {e}")
        
        # MULTI-USERBOT: Execute for additional accounts with delays
        await self._execute_additional_accounts(campaign_id, campaign)
    
    async def _execute_additional_accounts(self, campaign_id: int, campaign: dict):
        """Execute campaign for additional accounts with spam avoidance"""
        try:
            additional_accounts = campaign.get('additional_accounts')
            if not additional_accounts:
                return
                
            try:
                additional_accounts_data = json.loads(additional_accounts) if isinstance(additional_accounts, str) else additional_accounts
                if not additional_accounts_data:
                    return
                    
                logger.info(f"🚀 MULTI-USERBOT: Found {len(additional_accounts_data)} additional accounts for campaign {campaign_id}")
                
                for account_config in additional_accounts_data:
                    account_id = account_config.get('account_id')
                    delay_minutes = account_config.get('delay_minutes', 0)
                    content_variation_index = account_config.get('content_variation', 0)
                    
                    if not account_id:
                        continue
                        
                    if delay_minutes > 0:
                        logger.info(f"🕐 MULTI-USERBOT: Scheduling account {account_id} with {delay_minutes} minute delay")
                        # Schedule the additional account execution
                        asyncio.create_task(self._execute_delayed_account(campaign_id, account_id, delay_minutes, content_variation_index))
                    else:
                        # Execute immediately for this additional account
                        await self._execute_single_additional_account(campaign_id, account_id, content_variation_index)
                        
            except (json.JSONDecodeError, Exception) as e:
                logger.error(f"❌ Error processing additional accounts: {e}")
                
        except Exception as e:
            logger.error(f"❌ Error in _execute_additional_accounts: {e}")
    
    async def _execute_delayed_account(self, campaign_id: int, account_id: int, delay_minutes: int, content_variation_index: int = 0):
        """Execute campaign for an additional account after delay"""
        try:
            # Apply spam avoidance timing variation
            import random
            base_delay = delay_minutes * 60
            spam_variation = random.randint(0, 300)  # 0-5 minutes additional variation
            total_delay = base_delay + spam_variation
            
            logger.info(f"🕐 MULTI-USERBOT: Waiting {total_delay/60:.1f} minutes for account {account_id} (base: {delay_minutes}m + spam avoidance: {spam_variation/60:.1f}m)")
            await asyncio.sleep(total_delay)
            
            logger.info(f"🚀 MULTI-USERBOT: Executing delayed campaign for account {account_id}")
            await self._execute_single_additional_account(campaign_id, account_id, content_variation_index)
            
        except Exception as e:
            logger.error(f"❌ Error in delayed execution for account {account_id}: {e}")
    
    async def _execute_single_additional_account(self, campaign_id: int, account_id: int, content_variation_index: int = 0):
        """Execute campaign for a single additional account with spam avoidance"""
        start_time = time.time()
        execution_log = {
            'campaign_id': campaign_id,
            'account_id': account_id,
            'content_variation_used': content_variation_index,
            'groups_count': 0,
            'success_count': 0,
            'delay_applied_minutes': 0
        }
        
        try:
            # Get campaign data
            campaign = self.get_campaign(campaign_id)
            if not campaign or not campaign['is_active']:
                logger.error(f"❌ Campaign {campaign_id} not found or inactive for additional account {account_id}")
                return
                
            # Get account info
            account = self.db.get_account(account_id)
            if not account:
                logger.error(f"❌ Additional account {account_id} not found")
                return
                
            account_name = account['account_name']
            logger.info(f"🚀 MULTI-USERBOT: Executing campaign '{campaign['campaign_name']}' for additional account '{account_name}'")
            
            # Apply spam avoidance
            spam_delay = await self._apply_spam_avoidance_timing(campaign)
            execution_log['delay_applied_minutes'] = spam_delay
            
            # Get content variation
            content_variation = self._get_content_variation(campaign, content_variation_index)
            
            # Initialize client for this account
            client = await self._async_initialize_client(account_id)
            if not client:
                logger.error(f"❌ Failed to initialize client for additional account {account_id}")
                return
                
            try:
                # Get target groups for this account
                target_entities = await self._get_account_groups(client, campaign)
                execution_log['groups_count'] = len(target_entities)
                
                if not target_entities:
                    logger.warning(f"⚠️ No target groups found for additional account {account_name}")
                    return
                    
                logger.info(f"🎯 MULTI-USERBOT: Found {len(target_entities)} groups for account {account_name}")
                
                # Execute forwarding for each group
                success_count = 0
                for chat_entity in target_entities:
                    try:
                        # Apply per-message spam avoidance delay
                        await self._apply_per_message_delay()
                        
                        # Forward the message (same logic as main account)
                        await self._forward_campaign_message(client, chat_entity, campaign, content_variation)
                        success_count += 1
                        logger.info(f"✅ MULTI-USERBOT: Sent to {chat_entity.title} via {account_name}")
                        
                    except Exception as msg_error:
                        logger.error(f"❌ MULTI-USERBOT: Failed to send to {chat_entity.title} via {account_name}: {msg_error}")
                        
                execution_log['success_count'] = success_count
                logger.info(f"🎯 MULTI-USERBOT: Account {account_name} completed: {success_count}/{len(target_entities)} messages sent")
                
            finally:
                # Disconnect client
                try:
                    await client.disconnect()
                    logger.info(f"🔌 MULTI-USERBOT: Disconnected client for {account_name}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to disconnect client for {account_name}: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error executing additional account {account_id}: {e}")
        finally:
            # Log execution
            self._log_campaign_execution(execution_log)
            duration = time.time() - start_time
            logger.info(f"⏱️ MULTI-USERBOT: Account {account_id} execution completed in {duration:.2f}s")
    
    async def _apply_spam_avoidance_timing(self, campaign: dict) -> float:
        """Apply spam avoidance timing delays"""
        import random
        
        spam_avoidance_enabled = campaign.get('spam_avoidance_enabled', True)
        timing_variation = campaign.get('timing_variation_minutes', 5)
        
        if not spam_avoidance_enabled:
            logger.info("📵 SPAM AVOIDANCE: Disabled for this campaign")
            return 0
            
        if timing_variation <= 0:
            return 0
            
        # Apply random delay (0 to timing_variation minutes)
        delay_seconds = random.randint(0, timing_variation * 60)
        delay_minutes = delay_seconds / 60
        
        logger.info(f"⏱️ SPAM AVOIDANCE: Applying random delay of {delay_minutes:.1f} minutes")
        await asyncio.sleep(delay_seconds)
        
        return delay_minutes
    
    async def _apply_per_message_delay(self):
        """Apply small delay between messages to avoid spam detection"""
        import random
        delay = random.uniform(1, 3)  # 1-3 seconds between messages
        await asyncio.sleep(delay)
    
    def _get_content_variation(self, campaign: dict, variation_index: int = 0):
        """Get content variation for spam avoidance"""
        content_variations = campaign.get('content_variations')
        if not content_variations:
            return None
            
        try:
            variations = json.loads(content_variations) if isinstance(content_variations, str) else content_variations
            if variations and len(variations) > variation_index:
                selected_variation = variations[variation_index]
                logger.info(f"📝 SPAM AVOIDANCE: Using content variation {variation_index + 1}/{len(variations)}")
                return selected_variation
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"❌ Error processing content variations: {e}")
            
        return None
    
    async def _get_account_groups(self, client, campaign: dict):
        """Get target groups for a specific account"""
        target_entities = []
        target_chats = campaign.get('target_chats', [])
        target_mode = campaign.get('target_mode', 'specific')
        
        if target_mode == 'all_groups' or 'ALL_WORKER_GROUPS' in target_chats:
            # Get all groups this account is member of
            try:
                async for dialog in client.iter_dialogs():
                    if dialog.is_group or dialog.is_channel:
                        if hasattr(dialog.entity, 'broadcast') and not dialog.entity.broadcast:
                            target_entities.append(dialog.entity)
                        elif not hasattr(dialog.entity, 'broadcast'):
                            target_entities.append(dialog.entity)
            except Exception as e:
                logger.error(f"❌ Error getting groups for account: {e}")
        else:
            # Get specific groups
            for chat in target_chats:
                try:
                    if chat == 'ALL_WORKER_GROUPS':
                        continue
                    entity = await client.get_entity(chat)
                    target_entities.append(entity)
                except Exception as e:
                    logger.warning(f"⚠️ Could not get entity {chat}: {e}")
                    
        return target_entities
    
    async def _forward_campaign_message(self, client, chat_entity, campaign: dict, content_variation=None):
        """Forward campaign message to a specific chat"""
        try:
            ad_content = campaign.get('ad_content', [])
            if not ad_content:
                return
            
            # Process each message in ad_content
            for message_data in ad_content:
                if message_data.get('type') == 'linked_message':
                    storage_message_id = int(message_data.get('storage_message_id'))
                    storage_chat_id = message_data.get('storage_chat_id')
                    
                    # Use content variation if available
                    if content_variation and content_variation.get('storage_message_id'):
                        storage_message_id = int(content_variation['storage_message_id'])
                        # Also update storage_chat_id if provided in variation
                        if content_variation.get('storage_chat_id'):
                            storage_chat_id = content_variation['storage_chat_id']
                        logger.info(f"📝 Using variation message {storage_message_id}")
                    
                    # Get storage channel entity
                    storage_channel_entity = None
                    if storage_chat_id:
                        try:
                            storage_channel_entity = await client.get_entity(int(storage_chat_id))
                            logger.debug(f"🔧 MULTI-USERBOT: Got storage channel entity for forwarding")
                        except Exception as entity_error:
                            logger.error(f"❌ MULTI-USERBOT: Failed to get storage channel entity: {entity_error}")
                            # Try fallback with Config
                            try:
                                from forwarder_config import Config
                                storage_channel_id = Config.STORAGE_CHANNEL_ID
                                if storage_channel_id:
                                    storage_channel_entity = await client.get_entity(int(storage_channel_id))
                            except Exception as fallback_error:
                                logger.error(f"❌ MULTI-USERBOT: Fallback storage channel failed: {fallback_error}")
                                continue
                    
                    if not storage_channel_entity:
                        logger.error(f"❌ MULTI-USERBOT: No storage channel entity available")
                        continue
                    
                    # 🎭 ADVANCED ANTI-BAN: Simulate human behavior for multi-userbot sends
                    await self._simulate_read_receipts(client, additional_account['account_id'], chat_entity)
                    await self._simulate_typing(client, chat_entity, 100)
                    
                    # Forward the message directly
                    sent_msg = await client.forward_messages(
                        entity=chat_entity,
                        messages=storage_message_id,
                        from_peer=storage_channel_entity
                    )
                    
                    if sent_msg:
                        logger.debug(f"✅ Forwarded message to {chat_entity.title}")
                    else:
                        logger.error(f"❌ Failed to forward message to {chat_entity.title}")
                        
        except Exception as e:
            logger.error(f"❌ Error forwarding message: {e}")
            raise
    
    def _log_campaign_execution(self, execution_log: dict):
        """Log campaign execution for analytics"""
        try:
            with self._get_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO campaign_execution_logs 
                    (campaign_id, account_id, content_variation_used, groups_count, 
                     success_count, delay_applied_minutes)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    execution_log['campaign_id'],
                    execution_log['account_id'], 
                    execution_log['content_variation_used'],
                    execution_log['groups_count'],
                    execution_log['success_count'],
                    execution_log['delay_applied_minutes']
                ))
                conn.commit()
        except Exception as e:
            logger.error(f"❌ Error logging campaign execution: {e}")
    
    def log_ad_performance(self, campaign_id: int, user_id: int, target_chat: str, 
                          message_id: Optional[int], status: str = 'sent'):
        """Log ad performance"""
        import sqlite3
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO ad_performance 
                (campaign_id, user_id, target_chat, message_id, status)
                VALUES (?, ?, ?, ?, ?)
            ''', (campaign_id, user_id, target_chat, message_id, status))
            conn.commit()
    
    def update_campaign_stats(self, campaign_id: int, sent_count: int):
        """Update campaign statistics"""
        import sqlite3
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE ad_campaigns 
                SET last_run = CURRENT_TIMESTAMP, total_sends = total_sends + ?
                WHERE id = ?
            ''', (sent_count, campaign_id))
            conn.commit()
    
    def schedule_campaign(self, campaign_id: int):
        """Schedule a campaign based on its schedule type"""
        campaign = self.get_campaign(campaign_id)
        if not campaign:
            return
        
        schedule_type = campaign['schedule_type']
        schedule_time = campaign['schedule_time']
        
        if schedule_type == 'daily':
            schedule.every().day.at(schedule_time).do(self.run_campaign_job, campaign_id)
        elif schedule_type == 'weekly':
            # Assuming format like "Monday 14:30"
            day, time_str = schedule_time.split(' ')
            getattr(schedule.every(), day.lower()).at(time_str).do(self.run_campaign_job, campaign_id)
        elif schedule_type == 'hourly':
            job = schedule.every().hour.do(self.run_campaign_job, campaign_id)
            # Only run immediately if this is a new campaign with immediate_start=True
            # Existing campaigns loaded from database should not run immediately
            if campaign.get('is_active', False) and campaign.get('immediate_start', False):
                logger.info(f"🚀 Running campaign {campaign_id} immediately on hourly schedule activation")
                self.run_campaign_job(campaign_id)
            else:
                logger.info(f"📅 Campaign {campaign_id} scheduled for hourly execution (no immediate start)")
        elif schedule_type == 'custom':
            # Parse custom interval (e.g., "every 3 minutes", "every 4 hours")
            try:
                if 'hour' in schedule_time.lower():
                    hours = int(schedule_time.split()[1])
                    job = schedule.every(hours).hours.do(self.run_campaign_job, campaign_id)
                    
                    # Only run immediately if this is a new campaign with immediate_start=True
                    # Existing campaigns loaded from database should not run immediately
                    campaign = self.get_campaign(campaign_id)
                    if campaign and campaign.get('is_active', False) and campaign.get('immediate_start', False):
                        logger.info(f"🚀 Running campaign {campaign_id} immediately on schedule activation")
                        # Add staggered delay to prevent database conflicts
                        import random
                        delay = random.uniform(0.5, 2.0)  # Random delay between 0.5-2 seconds
                        # Run in a separate thread to avoid blocking
                        import threading
                        threading.Thread(target=lambda: (time.sleep(delay), self.run_campaign_job(campaign_id)), daemon=True).start()
                    else:
                        logger.info(f"📅 Campaign {campaign_id} scheduled for custom execution (no immediate start)")
                    
                    logger.info(f"📅 Campaign {campaign_id} scheduled every {hours} hours")
                elif 'minute' in schedule_time.lower():
                    # Handle formats like "3 minutes", "every 3 minutes"
                    parts = schedule_time.split()
                    if len(parts) >= 2:
                        # Find the number in the string
                        for part in parts:
                            if part.isdigit():
                                minutes = int(part)
                                break
                        else:
                            minutes = 10  # default
                    else:
                        minutes = 10  # default
                    
                    # Schedule the job to run every X minutes
                    job = schedule.every(minutes).minutes.do(self.run_campaign_job, campaign_id)
                    
                    # IMPORTANT: Run the job immediately for the first time if campaign is active AND immediate_start is True
                    campaign = self.get_campaign(campaign_id)
                    if campaign and campaign.get('is_active', False) and campaign.get('immediate_start', False):
                        logger.info(f"🚀 Running campaign {campaign_id} immediately on schedule activation")
                        # Add staggered delay to prevent database conflicts
                        import random
                        delay = random.uniform(0.5, 2.0)  # Random delay between 0.5-2 seconds
                        # Run in a separate thread to avoid blocking
                        import threading
                        threading.Thread(target=lambda: (time.sleep(delay), self.run_campaign_job(campaign_id)), daemon=True).start()
                    else:
                        logger.info(f"📅 Campaign {campaign_id} scheduled for first run (no immediate start)")
                    
                    logger.info(f"📅 Campaign {campaign_id} scheduled every {minutes} minutes")
                elif schedule_time.isdigit():
                    # If just a number, assume minutes
                    minutes = int(schedule_time)
                    job = schedule.every(minutes).minutes.do(self.run_campaign_job, campaign_id)
                    
                    # IMPORTANT: Run the job immediately for the first time if campaign is active AND immediate_start is True
                    campaign = self.get_campaign(campaign_id)
                    if campaign and campaign.get('is_active', False) and campaign.get('immediate_start', False):
                        logger.info(f"🚀 Running campaign {campaign_id} immediately on schedule activation")
                        # Add staggered delay to prevent database conflicts
                        import random
                        delay = random.uniform(0.5, 2.0)  # Random delay between 0.5-2 seconds
                        # Run in a separate thread to avoid blocking
                        import threading
                        threading.Thread(target=lambda: (time.sleep(delay), self.run_campaign_job(campaign_id)), daemon=True).start()
                    else:
                        logger.info(f"📅 Campaign {campaign_id} scheduled for first run (no immediate start)")
                    
                    logger.info(f"📅 Campaign {campaign_id} scheduled every {minutes} minutes")
                else:
                    logger.warning(f"⚠️ Unknown custom schedule format: {schedule_time}")
            except (ValueError, IndexError) as e:
                logger.error(f"❌ Error parsing custom schedule '{schedule_time}': {e}")
                # Default to 10 minutes if parsing fails
                schedule.every(10).minutes.do(self.run_campaign_job, campaign_id)
                logger.info(f"📅 Campaign {campaign_id} defaulted to every 10 minutes")
        
        self.active_campaigns[campaign_id] = campaign
        logger.info(f"Scheduled campaign {campaign_id} ({schedule_type} at {schedule_time})")
    
    def run_campaign_job(self, campaign_id: int):
        """Execute scheduled campaign automatically - Queue-based for 50+ accounts with smart staggering"""
        try:
            import datetime
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"🔄 Scheduler triggered campaign {campaign_id} at {current_time}")
            
            # 🎯 SMART STAGGER: Apply delay if this campaign is part of a staggered group
            if hasattr(self, 'campaign_stagger_delays') and campaign_id in self.campaign_stagger_delays:
                stagger_delay = self.campaign_stagger_delays[campaign_id]
                stagger_minutes = stagger_delay / 60
                
                logger.info(f"⏰ SMART STAGGER: Campaign {campaign_id} has {stagger_minutes:.0f}-minute delay")
                logger.info(f"⏳ Waiting {stagger_minutes:.0f} minutes before starting (accounts sharing same message)")
                
                # Wait the stagger delay
                time.sleep(stagger_delay)
                
                logger.info(f"✅ Stagger delay complete! Starting campaign {campaign_id} now")
            
            # Get campaign from database
            campaign = self.get_campaign(campaign_id)
            if not campaign:
                logger.warning(f"Campaign {campaign_id} not found for scheduled execution - removing from active campaigns")
                # Remove from active campaigns if campaign doesn't exist
                if campaign_id in self.active_campaigns:
                    del self.active_campaigns[campaign_id]
                return
                
            if not campaign.get('is_active', False):
                logger.warning(f"Campaign {campaign_id} is not active, removing from active campaigns")
                # Remove inactive campaigns from active campaigns
                if campaign_id in self.active_campaigns:
                    del self.active_campaigns[campaign_id]
                return
            
            # Log campaign details
            logger.info(f"📋 Campaign {campaign_id}: {campaign['campaign_name']}")
            logger.info(f"📅 Schedule: {campaign['schedule_type']} at {campaign['schedule_time']}")
            logger.info(f"👤 Account ID: {campaign['account_id']}")
            
            # Check account status
            account = self.db.get_account(campaign['account_id'])
            if not account:
                logger.error(f"❌ Account {campaign['account_id']} not found for campaign {campaign_id}")
                return
            
            if not account.get('session_string'):
                logger.error(f"❌ Account {campaign['account_id']} has no session string")
                return
            
            logger.info(f"✅ Account {account.get('account_name', 'Unknown')} is ready")
            
            # Add to execution queue (worker threads will process it)
            queue_size = self.execution_queue.qsize()
            logger.info(f"📥 Adding campaign {campaign_id} to execution queue (current queue size: {queue_size})")
            self.execution_queue.put(campaign_id)
            logger.info(f"✅ Campaign {campaign_id} added to queue successfully")
            
        except Exception as e:
            logger.error(f"Error in campaign scheduler for {campaign_id}: {e}")
    
    def cleanup_corrupted_sessions(self):
        """Clean up any corrupted session files"""
        import os
        import glob
        
        try:
            # Find all bump session files
            session_files = glob.glob("bump_session_*.session")
            cleaned_count = 0
            
            for session_file in session_files:
                try:
                    # Check if file is empty or corrupted
                    if os.path.getsize(session_file) == 0:
                        os.remove(session_file)
                        cleaned_count += 1
                        logger.info(f"Cleaned up empty session file: {session_file}")
                except Exception as e:
                    logger.warning(f"Could not clean up session file {session_file}: {e}")
            
            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} corrupted session files")
                
        except Exception as e:
            logger.warning(f"Error during session cleanup: {e}")

    def start_scheduler(self):
        """Start the campaign scheduler with proper background thread"""
        logger.info("🚀 Bump service scheduler started (automatic execution mode)")
        
        # Clean up any corrupted session files
        self.cleanup_corrupted_sessions()
        
        # Load existing campaigns into memory
        self.load_existing_campaigns()
        
        # Start background scheduler thread
        def scheduler_worker():
            """Background worker that runs scheduled campaigns"""
            logger.info("📅 Scheduler worker thread started")
            last_log_time = time.time()
            while self.is_running:
                try:
                    # Log scheduler status every 60 seconds
                    current_time = time.time()
                    if current_time - last_log_time >= 60:
                        jobs = schedule.get_jobs()
                        logger.info(f"⏰ Scheduler status: {len(jobs)} active jobs, {len(self.active_campaigns)} active campaigns")
                        # Log details about each job
                        for job in jobs:
                            next_run = job.next_run.strftime("%H:%M:%S") if job.next_run else "Not scheduled"
                            logger.info(f"  📅 Job scheduled for: {next_run}")
                        last_log_time = current_time
                    
                    # Run pending scheduled jobs
                    schedule.run_pending()
                    time.sleep(1)  # Check every second
                except Exception as e:
                    logger.error(f"Error in scheduler worker: {e}")
                    time.sleep(5)  # Wait 5 seconds on error
            logger.info("📅 Scheduler worker thread stopped")
        
        # Start the scheduler thread
        self.scheduler_thread = threading.Thread(target=scheduler_worker, daemon=True)
        self.scheduler_thread.start()
        logger.info("✅ Background scheduler thread started successfully")
    
    def stop_scheduler(self):
        """Stop the campaign scheduler"""
        self.is_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        schedule.clear()
        logger.info("Bump service scheduler stopped")
    
    def _calculate_smart_stagger_delay(self, account_count: int) -> int:
        """
        Calculate stagger delay in minutes based on number of accounts sharing same message.
        
        2 accounts: 30-minute gaps
        3 accounts: 25-minute gaps
        4 accounts: 15-minute gaps
        5+ accounts: 10-minute gaps
        """
        if account_count <= 1:
            return 0  # No stagger needed for single account
        elif account_count == 2:
            return 30  # 30 minutes between accounts
        elif account_count == 3:
            return 25  # 25 minutes between accounts
        elif account_count == 4:
            return 15  # 15 minutes between accounts
        else:  # 5 or more
            return 10  # 10 minutes between accounts
    
    def load_existing_campaigns(self):
        """Load and schedule existing active campaigns with smart staggering"""
        import sqlite3
        from forwarder_config import Config
        from collections import defaultdict
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Get all active campaigns with their content (to group by same message)
            cursor.execute('''
                SELECT id, campaign_name, schedule_time, account_id, ad_content
                FROM ad_campaigns 
                WHERE is_active = 1
                ORDER BY id
            ''')
            rows = cursor.fetchall()
            
            if not rows:
                logger.info("✅ No active campaigns to load")
                return
            
            # Group campaigns by their message content (campaigns with same content = same message)
            message_groups = defaultdict(list)
            for row in rows:
                campaign_id, campaign_name, schedule_time, account_id, ad_content = row
                
                # Create unique key based on ad_content (first 100 chars to avoid huge keys)
                # Campaigns with identical content are assumed to be sharing the same message
                content_key = str(ad_content)[:100] if ad_content else f"campaign_{campaign_id}"
                
                message_groups[content_key].append({
                    'id': campaign_id,
                    'name': campaign_name,
                    'schedule': schedule_time,
                    'account_id': account_id
                })
            
            logger.info(f"📊 Found {len(rows)} active campaigns grouped into {len(message_groups)} message types")
            
            # Schedule campaigns with smart staggering
            total_campaigns_loaded = 0
            for content_key, campaigns in message_groups.items():
                account_count = len(campaigns)
                stagger_minutes = self._calculate_smart_stagger_delay(account_count)
                
                logger.info(f"📬 Message group: {account_count} accounts sending same content, {stagger_minutes}-min stagger")
                
                for index, campaign in enumerate(campaigns):
                    campaign_id = campaign['id']
                    campaign_name = campaign['name']
                    schedule_time = campaign['schedule']
                    
                    # Calculate stagger delay for this campaign
                    stagger_delay_seconds = index * stagger_minutes * 60  # Convert minutes to seconds
                    
                    if stagger_delay_seconds > 0:
                        logger.info(f"⏰ Campaign {campaign_id} ({campaign_name}): Will start {index * stagger_minutes} min after first account")
                    else:
                        logger.info(f"🚀 Campaign {campaign_id} ({campaign_name}): First account, starts immediately")
                    
                    # Schedule the campaign
                    self.schedule_campaign(campaign_id)
                    
                    # Apply stagger delay if this is not the first campaign in the group
                    if stagger_delay_seconds > 0 and Config.ENABLE_AUTO_STAGGER:
                        # Store the stagger delay in memory for runtime execution
                        if not hasattr(self, 'campaign_stagger_delays'):
                            self.campaign_stagger_delays = {}
                        self.campaign_stagger_delays[campaign_id] = stagger_delay_seconds
                        logger.debug(f"📝 Stored {stagger_delay_seconds}s stagger delay for campaign {campaign_id}")
                    
                    total_campaigns_loaded += 1
            
            logger.info(f"✅ Loaded {total_campaigns_loaded} campaigns with smart staggering")
            
            # Log stagger summary
            if hasattr(self, 'campaign_stagger_delays') and self.campaign_stagger_delays:
                total_stagger = sum(self.campaign_stagger_delays.values())
                logger.info(f"🎯 Smart stagger enabled: Total spread of {total_stagger/60:.1f} minutes across all campaigns")
    
    def get_campaign_performance(self, campaign_id: int) -> Dict[str, Any]:
        """Get performance statistics for a campaign"""
        import sqlite3
        
        with self._get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_attempts,
                    SUM(CASE WHEN status = 'sent' THEN 1 ELSE 0 END) as successful_sends,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_sends
                FROM ad_performance 
                WHERE campaign_id = ?
            ''', (campaign_id,))
            row = cursor.fetchone()
            
            return {
                'total_attempts': row[0] or 0,
                'successful_sends': row[1] or 0,
                'failed_sends': row[2] or 0,
                'success_rate': (row[1] / row[0] * 100) if row[0] > 0 else 0
            }
    
    def add_additional_account_to_campaign(self, campaign_id: int, account_id: int, delay_minutes: int = 0, content_variation_index: int = 0):
        """Add additional userbot to existing campaign"""
        try:
            campaign = self.get_campaign(campaign_id)
            if not campaign:
                logger.error(f"❌ Campaign {campaign_id} not found")
                return False
                
            # Get existing additional accounts
            additional_accounts = campaign.get('additional_accounts', '[]')
            try:
                additional_accounts_data = json.loads(additional_accounts) if isinstance(additional_accounts, str) else (additional_accounts or [])
            except (json.JSONDecodeError, TypeError):
                additional_accounts_data = []
            
            # Check if account already exists
            for account in additional_accounts_data:
                if account.get('account_id') == account_id:
                    logger.warning(f"⚠️ Account {account_id} already exists in campaign {campaign_id}")
                    return False
            
            # Add new account
            new_account_config = {
                'account_id': account_id,
                'delay_minutes': delay_minutes,
                'content_variation': content_variation_index
            }
            additional_accounts_data.append(new_account_config)
            
            # Update campaign
            self.update_campaign(campaign_id, additional_accounts=json.dumps(additional_accounts_data))
            
            logger.info(f"✅ Added account {account_id} to campaign {campaign_id} with {delay_minutes}m delay")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error adding additional account: {e}")
            return False
    
    def add_content_variation_to_campaign(self, campaign_id: int, storage_message_id: int, variation_name: str = ""):
        """Add content variation for spam avoidance"""
        try:
            campaign = self.get_campaign(campaign_id)
            if not campaign:
                logger.error(f"❌ Campaign {campaign_id} not found")
                return False
                
            # Get existing variations
            content_variations = campaign.get('content_variations', '[]')
            try:
                variations_data = json.loads(content_variations) if isinstance(content_variations, str) else (content_variations or [])
            except (json.JSONDecodeError, TypeError):
                variations_data = []
            
            # Add new variation
            new_variation = {
                'storage_message_id': storage_message_id,
                'name': variation_name or f"Variation {len(variations_data) + 1}",
                'created_at': time.strftime("%Y-%m-%d %H:%M:%S")
            }
            variations_data.append(new_variation)
            
            # Update campaign
            self.update_campaign(campaign_id, content_variations=json.dumps(variations_data))
            
            logger.info(f"✅ Added content variation '{new_variation['name']}' to campaign {campaign_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error adding content variation: {e}")
            return False
    
    def update_spam_avoidance_settings(self, campaign_id: int, enabled: bool = True, timing_variation_minutes: int = 5):
        """Update spam avoidance settings for campaign"""
        try:
            self.update_campaign(
                campaign_id, 
                spam_avoidance_enabled=enabled,
                timing_variation_minutes=timing_variation_minutes
            )
            
            status = "enabled" if enabled else "disabled"
            logger.info(f"✅ Spam avoidance {status} for campaign {campaign_id} (variation: {timing_variation_minutes}m)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error updating spam avoidance settings: {e}")
            return False
    
    async def close(self):
        """Close all connections"""
        self.stop_scheduler()
        
        for account_id, client in self.telegram_clients.items():
            try:
                await client.disconnect()
                logger.info(f"Disconnected bump service client for account {account_id}")
            except Exception as e:
                logger.error(f"Error disconnecting client {account_id}: {e}")
        
        self.telegram_clients.clear()
