"""
Database Management Module
"""
import sqlite3
import json
import os
from typing import Dict, List, Optional
from forwarder_config import Config

class Database:
    def __init__(self, db_path: str = None):
        # Use persistent disk if available, otherwise local storage
        if db_path is None:
            # Check for Render persistent disk mount (same as main bot uses)
            if os.path.exists('/mnt/data'):
                self.db_path = '/mnt/data/auto_ads.db'
            else:
                self.db_path = 'auto_ads.db'
        else:
            self.db_path = db_path
        
        # Ensure directory exists (only if path contains directory)
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.init_database()
    
    def _get_connection(self):
        """Get database connection with proper configuration"""
        conn = sqlite3.connect(
            self.db_path,
            timeout=30.0,
            check_same_thread=False,
            isolation_level=None
        )
        # Enable WAL mode for better concurrent access
        conn.execute('PRAGMA journal_mode=WAL')
        # Set busy timeout to handle locks better
        conn.execute('PRAGMA busy_timeout=30000')
        return conn
    
    def init_database(self):
        """Initialize database tables with WAL mode for better concurrency"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Enable WAL mode for better concurrency
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA cache_size=10000")
            cursor.execute("PRAGMA temp_store=MEMORY")
            
            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Telegram accounts
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS telegram_accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    account_name TEXT,
                    phone_number TEXT,
                    api_id TEXT,
                    api_hash TEXT,
                    session_string TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                )
            ''')
            
            # Forwarding configurations
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS forwarding_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    account_id INTEGER,
                    source_chat_id TEXT,
                    destination_chat_id TEXT,
                    config_name TEXT,
                    config_data TEXT,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    FOREIGN KEY (account_id) REFERENCES telegram_accounts (id)
                )
            ''')
            
            # Message logs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS message_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    account_id INTEGER,
                    source_message_id INTEGER,
                    destination_message_id INTEGER,
                    source_chat_id TEXT,
                    destination_chat_id TEXT,
                    forwarded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    FOREIGN KEY (account_id) REFERENCES telegram_accounts (id)
                )
            ''')
            
            conn.commit()
    
    def add_user(self, user_id: int, username: str = None, first_name: str = None, last_name: str = None):
        """Add or update user"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO users (user_id, username, first_name, last_name)
                VALUES (?, ?, ?, ?)
            ''', (user_id, username, first_name, last_name))
            conn.commit()
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user by ID"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
            row = cursor.fetchone()
            if row:
                return {
                    'user_id': row[0],
                    'username': row[1],
                    'first_name': row[2],
                    'last_name': row[3],
                    'is_active': row[4],
                    'created_at': row[5]
                }
            return None
    
    def add_telegram_account(self, user_id: int, account_name: str, phone_number: str, 
                           api_id: str, api_hash: str, session_string: str = None) -> int:
        """Add Telegram account"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO telegram_accounts 
                (user_id, account_name, phone_number, api_id, api_hash, session_string)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, account_name, phone_number, api_id, api_hash, session_string))
            conn.commit()
            return cursor.lastrowid
    
    def get_user_accounts(self, user_id: int) -> List[Dict]:
        """Get all Telegram accounts for a user"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM telegram_accounts 
                WHERE user_id = ? AND is_active = 1
                ORDER BY created_at DESC
            ''', (user_id,))
            rows = cursor.fetchall()
            return [{
                'id': row[0],
                'user_id': row[1],
                'account_name': row[2],
                'phone_number': row[3],
                'api_id': row[4],
                'api_hash': row[5],
                'session_string': row[6],
                'is_active': row[7],
                'created_at': row[8]
            } for row in rows]
    
    def get_account(self, account_id: int) -> Optional[Dict]:
        """Get account by ID with retry logic for database locks"""
        import time
        import random
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                with self._get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('SELECT * FROM telegram_accounts WHERE id = ?', (account_id,))
                    row = cursor.fetchone()
                    if row:
                        return {
                            'id': row[0],
                            'user_id': row[1],
                            'account_name': row[2],
                            'phone_number': row[3],
                            'api_id': row[4],
                            'api_hash': row[5],
                            'session_string': row[6],
                            'is_active': row[7],
                            'created_at': row[8]
                        }
                    return None
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    # Wait with exponential backoff + jitter
                    wait_time = (2 ** attempt) + random.uniform(0.1, 0.5)
                    print(f"Database locked, retrying in {wait_time:.2f}s (attempt {attempt + 1}/{max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        return None
    
    def update_account_session(self, account_id: int, session_string: str):
        """Update account session string"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE telegram_accounts 
                SET session_string = ?
                WHERE id = ?
            ''', (session_string, account_id))
            conn.commit()
    
    def delete_account(self, account_id: int):
        """Delete Telegram account and clean up all related data"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Get account info before deletion for logging
            cursor.execute('SELECT account_name, phone_number FROM telegram_accounts WHERE id = ?', (account_id,))
            account_info = cursor.fetchone()
            
            # Completely remove the account record (not just deactivate)
            cursor.execute('DELETE FROM telegram_accounts WHERE id = ?', (account_id,))
            
            # Also clean up related data
            # Delete any forwarding configs using this account
            cursor.execute('DELETE FROM forwarding_configs WHERE account_id = ?', (account_id,))
            
            # Delete any campaigns using this account
            cursor.execute('DELETE FROM ad_campaigns WHERE account_id = ?', (account_id,))
            
            # Delete any message logs for this account
            cursor.execute('DELETE FROM message_logs WHERE account_id = ?', (account_id,))
            
            conn.commit()
            
            if account_info:
                print(f"✅ Completely deleted account '{account_info[0]}' ({account_info[1]}) and all related data")
            else:
                print(f"✅ Deleted account {account_id} and all related data")
    
    def add_forwarding_config(self, user_id: int, account_id: int, source_chat_id: str, 
                            destination_chat_id: str, config_name: str, config_data: Dict) -> int:
        """Add forwarding configuration"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO forwarding_configs 
                (user_id, account_id, source_chat_id, destination_chat_id, config_name, config_data)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, account_id, source_chat_id, destination_chat_id, config_name, json.dumps(config_data)))
            conn.commit()
            return cursor.lastrowid
    
    def get_user_configs(self, user_id: int, account_id: int = None) -> List[Dict]:
        """Get all forwarding configurations for a user"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if account_id:
                cursor.execute('''
                    SELECT fc.*, ta.account_name 
                    FROM forwarding_configs fc
                    LEFT JOIN telegram_accounts ta ON fc.account_id = ta.id
                    WHERE fc.user_id = ? AND fc.account_id = ? AND fc.is_active = 1
                    ORDER BY fc.created_at DESC
                ''', (user_id, account_id))
            else:
                cursor.execute('''
                    SELECT fc.*, ta.account_name 
                    FROM forwarding_configs fc
                    LEFT JOIN telegram_accounts ta ON fc.account_id = ta.id
                    WHERE fc.user_id = ? AND fc.is_active = 1
                    ORDER BY fc.created_at DESC
                ''', (user_id,))
            rows = cursor.fetchall()
            return [{
                'id': row[0],
                'user_id': row[1],
                'account_id': row[2],
                'source_chat_id': row[3],
                'destination_chat_id': row[4],
                'config_name': row[5],
                'config_data': json.loads(row[6]),
                'is_active': row[7],
                'created_at': row[8],
                'account_name': row[9]
            } for row in rows]
    
    def update_config(self, config_id: int, config_data: Dict):
        """Update forwarding configuration"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE forwarding_configs 
                SET config_data = ?
                WHERE id = ?
            ''', (json.dumps(config_data), config_id))
            conn.commit()
    
    def delete_config(self, config_id: int):
        """Delete forwarding configuration"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE forwarding_configs SET is_active = 0 WHERE id = ?', (config_id,))
            conn.commit()
    
    def log_message(self, user_id: int, account_id: int, source_message_id: int, 
                   destination_message_id: int, source_chat_id: str, destination_chat_id: str):
        """Log forwarded message"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO message_logs 
                (user_id, account_id, source_message_id, destination_message_id, source_chat_id, destination_chat_id)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, account_id, source_message_id, destination_message_id, source_chat_id, destination_chat_id))
            conn.commit()
    
    def get_campaign(self, campaign_id: int) -> Optional[Dict]:
        """Get a campaign by ID"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT c.*, a.account_name 
                FROM ad_campaigns c
                LEFT JOIN telegram_accounts a ON c.account_id = a.id
                WHERE c.id = ?
            ''', (campaign_id,))
            
            row = cursor.fetchone()
            if row:
                columns = [description[0] for description in cursor.description]
                campaign = dict(zip(columns, row))
                
                # Parse JSON fields
                if campaign.get('ad_content'):
                    try:
                        campaign['ad_content'] = json.loads(campaign['ad_content'])
                    except json.JSONDecodeError:
                        campaign['ad_content'] = {}
                
                if campaign.get('target_chats'):
                    try:
                        campaign['target_chats'] = json.loads(campaign['target_chats'])
                    except json.JSONDecodeError:
                        campaign['target_chats'] = []
                
                if campaign.get('buttons'):
                    try:
                        campaign['buttons'] = json.loads(campaign['buttons'])
                    except json.JSONDecodeError:
                        campaign['buttons'] = []
                
                return campaign
            return None
    
    def update_campaign_last_run(self, campaign_id: int):
        """Update the last run time for a campaign"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE ad_campaigns 
                SET last_run = CURRENT_TIMESTAMP,
                    total_sends = total_sends + 1
                WHERE id = ?
            ''', (campaign_id,))
            conn.commit()
    
    def update_campaign_storage_message_id(self, campaign_id: int, new_storage_message_id: int):
        """Update the storage message ID in a campaign's ad_content"""
        import json
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Get the current ad_content
            cursor.execute('SELECT ad_content FROM ad_campaigns WHERE id = ?', (campaign_id,))
            row = cursor.fetchone()
            if not row:
                return False
            
            ad_content_str = row[0]
            if not ad_content_str:
                return False
            
            try:
                # Parse the JSON
                ad_content = json.loads(ad_content_str)
                
                # Update the storage_message_id
                ad_content['storage_message_id'] = new_storage_message_id
                
                # Convert back to JSON
                updated_ad_content_str = json.dumps(ad_content)
                
                # Update the database
                cursor.execute('''
                    UPDATE ad_campaigns 
                    SET ad_content = ?
                    WHERE id = ?
                ''', (updated_ad_content_str, campaign_id))
                conn.commit()
                
                return True
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error updating campaign storage message ID: {e}")
                return False
