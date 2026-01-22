"""
Unified Telethon Client Manager
Handles both storage creation and message forwarding with proper session management
"""

import asyncio
import logging
import os
import time
from typing import Optional, Dict, Any, List
from telethon import TelegramClient
from telethon.tl.types import MessageEntityCustomEmoji, MessageEntityBold, MessageEntityItalic, MessageEntityMention
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError

logger = logging.getLogger(__name__)

class TelethonManager:
    """Unified Telethon client manager for storage and forwarding operations"""
    
    def __init__(self):
        self.clients: Dict[str, TelegramClient] = {}
        # Use persistent disk if available, otherwise local directory
        if os.path.exists('/data'):
            self.session_dir = "/data/sessions"
        else:
            self.session_dir = "sessions"
        os.makedirs(self.session_dir, exist_ok=True)
    
    async def get_client(self, account_data: Dict[str, Any]) -> Optional[TelegramClient]:
        """Get or create a Telethon client for the given account with improved error handling"""
        account_id = str(account_data['id'])
        
        # Check if existing client is still valid
        if account_id in self.clients:
            client = self.clients[account_id]
            try:
                # Test if client is still authorized and connected
                if client.is_connected() and await client.is_user_authorized():
                    # Test with a simple API call to ensure it's working
                    await client.get_me()
                    logger.info(f"✅ Existing client for account {account_id} is valid and authorized")
                    return client
                else:
                    logger.warning(f"⚠️ Existing client for account {account_id} is not authorized, recreating...")
                    await client.disconnect()
                    del self.clients[account_id]
            except Exception as e:
                logger.warning(f"⚠️ Existing client for account {account_id} failed test: {e}, recreating...")
                try:
                    await client.disconnect()
                except:
                    pass
                del self.clients[account_id]
        
        try:
            # Check if we have a stored session string
            if account_data.get('session_string'):
                # Use StringSession for headless environments
                from telethon.sessions import StringSession
                session_str = account_data['session_string']
                
                # Validate session string
                logger.info(f"🔧 DEBUG: Account {account_id} session_string type: {type(session_str)}")
                logger.info(f"🔧 DEBUG: Account {account_id} session_string length: {len(session_str) if session_str else 'None'}")
                logger.info(f"🔧 DEBUG: Account {account_id} session_string preview: {repr(session_str[:50]) if session_str else 'None'}")
                
                if not session_str or not isinstance(session_str, str):
                    logger.error(f"❌ Invalid session_string for account {account_id}: {type(session_str)} - {repr(session_str)}")
                    return None
                
                # Clean session string (remove whitespace)
                session_str = session_str.strip()
                
                if not session_str:
                    logger.error(f"❌ Empty session_string for account {account_id}")
                    return None
                
                try:
                    # Check if session_str is base64 encoded session data
                    if session_str.startswith('U1FMaXRlIGZvcm1hdCAz') or len(session_str) > 1000:
                        logger.info(f"🔄 Detected base64 session data for account {account_id}, converting to session file")
                        # This is base64 encoded session data, not a StringSession string
                        import base64
                        session_name = f"unified_{account_id}"
                        session_path = os.path.join(self.session_dir, f"{session_name}.session")
                        
                        # Decode and write session data to file
                        try:
                            session_data = base64.b64decode(session_str)
                            with open(session_path, 'wb') as f:
                                f.write(session_data)
                            
                            # Use session file instead of StringSession
                            client = TelegramClient(
                                session_path,
                                account_data['api_id'],
                                account_data['api_hash']
                            )
                            logger.info(f"✅ Created client from base64 session data for account {account_id}")
                        except Exception as decode_error:
                            logger.error(f"❌ Failed to decode base64 session data for account {account_id}: {decode_error}")
                            return None
                    else:
                        # This is a proper StringSession string
                        client = TelegramClient(
                            StringSession(session_str),
                            account_data['api_id'],
                            account_data['api_hash']
                        )
                        logger.info(f"✅ Created client from StringSession for account {account_id}")
                        
                except Exception as session_error:
                    logger.error(f"❌ Failed to create client for account {account_id}: {session_error}")
                    # Try fallback to session_data if available
                    if account_data.get('session_data'):
                        logger.info(f"🔄 Trying fallback to session_data for account {account_id}")
                        # Fall through to session_data handling below
                        pass  
                    else:
                        return None
            
            # Handle session_data (either primary or fallback)
            if not account_data.get('session_string') or 'session_error' in locals():
                if account_data.get('session_data'):
                    # Use existing session file
                    import base64
                    session_name = f"unified_{account_id}"
                    session_path = os.path.join(self.session_dir, f"{session_name}.session")
                    
                    # Write session data to file
                    try:
                        session_data = base64.b64decode(account_data['session_data'])
                        with open(session_path, 'wb') as f:
                            f.write(session_data)
                        
                        client = TelegramClient(
                            session_path.replace('.session', ''),
                            account_data['api_id'],
                            account_data['api_hash']
                        )
                    except Exception as data_error:
                        logger.error(f"❌ Failed to create client from session_data for account {account_id}: {data_error}")
                        return None
                else:
                    logger.error(f"❌ No valid session data available for account {account_id}")
                    return None
            
            # Connect with retry mechanism
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await client.connect()
                    logger.info(f"✅ Client connected successfully (attempt {attempt + 1}/{max_retries})")
                    break
                except Exception as connect_error:
                    logger.warning(f"⚠️ Connection attempt {attempt + 1}/{max_retries} failed: {connect_error}")
                    if attempt == max_retries - 1:
                        logger.error(f"❌ Failed to connect after {max_retries} attempts")
                        return None
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
            
            # Ensure client is properly initialized for cross-context usage
            try:
                # Check if already authorized first
                if not await client.is_user_authorized():
                    logger.error(f"❌ Account {account_id} is not authorized - cannot authenticate in headless environment")
                    await client.disconnect()
                    return None
                
                # Test the client by getting self info to ensure it's working
                me = await client.get_me()
                logger.info(f"✅ Client connected and authorized for {me.first_name} (ID: {me.id})")
            except Exception as test_error:
                logger.error(f"❌ Client connection test failed for account {account_id}: {test_error}")
                await client.disconnect()
                return None
            
            # Store client for reuse
            self.clients[account_id] = client
            logger.info(f"✅ Created unified Telethon client for account {account_data['account_name']}")
            
            return client
            
        except Exception as e:
            logger.error(f"❌ Failed to create Telethon client for account {account_id}: {e}")
            return None
    
    async def create_storage_message(self, account_data: Dict[str, Any], storage_channel_id: int, 
                                   media_data: Dict[str, Any], bot_instance=None) -> Optional[Dict[str, Any]]:
        """Create a storage message using Telethon with proper custom emoji handling"""
        try:
            client = await self.get_client(account_data)
            if not client:
                return None
            
            # Convert Bot API entities to Telethon entities
            telethon_entities = self._convert_entities_to_telethon(media_data.get('caption_entities', []))
            
            # For now, let's use a simpler approach - forward the original message
            # This preserves all entities and custom emojis perfectly
            if media_data.get('original_message_id') and media_data.get('original_chat_id'):
                # Forward the original message to storage channel
                original_chat = await client.get_entity(media_data['original_chat_id'])
                sent_message = await client.forward_messages(
                    entity=storage_channel_id,
                    messages=media_data['original_message_id'],
                    from_peer=original_chat
                )
                
                if sent_message:
                    message = sent_message[0] if isinstance(sent_message, list) else sent_message
                    logger.info(f"✅ Forwarded original message to storage: ID {message.id}")
                    
                    return {
                        'storage_message_id': message.id,
                        'storage_chat_id': storage_channel_id,
                        'client': client
                    }
            
            # Fallback: Create new message (this won't preserve custom emojis perfectly)
            logger.warning("⚠️ Using fallback message creation - custom emojis may not be preserved")
            
            # Send text message with entities
            sent_message = await client.send_message(
                entity=storage_channel_id,
                message=media_data.get('caption', ''),
                formatting_entities=telethon_entities,
                parse_mode=None
            )
            
            logger.info(f"✅ Created storage message with Telethon: ID {sent_message.id}")
            
            return {
                'storage_message_id': sent_message.id,
                'storage_chat_id': storage_channel_id,
                'client': client
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to create storage message with Telethon: {e}")
            return None
    
    async def forward_storage_message(self, client: TelegramClient, target_chat_id: int, 
                                    storage_message_id: int, storage_channel_id: int) -> bool:
        """Forward a storage message to target chat using the same client with enhanced error handling"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Verify client is still connected and authorized
                if not client.is_connected():
                    logger.warning(f"Client not connected, attempting to reconnect (attempt {attempt + 1})")
                    await client.connect()
                
                if not await client.is_user_authorized():
                    logger.error(f"❌ Client not authorized for forwarding (attempt {attempt + 1})")
                    return False
                
                # Get storage channel entity with retry
                try:
                    storage_channel = await client.get_entity(storage_channel_id)
                except Exception as entity_error:
                    logger.warning(f"Failed to get storage channel entity: {entity_error}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        return False
                
                # Get target chat entity with retry
                try:
                    target_entity = await client.get_entity(target_chat_id)
                except Exception as target_error:
                    logger.warning(f"Failed to get target entity {target_chat_id}: {target_error}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        return False
                
                # Forward the message with retry
                forwarded_messages = await client.forward_messages(
                    entity=target_entity,
                    messages=storage_message_id,
                    from_peer=storage_channel
                )
                
                if forwarded_messages:
                    logger.info(f"✅ Forwarded storage message {storage_message_id} to {target_chat_id}")
                    return True
                else:
                    logger.warning(f"❌ No messages forwarded from storage (attempt {attempt + 1})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    else:
                        return False
                    
            except FloodWaitError as flood_error:
                wait_time = flood_error.seconds
                logger.warning(f"⏳ FloodWaitError: waiting {wait_time} seconds before retry")
                await asyncio.sleep(wait_time)
                continue
                
            except Exception as e:
                logger.error(f"❌ Failed to forward storage message (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    continue
                else:
                    return False
        
        return False
    
    def _convert_entities_to_telethon(self, bot_entities: List[Dict[str, Any]]) -> List:
        """Convert Bot API entities to Telethon entities"""
        telethon_entities = []
        
        for entity_data in bot_entities:
            try:
                entity_type = entity_data['type']
                if hasattr(entity_type, 'value'):
                    entity_type = entity_type.value
                elif hasattr(entity_type, 'name'):
                    entity_type = entity_type.name.lower()
                
                if entity_type == 'custom_emoji':
                    entity = MessageEntityCustomEmoji(
                        offset=entity_data['offset'],
                        length=entity_data['length'],
                        document_id=int(entity_data['custom_emoji_id'])
                    )
                elif entity_type == 'bold':
                    entity = MessageEntityBold(
                        offset=entity_data['offset'],
                        length=entity_data['length']
                    )
                elif entity_type == 'italic':
                    entity = MessageEntityItalic(
                        offset=entity_data['offset'],
                        length=entity_data['length']
                    )
                elif entity_type == 'mention':
                    entity = MessageEntityMention(
                        offset=entity_data['offset'],
                        length=entity_data['length']
                    )
                else:
                    continue
                
                telethon_entities.append(entity)
                
            except Exception as e:
                logger.warning(f"Failed to convert entity: {e}")
                continue
        
        return telethon_entities
    
    async def _get_media_file(self, media_data: Dict[str, Any]) -> Optional[str]:
        """Download media file for Telethon upload"""
        try:
            # For now, we'll use the Bot API to download the file
            # In a full implementation, we'd need to pass the bot instance
            # This is a simplified version that returns the file_id for now
            return media_data.get('file_id')
        except Exception as e:
            logger.error(f"Failed to get media file: {e}")
            return None
    
    async def validate_and_reconnect_client(self, account_id: str, client: TelegramClient) -> bool:
        """Validate client and reconnect if necessary"""
        try:
            # Check if client is connected
            if not client.is_connected():
                logger.info(f"🔄 Client {account_id} not connected, attempting to reconnect...")
                await client.connect()
            
            # Check if client is authorized
            if not await client.is_user_authorized():
                logger.error(f"❌ Client {account_id} not authorized")
                return False
            
            # Test with a simple API call
            await client.get_me()
            logger.info(f"✅ Client {account_id} validation successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ Client {account_id} validation failed: {e}")
            return False
    
    async def get_validated_client(self, account_data: Dict[str, Any]) -> Optional[TelegramClient]:
        """Get a validated client, recreating if necessary"""
        account_id = str(account_data['id'])
        
        # Try to get existing client and validate it
        if account_id in self.clients:
            client = self.clients[account_id]
            if await self.validate_and_reconnect_client(account_id, client):
                return client
            else:
                # Remove invalid client
                logger.warning(f"⚠️ Removing invalid client for account {account_id}")
                try:
                    await client.disconnect()
                except:
                    pass
                del self.clients[account_id]
        
        # Create new client if needed
        return await self.get_client(account_data)
    
    async def cleanup(self):
        """Cleanup all clients"""
        for client in self.clients.values():
            try:
                await client.disconnect()
            except:
                pass
        self.clients.clear()

# Global instance
telethon_manager = TelethonManager()
