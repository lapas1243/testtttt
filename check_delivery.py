#!/usr/bin/env python3
"""
Diagnostic tool to check if messages are actually being delivered.
Tests sending to a group and verifies the message appears.
"""
import asyncio
import sys
from telethon import TelegramClient, errors
from telethon.tl.types import Message
from forwarder_database import Database
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_account_delivery(account_id: int, test_chat_username: str):
    """
    Test if an account can actually deliver messages (not shadow banned).
    
    Args:
        account_id: Database ID of the account to test
        test_chat_username: Username of a test group (e.g., @yourgroup)
    """
    db = Database()
    account = db.get_account(account_id)
    
    if not account:
        logger.error(f"Account {account_id} not found!")
        return False
    
    logger.info(f"🔍 Testing delivery for account: {account['account_name']}")
    logger.info(f"📱 Phone: {account['phone_number']}")
    logger.info(f"🎯 Test group: {test_chat_username}")
    
    try:
        # Initialize client
        client = TelegramClient(
            f"sessions/{account['session_name']}",
            account['api_id'],
            account['api_hash']
        )
        
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.error("❌ Account not authorized! Need to re-login.")
            return False
        
        # Get the test chat
        try:
            test_entity = await client.get_entity(test_chat_username)
            logger.info(f"✅ Found test group: {test_entity.title}")
        except Exception as e:
            logger.error(f"❌ Cannot access test group: {e}")
            logger.error(f"💡 Make sure the account is a member of {test_chat_username}")
            return False
        
        # Send a test message
        import random
        import datetime
        test_id = random.randint(1000, 9999)
        test_message = f"🔍 Delivery Test #{test_id}\nTimestamp: {datetime.datetime.now().strftime('%H:%M:%S')}\n\n⚠️ Testing message delivery - please confirm if you see this!"
        
        logger.info(f"📤 Sending test message #{test_id}...")
        
        try:
            sent_msg = await client.send_message(test_entity, test_message)
            
            if not sent_msg:
                logger.error("❌ CRITICAL: API returned None - message not sent!")
                return False
            
            logger.info(f"✅ API says message sent (ID: {sent_msg.id})")
            
            # Wait a moment for message to propagate
            await asyncio.sleep(3)
            
            # Try to verify the message exists by reading it back
            logger.info(f"🔍 Verifying message delivery...")
            
            try:
                # Get recent messages from the chat
                messages = await client.get_messages(test_entity, limit=10)
                
                # Check if our message is there
                found_message = None
                for msg in messages:
                    if msg.id == sent_msg.id:
                        found_message = msg
                        break
                
                if found_message:
                    logger.info(f"✅ SUCCESS: Message verified in chat!")
                    logger.info(f"📊 Message text: {found_message.message[:100]}...")
                    logger.info(f"🎯 ACCOUNT IS NOT SHADOW BANNED")
                    
                    # Try to delete the test message (cleanup)
                    try:
                        await client.delete_messages(test_entity, [sent_msg.id])
                        logger.info(f"🧹 Test message deleted")
                    except:
                        logger.info(f"💡 Could not delete test message - you may need to delete manually")
                    
                    return True
                else:
                    logger.error(f"⚠️ WARNING: Message sent but NOT FOUND in chat!")
                    logger.error(f"🚨 This indicates SHADOW BAN or MESSAGE FILTERING")
                    logger.error(f"💡 Messages from this account may not be visible to users!")
                    return False
                    
            except Exception as verify_error:
                logger.warning(f"⚠️ Could not verify message: {verify_error}")
                logger.warning(f"💡 Message may have been sent, but verification failed")
                logger.warning(f"🔍 Manually check the test group to confirm delivery")
                return None  # Unknown status
        
        except errors.ChatWriteForbiddenError:
            logger.error(f"❌ FORBIDDEN: Account cannot write to this chat!")
            logger.error(f"💡 Account may be restricted or banned in this group")
            return False
            
        except errors.UserBannedInChannelError:
            logger.error(f"❌ BANNED: Account is banned in this channel!")
            return False
            
        except errors.PeerFloodError:
            logger.error(f"🚨 PEER FLOOD: Account has sent too many messages!")
            logger.error(f"💡 This account needs to rest for 24+ hours")
            return False
            
        except Exception as send_error:
            logger.error(f"❌ Send error: {send_error}")
            return False
    
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return False
    
    finally:
        if client:
            await client.disconnect()


async def check_all_accounts(test_chat_username: str):
    """Check delivery status for all accounts."""
    db = Database()
    accounts = db.get_all_accounts_for_user(1)  # Assuming user_id=1
    
    if not accounts:
        logger.error("No accounts found!")
        return
    
    logger.info(f"🔍 Testing {len(accounts)} accounts...")
    logger.info(f"🎯 Test group: {test_chat_username}")
    logger.info("=" * 60)
    
    results = {
        'working': [],
        'shadow_banned': [],
        'restricted': [],
        'unknown': []
    }
    
    for account in accounts:
        account_id = account['id']
        account_name = account['account_name']
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing: {account_name}")
        logger.info(f"{'='*60}")
        
        result = await test_account_delivery(account_id, test_chat_username)
        
        if result is True:
            results['working'].append(account_name)
            logger.info(f"✅ {account_name}: WORKING")
        elif result is False:
            results['shadow_banned'].append(account_name)
            logger.error(f"⚠️ {account_name}: SHADOW BANNED / RESTRICTED")
        else:
            results['unknown'].append(account_name)
            logger.warning(f"❓ {account_name}: UNKNOWN (manual check needed)")
        
        # Wait between tests
        await asyncio.sleep(5)
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info(f"📊 DELIVERY TEST SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"✅ Working accounts: {len(results['working'])}")
    for acc in results['working']:
        logger.info(f"   - {acc}")
    
    logger.info(f"\n⚠️ Shadow banned / Restricted: {len(results['shadow_banned'])}")
    for acc in results['shadow_banned']:
        logger.error(f"   - {acc}")
    
    logger.info(f"\n❓ Unknown status: {len(results['unknown'])}")
    for acc in results['unknown']:
        logger.warning(f"   - {acc}")
    
    logger.info(f"\n{'='*60}")
    
    if results['shadow_banned']:
        logger.error(f"🚨 {len(results['shadow_banned'])} accounts need attention!")
        logger.error(f"💡 These accounts are sending messages but users can't see them")
        logger.error(f"💡 Solutions:")
        logger.error(f"   1. Stop using these accounts for 48-72 hours")
        logger.error(f"   2. Enable warm-up mode: python manage_accounts.py warmup <account_id>")
        logger.error(f"   3. Reduce message frequency significantly")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  Test single account:  python check_delivery.py <account_id> <test_group_username>")
        print("  Test all accounts:    python check_delivery.py all <test_group_username>")
        print("")
        print("Example:")
        print("  python check_delivery.py 1 @mytestgroup")
        print("  python check_delivery.py all @mytestgroup")
        sys.exit(1)
    
    if sys.argv[1].lower() == 'all':
        if len(sys.argv) < 3:
            print("❌ Error: Please provide test group username")
            print("Usage: python check_delivery.py all @yourgroup")
            sys.exit(1)
        
        test_group = sys.argv[2]
        asyncio.run(check_all_accounts(test_group))
    else:
        if len(sys.argv) < 3:
            print("❌ Error: Please provide test group username")
            print("Usage: python check_delivery.py <account_id> @yourgroup")
            sys.exit(1)
        
        account_id = int(sys.argv[1])
        test_group = sys.argv[2]
        asyncio.run(test_account_delivery(account_id, test_group))

