"""
Auto Ads System - Admin Integration
Integrates forwarder/bump service into the main shop bot admin panel
"""

import logging
import os
import base64
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telethon import TelegramClient
from telethon.sessions import StringSession

from utils import is_primary_admin, send_message_with_retry
from forwarder_database import Database as ForwarderDatabase
from bump_service import BumpService

logger = logging.getLogger(__name__)

# Global instances
_forwarder_db = None
_bump_service = None
_user_sessions = {}

def get_forwarder_db():
    global _forwarder_db
    if _forwarder_db is None:
        _forwarder_db = ForwarderDatabase()
    return _forwarder_db

def get_bump_service(bot_instance=None):
    global _bump_service
    if _bump_service is None:
        _bump_service = BumpService(bot_instance=bot_instance)
        _bump_service.start_scheduler()
        logger.info("📢 Auto Ads bump service initialized")
    return _bump_service

# ============================================================================
# AUTO ADS MAIN MENU
# ============================================================================

async def handle_auto_ads_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show Auto Ads main menu"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    text = """📢 **Auto Ads System**

*Automated Telegram Advertising Platform*

**Features:**
• 👥 Multi-Account Management
• 📢 Campaign Automation (Bump Service)
• ⚡ Smart Scheduling
• 🛡️ Anti-Ban Protection

Select an option below:"""
    
    keyboard = [
        [InlineKeyboardButton("👥 Manage Accounts", callback_data="auto_ads_accounts")],
        [InlineKeyboardButton("📢 Campaigns", callback_data="auto_ads_campaigns")],
        [InlineKeyboardButton("➕ Create Campaign", callback_data="auto_ads_new_campaign")],
        [InlineKeyboardButton("📊 Statistics", callback_data="auto_ads_stats")],
        [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_menu")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ============================================================================
# ACCOUNT MANAGEMENT
# ============================================================================

async def handle_auto_ads_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show managed Telegram accounts"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    db = get_forwarder_db()
    user_id = query.from_user.id
    
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        text = """👥 **Telegram Accounts**

No accounts configured yet.

To add an account, you need:
• API ID & API Hash from my.telegram.org
• Phone number
• Session string (from Telethon)"""
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Account", callback_data="auto_ads_add_account")],
            [InlineKeyboardButton("🔙 Back", callback_data="auto_ads_menu")]
        ]
    else:
        text = f"👥 **Telegram Accounts** ({len(accounts)})\n\n"
        keyboard = []
        
        for acc in accounts:
            status = "🟢" if acc['is_active'] else "🔴"
            text += f"{status} **{acc['account_name']}**\n"
            text += f"   📱 {acc['phone_number']}\n\n"
            keyboard.append([
                InlineKeyboardButton(f"⚙️ {acc['account_name']}", callback_data=f"auto_ads_account|{acc['id']}"),
                InlineKeyboardButton("🗑️", callback_data=f"auto_ads_del_account|{acc['id']}")
            ])
        
        keyboard.append([InlineKeyboardButton("➕ Add Account", callback_data="auto_ads_add_account")])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="auto_ads_menu")])
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_add_account(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start adding a new Telegram account"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    _user_sessions[user_id] = {
        'step': 'account_name',
        'data': {}
    }
    
    text = """➕ **Add New Work Account**

**Step 1/4: Account Name**

Enter a friendly name for this account (e.g., "Main Account", "Worker 1"):"""
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_accounts")]]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# Keep these for backwards compatibility but they redirect to main flow
async def handle_auto_ads_upload_session(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Redirect to main add account flow"""
    return await handle_auto_ads_add_account(update, context, params)

async def handle_auto_ads_manual_setup(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Redirect to main add account flow"""
    return await handle_auto_ads_add_account(update, context, params)

async def handle_auto_ads_account_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show account details"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    account_id = int(params[0]) if params else None
    if not account_id:
        return await query.answer("Invalid account", show_alert=True)
    
    db = get_forwarder_db()
    account = db.get_account(account_id)
    
    if not account:
        return await query.answer("Account not found", show_alert=True)
    
    status = "🟢 Active" if account['is_active'] else "🔴 Inactive"
    text = f"""⚙️ **Account: {account['account_name']}**

{status}
📱 Phone: {account['phone_number']}
🔑 API ID: {account['api_id']}
📅 Added: {account['created_at'][:10]}

Session: {'✅ Configured' if account.get('session_string') else '❌ Not set'}"""
    
    keyboard = [
        [InlineKeyboardButton("🗑️ Delete Account", callback_data=f"auto_ads_del_account|{account_id}")],
        [InlineKeyboardButton("🔙 Back", callback_data="auto_ads_accounts")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_del_account(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Delete a Telegram account"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    account_id = int(params[0]) if params else None
    if not account_id:
        return await query.answer("Invalid account", show_alert=True)
    
    db = get_forwarder_db()
    db.delete_account(account_id)
    
    await query.answer("✅ Account deleted!", show_alert=True)
    return await handle_auto_ads_accounts(update, context)

# ============================================================================
# CAMPAIGN MANAGEMENT
# ============================================================================

async def handle_auto_ads_campaigns(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show all campaigns"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    bump_service = get_bump_service()
    user_id = query.from_user.id
    
    campaigns = bump_service.get_user_campaigns(user_id)
    
    if not campaigns:
        text = """📢 **Ad Campaigns**

No campaigns created yet.

Create your first campaign to start automated advertising!"""
        
        keyboard = [
            [InlineKeyboardButton("➕ Create Campaign", callback_data="auto_ads_new_campaign")],
            [InlineKeyboardButton("🔙 Back", callback_data="auto_ads_menu")]
        ]
    else:
        text = f"📢 **Ad Campaigns** ({len(campaigns)})\n\n"
        keyboard = []
        
        for camp in campaigns:
            status = "🟢" if camp.get('is_active') else "🔴"
            targets = camp.get('target_chats', [])
            target_count = len(targets) if isinstance(targets, list) else 0
            
            text += f"{status} **{camp['campaign_name']}**\n"
            text += f"   📍 {target_count} targets | 📊 {camp.get('total_sends', 0)} sends\n\n"
            
            keyboard.append([
                InlineKeyboardButton(f"📋 {camp['campaign_name']}", callback_data=f"auto_ads_campaign|{camp['id']}"),
            ])
        
        keyboard.append([InlineKeyboardButton("➕ Create Campaign", callback_data="auto_ads_new_campaign")])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="auto_ads_menu")])
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_campaign_detail(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show campaign details"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    campaign_id = int(params[0]) if params else None
    if not campaign_id:
        return await query.answer("Invalid campaign", show_alert=True)
    
    bump_service = get_bump_service()
    campaign = bump_service.db.get_campaign(campaign_id)
    
    if not campaign:
        return await query.answer("Campaign not found", show_alert=True)
    
    status = "🟢 Active" if campaign.get('is_active') else "🔴 Paused"
    targets = campaign.get('target_chats', [])
    target_count = len(targets) if isinstance(targets, list) else 0
    
    text = f"""📋 **Campaign: {campaign['campaign_name']}**

{status}
📍 Targets: {target_count} groups/channels
📊 Total Sends: {campaign.get('total_sends', 0)}
⏰ Schedule: {campaign.get('schedule_type', 'manual')}
📅 Last Run: {campaign.get('last_run', 'Never')[:16] if campaign.get('last_run') else 'Never'}"""
    
    toggle_text = "⏸️ Pause" if campaign.get('is_active') else "▶️ Activate"
    
    keyboard = [
        [InlineKeyboardButton("🚀 Run Now", callback_data=f"auto_ads_run_campaign|{campaign_id}")],
        [InlineKeyboardButton(toggle_text, callback_data=f"auto_ads_toggle_campaign|{campaign_id}")],
        [InlineKeyboardButton("✏️ Edit", callback_data=f"auto_ads_edit_campaign|{campaign_id}")],
        [InlineKeyboardButton("🗑️ Delete", callback_data=f"auto_ads_del_campaign|{campaign_id}")],
        [InlineKeyboardButton("🔙 Back", callback_data="auto_ads_campaigns")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_new_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start creating a new campaign"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    # Check if user has accounts first
    db = get_forwarder_db()
    accounts = db.get_user_accounts(user_id)
    
    if not accounts:
        text = """❌ **No Accounts Available**

You need to add at least one Telegram account before creating campaigns.

Go to "Manage Accounts" to add one first."""
        
        keyboard = [
            [InlineKeyboardButton("👥 Manage Accounts", callback_data="auto_ads_accounts")],
            [InlineKeyboardButton("🔙 Back", callback_data="auto_ads_menu")]
        ]
        
        return await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    _user_sessions[user_id] = {
        'step': 'campaign_name',
        'data': {'accounts': accounts}
    }
    
    text = """➕ **Create New Campaign**

**Step 1/5: Campaign Name**

Enter a name for this campaign (e.g., "Daily Promo", "Weekend Sale"):"""
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_toggle_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle campaign active status"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    campaign_id = int(params[0]) if params else None
    if not campaign_id:
        return await query.answer("Invalid campaign", show_alert=True)
    
    bump_service = get_bump_service()
    bump_service.toggle_campaign(campaign_id)
    
    await query.answer("✅ Campaign status toggled!")
    return await handle_auto_ads_campaign_detail(update, context, params)

async def handle_auto_ads_del_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Delete a campaign"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    campaign_id = int(params[0]) if params else None
    if not campaign_id:
        return await query.answer("Invalid campaign", show_alert=True)
    
    bump_service = get_bump_service()
    bump_service.delete_campaign(campaign_id)
    
    await query.answer("✅ Campaign deleted!", show_alert=True)
    return await handle_auto_ads_campaigns(update, context)

async def handle_auto_ads_run_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Run a campaign immediately"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    campaign_id = int(params[0]) if params else None
    if not campaign_id:
        return await query.answer("Invalid campaign", show_alert=True)
    
    await query.answer("🚀 Starting campaign...", show_alert=True)
    
    bump_service = get_bump_service(context.bot)
    
    # Run in background using the correct async method
    asyncio.create_task(bump_service._execute_campaign_async(campaign_id))
    
    text = f"🚀 **Campaign Started!**\n\nCampaign #{campaign_id} is now running.\n\nMessages will be sent according to anti-ban delays."
    
    keyboard = [[InlineKeyboardButton("🔙 Back to Campaign", callback_data=f"auto_ads_campaign|{campaign_id}")]]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ============================================================================
# STATISTICS
# ============================================================================

async def handle_auto_ads_stats(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Show auto ads statistics"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    db = get_forwarder_db()
    bump_service = get_bump_service()
    user_id = query.from_user.id
    
    accounts = db.get_user_accounts(user_id)
    campaigns = bump_service.get_user_campaigns(user_id)
    
    total_sends = sum(c.get('total_sends', 0) for c in campaigns)
    active_campaigns = sum(1 for c in campaigns if c.get('is_active'))
    
    text = f"""📊 **Auto Ads Statistics**

👥 **Accounts:** {len(accounts)}
📢 **Campaigns:** {len(campaigns)} ({active_campaigns} active)
📨 **Total Messages Sent:** {total_sends}

🛡️ Anti-ban system: ✅ Active"""
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="auto_ads_menu")]]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ============================================================================
# MESSAGE HANDLERS (for multi-step flows)
# ============================================================================

async def handle_auto_ads_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle text messages for auto ads multi-step flows. Returns True if handled."""
    user_id = update.effective_user.id
    
    if not is_primary_admin(user_id):
        return False
    
    if user_id not in _user_sessions:
        return False
    
    session = _user_sessions[user_id]
    step = session.get('step')
    text = update.message.text
    
    if not step:
        return False
    
    db = get_forwarder_db()
    bump_service = get_bump_service()
    
    # Account creation flow - Step 1: Account Name
    if step == 'account_name':
        session['data']['account_name'] = text
        session['step'] = 'phone_number'
        
        await update.message.reply_text(
            "**Step 2/4: Phone Number**\n\nEnter the phone number with country code (e.g., +37061234567):",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    # Step 2: Phone Number
    elif step == 'phone_number':
        session['data']['phone_number'] = text
        session['step'] = 'api_id'
        
        await update.message.reply_text(
            "**Step 3/4: API ID**\n\nEnter your API ID from my.telegram.org:",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    # Step 3: API ID
    elif step == 'api_id':
        session['data']['api_id'] = text
        session['step'] = 'api_hash'
        
        await update.message.reply_text(
            "**Step 4/4: API Hash**\n\nEnter your API Hash from my.telegram.org:",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    # Step 4: API Hash - then send verification code
    elif step == 'api_hash':
        session['data']['api_hash'] = text
        data = session['data']
        
        await update.message.reply_text(
            "⏳ **Connecting to Telegram...**\n\nSending verification code to your phone...",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Create Telethon client and send code
            client = TelegramClient(
                StringSession(),
                int(data['api_id']),
                data['api_hash']
            )
            
            await client.connect()
            
            # Send verification code
            sent_code = await client.send_code_request(data['phone_number'])
            
            # Store client and phone_code_hash for verification
            session['client'] = client
            session['phone_code_hash'] = sent_code.phone_code_hash
            session['step'] = 'verification_code'
            
            await update.message.reply_text(
                f"📱 **Verification Code Sent!**\n\n"
                f"A code has been sent to **{data['phone_number']}**\n\n"
                f"Please enter the verification code:",
                parse_mode=ParseMode.MARKDOWN
            )
            
        except Exception as e:
            logger.error(f"Failed to send verification code: {e}")
            del _user_sessions[user_id]
            
            await update.message.reply_text(
                f"❌ **Failed to Connect**\n\n"
                f"Error: {str(e)}\n\n"
                f"Please check your API ID and API Hash and try again.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        return True
    
    # Step 5: Verification Code
    elif step == 'verification_code':
        code = text.strip().replace(" ", "").replace("-", "")
        client = session.get('client')
        data = session['data']
        
        if not client:
            del _user_sessions[user_id]
            await update.message.reply_text("❌ Session expired. Please start over.")
            return True
        
        try:
            # Sign in with the code
            await client.sign_in(
                data['phone_number'],
                code,
                phone_code_hash=session['phone_code_hash']
            )
            
            # Get session string
            session_string = client.session.save()
            
            # Save account to database
            account_id = db.add_telegram_account(
                user_id=user_id,
                account_name=data['account_name'],
                phone_number=data['phone_number'],
                api_id=data['api_id'],
                api_hash=data['api_hash'],
                session_string=session_string
            )
            
            # Cleanup
            await client.disconnect()
            del _user_sessions[user_id]
            
            keyboard = [[InlineKeyboardButton("👥 View Accounts", callback_data="auto_ads_accounts")]]
            
            await update.message.reply_text(
                f"✅ **Account Added Successfully!**\n\n"
                f"**Name:** {data['account_name']}\n"
                f"**Phone:** {data['phone_number']}\n\n"
                f"The account is ready to use for campaigns!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            error_str = str(e).lower()
            logger.error(f"Failed to verify code: {e}")
            
            # Check if 2FA is needed
            if "two-step" in error_str or "password" in error_str or "2fa" in error_str or "srp" in error_str:
                session['step'] = '2fa_password'
                await update.message.reply_text(
                    "🔐 **Two-Factor Authentication Required**\n\n"
                    "Your account has 2FA enabled.\n\n"
                    "Please enter your 2FA password:",
                    parse_mode=ParseMode.MARKDOWN
                )
            else:
                await update.message.reply_text(
                    f"❌ **Verification Failed**\n\n"
                    f"Error: {str(e)}\n\n"
                    f"Please check the code and try again.",
                    parse_mode=ParseMode.MARKDOWN
                )
        
        return True
    
    # Step 6 (if needed): 2FA Password
    elif step == '2fa_password':
        password = text
        client = session.get('client')
        data = session['data']
        
        if not client:
            del _user_sessions[user_id]
            await update.message.reply_text("❌ Session expired. Please start over.")
            return True
        
        try:
            # Sign in with 2FA password
            await client.sign_in(password=password)
            
            # Get session string
            session_string = client.session.save()
            
            # Save account to database
            account_id = db.add_telegram_account(
                user_id=user_id,
                account_name=data['account_name'],
                phone_number=data['phone_number'],
                api_id=data['api_id'],
                api_hash=data['api_hash'],
                session_string=session_string
            )
            
            # Cleanup
            await client.disconnect()
            del _user_sessions[user_id]
            
            keyboard = [[InlineKeyboardButton("👥 View Accounts", callback_data="auto_ads_accounts")]]
            
            await update.message.reply_text(
                f"✅ **Account Added Successfully!**\n\n"
                f"**Name:** {data['account_name']}\n"
                f"**Phone:** {data['phone_number']}\n\n"
                f"The account is ready to use for campaigns!",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        except Exception as e:
            logger.error(f"Failed to verify 2FA: {e}")
            await update.message.reply_text(
                f"❌ **2FA Authentication Failed**\n\n"
                f"Error: {str(e)}\n\n"
                f"Please check your password and try again.",
                parse_mode=ParseMode.MARKDOWN
            )
        
        return True
    
    # Campaign creation flow
    elif step == 'campaign_name':
        session['data']['campaign_name'] = text
        session['step'] = 'select_account'
        
        accounts = session['data'].get('accounts', [])
        keyboard = []
        for acc in accounts:
            keyboard.append([InlineKeyboardButton(
                f"📱 {acc['account_name']}",
                callback_data=f"auto_ads_select_account|{acc['id']}"
            )])
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")])
        
        await update.message.reply_text(
            "**Step 2/5: Select Account**\n\nChoose which account will send the messages:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
    
    elif step == 'ad_content':
        # Parse message link
        import re
        
        if 't.me/' not in text and 'telegram.me/' not in text:
            await update.message.reply_text(
                "❌ **Invalid link!**\n\nPlease send a valid Telegram message link.\n\n"
                "Example: `https://t.me/yourchannel/123`",
                parse_mode=ParseMode.MARKDOWN
            )
            return True
        
        # Parse the link
        # Private channel: https://t.me/c/1234567890/123
        # Public channel: https://t.me/channelname/123
        
        private_match = re.search(r't\.me/c/(\d+)/(\d+)', text)
        public_match = re.search(r't\.me/([^/]+)/(\d+)', text)
        
        if private_match:
            channel_id = f"-100{private_match.group(1)}"
            message_id = int(private_match.group(2))
            channel_display = f"Private Channel"
        elif public_match and public_match.group(1) != 'c':
            channel_id = f"@{public_match.group(1)}"
            message_id = int(public_match.group(2))
            channel_display = channel_id
        else:
            await update.message.reply_text(
                "❌ **Could not parse link!**\n\nPlease check the format and try again.",
                parse_mode=ParseMode.MARKDOWN
            )
            return True
        
        session['data']['ad_content'] = {
            'message_link': text,
            'channel_id': channel_id,
            'message_id': message_id,
            'media_type': 'bridge_channel'
        }
        session['step'] = 'add_buttons'
        
        keyboard = [
            [InlineKeyboardButton("✅ Yes, Add Buttons", callback_data="auto_ads_buttons_yes")],
            [InlineKeyboardButton("❌ No Buttons", callback_data="auto_ads_buttons_no")],
            [InlineKeyboardButton("🚫 Cancel", callback_data="auto_ads_campaigns")]
        ]
        
        await update.message.reply_text(
            f"✅ **Message Link Set!**\n\n"
            f"**Channel:** {channel_display}\n"
            f"**Message ID:** {message_id}\n\n"
            f"**Step 4/6: Add Buttons?**\n\n"
            f"Would you like to add clickable buttons under your ad?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
    
    elif step == 'button_input':
        # Parse buttons: "Button Text | https://link.com" per line
        buttons = []
        for line in text.strip().split('\n'):
            if '|' in line:
                parts = line.split('|', 1)
                if len(parts) == 2:
                    btn_text = parts[0].strip()
                    btn_url = parts[1].strip()
                    if btn_text and btn_url:
                        buttons.append({'text': btn_text, 'url': btn_url})
        
        if not buttons:
            await update.message.reply_text(
                "❌ **No valid buttons found!**\n\n"
                "Format: `Button Text | https://link.com`\n"
                "One button per line.\n\n"
                "Try again:",
                parse_mode=ParseMode.MARKDOWN
            )
            return True
        
        session['data']['buttons'] = buttons
        session['step'] = 'target_selection_method'
        
        # Show target selection options with buttons
        keyboard = [
            [InlineKeyboardButton("📋 Select Groups", callback_data="auto_ads_fetch_groups")],
            [InlineKeyboardButton("📤 Send to All Groups", callback_data="auto_ads_all_groups")],
            [InlineKeyboardButton("✍️ Enter Manually", callback_data="auto_ads_manual_targets")],
            [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]
        ]
        
        await update.message.reply_text(
            f"✅ **{len(buttons)} button(s) added!**\n\n"
            f"📍 **Step 5/6: Target Chats**\n\n"
            f"How would you like to select target groups?\n\n"
            f"• **Select Groups** - Choose from groups the account is in\n"
            f"• **All Groups** - Send to all groups the account is in\n"
            f"• **Enter Manually** - Type usernames/IDs manually",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
    
    elif step == 'target_chats':
        targets = [t.strip() for t in text.split('\n') if t.strip()]
        session['data']['target_chats'] = targets
        session['data']['target_mode'] = 'specific'
        session['step'] = 'schedule'
        
        keyboard = [
            [InlineKeyboardButton("🔄 Continuous (every ~30min)", callback_data="auto_ads_schedule|continuous")],
            [InlineKeyboardButton("📅 Daily (once per day)", callback_data="auto_ads_schedule|daily")],
            [InlineKeyboardButton("🎯 Manual Only (Run Now button)", callback_data="auto_ads_schedule|manual")],
            [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]
        ]
        
        await update.message.reply_text(
            f"✅ **{len(targets)} target(s) set!**\n\n"
            f"⏰ **Step 6/6: Schedule**\n\n"
            f"How often should ads be sent?\n\n"
            f"• **Continuous** - Auto-sends every ~30 min with anti-ban delays\n"
            f"• **Daily** - Sends once per day\n"
            f"• **Manual** - Only runs when you click 'Run Now'",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return True
    
    return False

async def handle_auto_ads_select_account(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle account selection in campaign creation"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    account_id = int(params[0]) if params else None
    _user_sessions[user_id]['data']['account_id'] = account_id
    _user_sessions[user_id]['step'] = 'ad_content'
    
    text = """**Step 3/6: Ad Content (Message Link)**

🔗 **Send me the Telegram message link**

**How to get the link:**
1️⃣ Go to your channel/group
2️⃣ Post your ad message with premium emojis
3️⃣ Right-click the message → Copy Message Link
4️⃣ Paste the link here

**Example formats:**
• `https://t.me/yourchannel/123`
• `https://t.me/c/1234567890/123`

**Why use a link?**
✨ Preserves premium emojis
📸 Keeps all media and formatting
🔘 You can add buttons after"""
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_buttons_yes(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """User wants to add buttons"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    _user_sessions[user_id]['step'] = 'button_input'
    
    text = """🔘 **Add Buttons**

Send your buttons in this format:
`Button Text | https://link.com`

One button per line. Example:
```
Shop Now | https://myshop.com
Contact Us | https://t.me/support
```

Send your buttons:"""
    
    keyboard = [
        [InlineKeyboardButton("⏭️ Skip Buttons", callback_data="auto_ads_buttons_no")],
        [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_buttons_no(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """User doesn't want buttons, go to target selection"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    _user_sessions[user_id]['data']['buttons'] = []
    _user_sessions[user_id]['step'] = 'target_selection_method'
    
    text = """📍 **Step 5/6: Target Chats**

How would you like to select target groups?

• **Select Groups** - Choose from groups the account is in
• **All Groups** - Send to all groups the account is in
• **Enter Manually** - Type usernames/IDs manually"""
    
    keyboard = [
        [InlineKeyboardButton("📋 Select Groups", callback_data="auto_ads_fetch_groups")],
        [InlineKeyboardButton("📤 Send to All Groups", callback_data="auto_ads_all_groups")],
        [InlineKeyboardButton("✍️ Enter Manually", callback_data="auto_ads_manual_targets")],
        [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_fetch_groups(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Fetch groups from userbot account for selection"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    await query.answer("⏳ Fetching groups...")
    
    # Show loading message
    await query.edit_message_text(
        "⏳ **Fetching groups from account...**\n\nThis may take a moment.",
        parse_mode=ParseMode.MARKDOWN
    )
    
    session = _user_sessions[user_id]
    account_id = session['data'].get('account_id')
    
    db = get_forwarder_db()
    account = db.get_account(account_id)
    
    if not account or not account.get('session_string'):
        await query.edit_message_text(
            "❌ **Account session not found!**\n\nPlease re-add the account.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    try:
        # Connect to Telegram and fetch groups
        client = TelegramClient(
            StringSession(account['session_string']),
            account['api_id'],
            account['api_hash']
        )
        
        await client.connect()
        
        if not await client.is_user_authorized():
            await client.disconnect()
            await query.edit_message_text(
                "❌ **Session expired!**\n\nPlease re-authenticate the account.",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        
        # Get all dialogs (groups/channels)
        dialogs = await client.get_dialogs()
        groups = []
        
        for dialog in dialogs:
            if dialog.is_group or dialog.is_channel:
                # Get proper chat ID format
                entity = dialog.entity
                chat_id = entity.id
                if hasattr(entity, 'megagroup') or hasattr(entity, 'broadcast'):
                    chat_id = f"-100{entity.id}"
                
                groups.append({
                    'id': str(chat_id),
                    'name': dialog.name or f"Chat {chat_id}",
                    'type': 'channel' if getattr(entity, 'broadcast', False) else 'group',
                    'members': getattr(entity, 'participants_count', 0) or 0
                })
        
        await client.disconnect()
        
        if not groups:
            await query.edit_message_text(
                "⚠️ **No groups found!**\n\nThe account isn't a member of any groups or channels.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("✍️ Enter Manually", callback_data="auto_ads_manual_targets")],
                    [InlineKeyboardButton("🔙 Back", callback_data="auto_ads_buttons_no")]
                ])
            )
            return
        
        # Store groups in session for selection
        _user_sessions[user_id]['data']['available_groups'] = groups
        _user_sessions[user_id]['data']['selected_groups'] = []
        _user_sessions[user_id]['step'] = 'group_selection'
        
        # Build group selection keyboard
        await _show_group_selection(query, user_id)
        
    except Exception as e:
        logger.error(f"Error fetching groups: {e}")
        await query.edit_message_text(
            f"❌ **Error fetching groups:**\n`{str(e)[:200]}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✍️ Enter Manually", callback_data="auto_ads_manual_targets")],
                [InlineKeyboardButton("🔙 Back", callback_data="auto_ads_buttons_no")]
            ])
        )

async def _show_group_selection(query, user_id, page=0):
    """Show group selection UI with pagination"""
    session = _user_sessions[user_id]
    groups = session['data'].get('available_groups', [])
    selected = set(session['data'].get('selected_groups', []))
    
    GROUPS_PER_PAGE = 8
    total_pages = (len(groups) + GROUPS_PER_PAGE - 1) // GROUPS_PER_PAGE
    start_idx = page * GROUPS_PER_PAGE
    end_idx = min(start_idx + GROUPS_PER_PAGE, len(groups))
    
    text = f"""📋 **Select Target Groups** (Page {page + 1}/{total_pages})

Found **{len(groups)}** groups/channels.
Selected: **{len(selected)}**

Tap to select/deselect:"""
    
    keyboard = []
    
    # Group buttons
    for i in range(start_idx, end_idx):
        group = groups[i]
        is_selected = group['id'] in selected
        icon = "✅" if is_selected else "⬜"
        type_icon = "📢" if group['type'] == 'channel' else "👥"
        name = group['name'][:25] + "..." if len(group['name']) > 25 else group['name']
        
        keyboard.append([
            InlineKeyboardButton(
                f"{icon} {type_icon} {name}",
                callback_data=f"auto_ads_toggle_group|{group['id']}|{page}"
            )
        ])
    
    # Pagination buttons
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"auto_ads_group_page|{page-1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"auto_ads_group_page|{page+1}"))
    if nav_row:
        keyboard.append(nav_row)
    
    # Selection actions
    keyboard.append([
        InlineKeyboardButton("☑️ Select All", callback_data="auto_ads_select_all_groups"),
        InlineKeyboardButton("⬜ Clear All", callback_data="auto_ads_clear_groups")
    ])
    
    # Confirm/Cancel
    keyboard.append([
        InlineKeyboardButton(f"✅ Confirm ({len(selected)} groups)", callback_data="auto_ads_confirm_groups")
    ])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")])
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_group_page(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle group selection pagination"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    page = int(params[0]) if params else 0
    await _show_group_selection(query, user_id, page)

async def handle_auto_ads_toggle_group(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Toggle group selection"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    group_id = params[0] if params else None
    page = int(params[1]) if len(params) > 1 else 0
    
    if not group_id:
        return await query.answer("Invalid group", show_alert=True)
    
    selected = _user_sessions[user_id]['data'].get('selected_groups', [])
    
    if group_id in selected:
        selected.remove(group_id)
        await query.answer("❌ Deselected")
    else:
        selected.append(group_id)
        await query.answer("✅ Selected")
    
    _user_sessions[user_id]['data']['selected_groups'] = selected
    await _show_group_selection(query, user_id, page)

async def handle_auto_ads_select_all_groups(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Select all groups"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    groups = _user_sessions[user_id]['data'].get('available_groups', [])
    _user_sessions[user_id]['data']['selected_groups'] = [g['id'] for g in groups]
    
    await query.answer(f"✅ Selected all {len(groups)} groups")
    await _show_group_selection(query, user_id, 0)

async def handle_auto_ads_clear_groups(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Clear all group selections"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    _user_sessions[user_id]['data']['selected_groups'] = []
    
    await query.answer("⬜ Cleared all selections")
    await _show_group_selection(query, user_id, 0)

async def handle_auto_ads_confirm_groups(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Confirm selected groups and proceed to schedule"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    selected = _user_sessions[user_id]['data'].get('selected_groups', [])
    
    if not selected:
        return await query.answer("⚠️ Select at least one group!", show_alert=True)
    
    await query.answer()
    
    # Store targets and proceed to schedule
    _user_sessions[user_id]['data']['target_chats'] = selected
    _user_sessions[user_id]['data']['target_mode'] = 'selected'
    _user_sessions[user_id]['step'] = 'schedule'
    
    text = f"""⏰ **Step 6/6: Schedule**

Selected **{len(selected)}** target groups.

How often should ads be sent?

• **Continuous** - Auto-sends every ~30 min with anti-ban delays
• **Daily** - Sends once per day
• **Manual** - Only runs when you click 'Run Now'"""
    
    keyboard = [
        [InlineKeyboardButton("🔄 Continuous (every ~30min)", callback_data="auto_ads_schedule|continuous")],
        [InlineKeyboardButton("📅 Daily (once per day)", callback_data="auto_ads_schedule|daily")],
        [InlineKeyboardButton("🎯 Manual Only (Run Now button)", callback_data="auto_ads_schedule|manual")],
        [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_all_groups(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Select all groups (without fetching list)"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    # Store special flag for all groups
    _user_sessions[user_id]['data']['target_chats'] = ['ALL_WORKER_GROUPS']
    _user_sessions[user_id]['data']['target_mode'] = 'all_groups'
    _user_sessions[user_id]['step'] = 'schedule'
    
    text = """⏰ **Step 6/6: Schedule**

📤 **Sending to ALL groups** the account is in.

How often should ads be sent?

• **Continuous** - Auto-sends every ~30 min with anti-ban delays
• **Daily** - Sends once per day
• **Manual** - Only runs when you click 'Run Now'"""
    
    keyboard = [
        [InlineKeyboardButton("🔄 Continuous (every ~30min)", callback_data="auto_ads_schedule|continuous")],
        [InlineKeyboardButton("📅 Daily (once per day)", callback_data="auto_ads_schedule|daily")],
        [InlineKeyboardButton("🎯 Manual Only (Run Now button)", callback_data="auto_ads_schedule|manual")],
        [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_manual_targets(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Switch to manual target input"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    _user_sessions[user_id]['step'] = 'target_chats'
    
    text = """✍️ **Step 5/6: Manual Target Entry**

Enter target group/channel usernames or IDs.
One per line:

`@mygroup1`
`@mygroup2`
`-1001234567890`

Send your targets:"""
    
    keyboard = [
        [InlineKeyboardButton("🔙 Back to Options", callback_data="auto_ads_buttons_no")],
        [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handle schedule selection and save campaign"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    if user_id not in _user_sessions:
        return await query.answer("Session expired", show_alert=True)
    
    schedule_type = params[0] if params else 'manual'
    session = _user_sessions[user_id]
    data = session['data']
    
    bump_service = get_bump_service()
    
    try:
        import json
        
        # Get target mode (defaults to 'specific' for manual entries)
        target_mode = data.get('target_mode', 'specific')
        
        # Get buttons if any
        buttons = data.get('buttons', [])
        
        campaign_id = bump_service.add_campaign(
            user_id=user_id,
            account_id=data['account_id'],
            campaign_name=data['campaign_name'],
            ad_content=data['ad_content'],
            target_chats=data['target_chats'],
            schedule_type=schedule_type,
            schedule_time=None,
            buttons=buttons,
            target_mode=target_mode
        )
        
        del _user_sessions[user_id]
        
        # Show target info based on mode
        if target_mode == 'all_groups':
            target_info = "📤 All account groups"
        else:
            target_info = f"📍 {len(data['target_chats'])} targets"
        
        text = f"""✅ **Campaign Created!**

**{data['campaign_name']}**
{target_info}
⏰ Schedule: {schedule_type}

Your campaign is ready. Use "Run Now" to start."""
        
        keyboard = [
            [InlineKeyboardButton("🚀 Run Now", callback_data=f"auto_ads_run_campaign|{campaign_id}")],
            [InlineKeyboardButton("📢 View Campaigns", callback_data="auto_ads_campaigns")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error creating campaign: {e}")
        await query.edit_message_text(
            f"❌ **Error creating campaign:**\n{str(e)}",
            parse_mode=ParseMode.MARKDOWN
        )

# ============================================================================
# DOCUMENT HANDLER (placeholder - not used in simplified flow)
# ============================================================================

async def handle_auto_ads_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Document handler placeholder. Returns False (not handled)."""
    return False
