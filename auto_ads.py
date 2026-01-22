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
    
    # Run in background
    import asyncio
    asyncio.create_task(bump_service.execute_campaign(campaign_id))
    
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
        session['data']['ad_content'] = {'text': text, 'media_type': 'text'}
        session['step'] = 'target_chats'
        
        await update.message.reply_text(
            "**Step 4/5: Target Chats**\n\nEnter target group/channel usernames or IDs.\n\nOne per line, e.g.:\n@mygroup1\n@mygroup2\n-1001234567890",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    elif step == 'target_chats':
        targets = [t.strip() for t in text.split('\n') if t.strip()]
        session['data']['target_chats'] = targets
        session['step'] = 'schedule'
        
        keyboard = [
            [InlineKeyboardButton("🔄 Continuous", callback_data="auto_ads_schedule|continuous")],
            [InlineKeyboardButton("📅 Daily", callback_data="auto_ads_schedule|daily")],
            [InlineKeyboardButton("🎯 Manual Only", callback_data="auto_ads_schedule|manual")],
            [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]
        ]
        
        await update.message.reply_text(
            f"**Step 5/5: Schedule**\n\nTargets: {len(targets)} groups/channels\n\nChoose how often to send:",
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
    
    text = """**Step 3/5: Ad Content**

Send the message you want to advertise.

You can send:
• Text message
• Photo with caption
• Video with caption

Just send your ad content now:"""
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_campaigns")]]
    
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
        campaign_id = bump_service.create_campaign(
            user_id=user_id,
            account_id=data['account_id'],
            campaign_name=data['campaign_name'],
            ad_content=json.dumps(data['ad_content']),
            target_chats=json.dumps(data['target_chats']),
            schedule_type=schedule_type,
            schedule_time=None
        )
        
        del _user_sessions[user_id]
        
        text = f"""✅ **Campaign Created!**

**{data['campaign_name']}**
📍 {len(data['target_chats'])} targets
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
