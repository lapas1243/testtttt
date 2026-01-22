"""
Auto Ads System - Admin Integration
Integrates forwarder/bump service into the main shop bot admin panel
"""

import logging
import os
import base64
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

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
    """Start adding a new Telegram account - choose method"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    
    text = """➕ **Add New Work Account**

**Choose your setup method:**

**📤 Upload Session File (Recommended)**
• Fastest setup method
• Upload .session file directly
• Account ready immediately

**🔧 Manual Setup**
• Enter API credentials step-by-step
• Phone verification required
• For creating new sessions"""
    
    keyboard = [
        [InlineKeyboardButton("📤 Upload Session File", callback_data="auto_ads_upload_session")],
        [InlineKeyboardButton("🔧 Manual Setup", callback_data="auto_ads_manual_setup")],
        [InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_accounts")]
    ]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_upload_session(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start session file upload process"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    _user_sessions[user_id] = {
        'step': 'upload_session',
        'data': {}
    }
    
    text = """📤 **Upload Session File**

Send me your Telegram session file (.session) as a document.

**Requirements:**
• File must have .session extension
• File size should be less than 50KB
• Session must be valid and active

**How to get a session file:**
1. Use Telethon to create a session on your PC
2. The .session file is created in your script directory
3. Upload that file here

Send the session file now:"""
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_accounts")]]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_auto_ads_manual_setup(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Start manual account setup"""
    query = update.callback_query
    if not is_primary_admin(query.from_user.id):
        return await query.answer("Access denied.", show_alert=True)
    
    await query.answer()
    user_id = query.from_user.id
    
    _user_sessions[user_id] = {
        'step': 'account_name',
        'data': {}
    }
    
    text = """🔧 **Manual Account Setup**

**Step 1/5: Account Name**

Enter a friendly name for this account (e.g., "Main Account", "Worker 1"):"""
    
    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="auto_ads_accounts")]]
    
    await query.edit_message_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

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
    
    # Account creation flow
    if step == 'account_name':
        session['data']['account_name'] = text
        session['step'] = 'phone_number'
        
        await update.message.reply_text(
            "**Step 2/5: Phone Number**\n\nEnter the phone number with country code (e.g., +37061234567):",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    elif step == 'phone_number':
        session['data']['phone_number'] = text
        session['step'] = 'api_id'
        
        await update.message.reply_text(
            "**Step 3/5: API ID**\n\nEnter your API ID from my.telegram.org:",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    elif step == 'api_id':
        session['data']['api_id'] = text
        session['step'] = 'api_hash'
        
        await update.message.reply_text(
            "**Step 4/5: API Hash**\n\nEnter your API Hash from my.telegram.org:",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    elif step == 'api_hash':
        session['data']['api_hash'] = text
        session['step'] = 'session_string'
        
        await update.message.reply_text(
            "**Step 5/5: Session String**\n\nPaste your Telethon session string.\n\n(Generate using Telethon's StringSession)",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    elif step == 'session_string':
        data = session['data']
        
        try:
            db.add_telegram_account(
                user_id=user_id,
                account_name=data['account_name'],
                phone_number=data['phone_number'],
                api_id=data['api_id'],
                api_hash=data['api_hash'],
                session_string=text
            )
            
            del _user_sessions[user_id]
            
            keyboard = [[InlineKeyboardButton("👥 View Accounts", callback_data="auto_ads_accounts")]]
            
            await update.message.reply_text(
                f"✅ **Account Added Successfully!**\n\n**{data['account_name']}** is now ready to use.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Error adding account: {e}")
            await update.message.reply_text(
                f"❌ **Error adding account:**\n{str(e)}",
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
# DOCUMENT HANDLER (for session file upload)
# ============================================================================

async def handle_auto_ads_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle document uploads for auto ads (session files). Returns True if handled."""
    user_id = update.effective_user.id
    
    if not is_primary_admin(user_id):
        return False
    
    if user_id not in _user_sessions:
        return False
    
    session = _user_sessions[user_id]
    step = session.get('step')
    
    if step != 'upload_session':
        return False
    
    document = update.message.document
    if not document:
        return False
    
    # Check if it's a session file
    file_name = document.file_name or ""
    if not file_name.endswith('.session'):
        await update.message.reply_text(
            "❌ **Invalid file!**\n\nPlease upload a .session file.",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    # Check file size (max 50KB)
    if document.file_size > 50 * 1024:
        await update.message.reply_text(
            "❌ **File too large!**\n\nSession files should be less than 50KB.",
            parse_mode=ParseMode.MARKDOWN
        )
        return True
    
    try:
        # Download the file
        file = await context.bot.get_file(document.file_id)
        file_bytes = await file.download_as_bytearray()
        
        # Encode session data as base64 for storage
        session_data = base64.b64encode(bytes(file_bytes)).decode('utf-8')
        
        # Extract account name from filename
        account_name = file_name.replace('.session', '')
        
        # Save to database
        db = get_forwarder_db()
        account_id = db.add_telegram_account(
            user_id=user_id,
            account_name=account_name,
            phone_number="(from session file)",
            api_id="0",
            api_hash="(from session file)",
            session_string=session_data
        )
        
        del _user_sessions[user_id]
        
        keyboard = [[InlineKeyboardButton("👥 View Accounts", callback_data="auto_ads_accounts")]]
        
        await update.message.reply_text(
            f"✅ **Session Uploaded Successfully!**\n\n"
            f"**Account:** {account_name}\n"
            f"**ID:** {account_id}\n\n"
            f"The account is ready to use for campaigns!",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error uploading session: {e}")
        await update.message.reply_text(
            f"❌ **Error uploading session:**\n{str(e)}",
            parse_mode=ParseMode.MARKDOWN
        )
    
    return True
