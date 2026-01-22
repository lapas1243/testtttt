#!/usr/bin/env python3
"""
Account Safety Status Checker
Checks your accounts' anti-ban status and provides recommendations
"""

import sqlite3
from datetime import datetime, timedelta
from forwarder_config import Config

def check_account_safety():
    """Check all accounts' safety status"""
    
    # Determine database path
    if hasattr(Config, 'DATABASE_PATH'):
        db_path = Config.DATABASE_PATH
    else:
        db_path = 'tgcf.db'
    
    print("=" * 80)
    print("🛡️  ACCOUNT SAFETY STATUS REPORT")
    print("=" * 80)
    print()
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if anti-ban table exists
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='account_usage_tracking'
        """)
        
        if not cursor.fetchone():
            print("⚠️  WARNING: Anti-ban system not initialized!")
            print("   Deploy the updated code to activate protection.")
            print()
            return
        
        # Get all accounts with tracking
        cursor.execute("""
            SELECT 
                t.account_id,
                a.account_name,
                t.account_created_date,
                t.messages_sent_today,
                t.daily_limit,
                t.last_message_time,
                t.last_campaign_time,
                t.is_restricted,
                t.restriction_reason,
                t.total_messages_sent,
                t.last_reset_date
            FROM account_usage_tracking t
            LEFT JOIN telegram_accounts a ON t.account_id = a.id
            ORDER BY t.account_id
        """)
        
        accounts = cursor.fetchall()
        
        if not accounts:
            print("ℹ️  No accounts tracked yet.")
            print("   Accounts will be tracked when campaigns run.")
            print()
            return
        
        now = datetime.now()
        today = now.date()
        
        for account in accounts:
            (account_id, account_name, created_date, messages_today, daily_limit,
             last_message_time, last_campaign_time, is_restricted, restriction_reason,
             total_messages, last_reset) = account
            
            print(f"📱 Account: {account_name or f'Account {account_id}'}")
            print(f"   ID: {account_id}")
            print()
            
            # Account age
            if created_date:
                created = datetime.fromisoformat(created_date)
                age_days = (now - created).days
                
                if age_days < Config.ACCOUNT_WARM_UP_DAYS:
                    status = "🆕 NEW (Warm-Up Period)"
                    recommendation = f"Keep under {Config.MAX_MESSAGES_PER_DAY_NEW_ACCOUNT} messages/day"
                elif age_days < Config.ACCOUNT_MATURE_DAYS:
                    status = "🌱 WARMED"
                    recommendation = f"Can send up to {Config.MAX_MESSAGES_PER_DAY_WARMED_ACCOUNT} messages/day"
                else:
                    status = "✅ MATURE"
                    recommendation = f"Can send up to {Config.MAX_MESSAGES_PER_DAY_MATURE_ACCOUNT} messages/day"
                
                print(f"   Age: {age_days} days - {status}")
                print(f"   📋 {recommendation}")
            
            # Daily usage
            print()
            print(f"   📊 Today's Usage: {messages_today}/{daily_limit} messages")
            
            remaining = daily_limit - messages_today
            percent_used = (messages_today / daily_limit * 100) if daily_limit > 0 else 0
            
            if percent_used >= 100:
                print(f"   ⛔ LIMIT REACHED - Account resting until tomorrow")
            elif percent_used >= 80:
                print(f"   ⚠️  {remaining} messages remaining (Use carefully!)")
            elif percent_used >= 50:
                print(f"   ✅ {remaining} messages remaining")
            else:
                print(f"   ✅ {remaining} messages remaining (Good capacity)")
            
            # Last activity
            print()
            if last_message_time:
                last_msg = datetime.fromisoformat(last_message_time)
                time_since = (now - last_msg).total_seconds()
                
                if time_since < 60:
                    ago = f"{int(time_since)} seconds ago"
                elif time_since < 3600:
                    ago = f"{int(time_since/60)} minutes ago"
                else:
                    ago = f"{int(time_since/3600)} hours ago"
                
                print(f"   🕐 Last Message: {ago}")
                
                # Check if can send now
                min_delay = Config.MIN_DELAY_BETWEEN_MESSAGES
                if time_since < min_delay:
                    wait_seconds = min_delay - time_since
                    wait_minutes = wait_seconds / 60
                    print(f"   ⏳ Must wait {wait_minutes:.1f} more minutes before next message")
                else:
                    print(f"   ✅ Can send message now")
            else:
                print(f"   🕐 Last Message: Never")
                print(f"   ✅ Ready to send")
            
            if last_campaign_time:
                last_camp = datetime.fromisoformat(last_campaign_time)
                time_since = (now - last_camp).total_seconds() / 60
                
                print(f"   📅 Last Campaign: {time_since:.1f} minutes ago")
                
                # Check cooldown
                cooldown = Config.MIN_COOLDOWN_BETWEEN_CAMPAIGNS_MINUTES
                if time_since < cooldown:
                    wait_minutes = cooldown - time_since
                    print(f"   ⏳ Cooldown active: Wait {wait_minutes:.1f} more minutes")
                else:
                    print(f"   ✅ Cooldown passed - Can run campaign")
            else:
                print(f"   📅 Last Campaign: Never")
            
            # Restriction status
            print()
            if is_restricted:
                print(f"   ⛔ RESTRICTED: {restriction_reason}")
                print(f"   ⚠️  Contact Telegram support: [email protected]")
            else:
                print(f"   ✅ Account Status: Active and healthy")
            
            # Total stats
            print()
            print(f"   📈 Total Messages Sent: {total_messages}")
            
            print()
            print("-" * 80)
            print()
        
        # System configuration
        print("⚙️  ANTI-BAN CONFIGURATION")
        print()
        print(f"   Delays: {Config.MIN_DELAY_BETWEEN_MESSAGES/60:.1f}-{Config.MAX_DELAY_BETWEEN_MESSAGES/60:.1f} minutes between messages")
        print(f"   Cooldown: {Config.MIN_COOLDOWN_BETWEEN_CAMPAIGNS_MINUTES} minutes between campaigns")
        print(f"   Random Breaks: {'Enabled' if Config.ENABLE_RANDOM_BREAKS else 'Disabled'}")
        if Config.ENABLE_RANDOM_BREAKS:
            print(f"   Break Duration: {Config.MIN_BREAK_DURATION_MINUTES}-{Config.MAX_BREAK_DURATION_MINUTES} minutes")
            print(f"   Break Probability: {Config.BREAK_PROBABILITY*100:.0f}%")
        print(f"   Safety Mode: {Config.SAFETY_MODE.upper()}")
        print()
        
        # Recommendations
        print("💡 RECOMMENDATIONS")
        print()
        
        # Count account types
        new_accounts = sum(1 for a in accounts if a[2] and (now - datetime.fromisoformat(a[2])).days < Config.ACCOUNT_WARM_UP_DAYS)
        at_limit = sum(1 for a in accounts if a[3] >= a[4])
        restricted = sum(1 for a in accounts if a[7])
        
        if restricted > 0:
            print(f"   ⚠️  {restricted} account(s) restricted - Appeal to Telegram immediately!")
        
        if at_limit > 0:
            print(f"   ⏸️  {at_limit} account(s) at daily limit - Will reset at midnight")
        
        if new_accounts > 0:
            print(f"   🆕 {new_accounts} account(s) in warm-up period - Use sparingly!")
        
        if new_accounts == 0 and at_limit == 0 and restricted == 0:
            print(f"   ✅ All accounts healthy and ready!")
        
        print()
        print("=" * 80)
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        print()
    except Exception as e:
        print(f"❌ Error: {e}")
        print()

if __name__ == "__main__":
    check_account_safety()

