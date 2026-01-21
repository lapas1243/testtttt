import logging
import json
import time
import asyncio
import requests
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from solana.rpc.api import Client
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction
from utils import get_db_connection, send_message_with_retry, format_currency

# --- CONFIGURATION ---
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
ADMIN_WALLET = os.getenv("SOLANA_ADMIN_WALLET")  # Must be set in environment
RECOVERY_WALLET = os.getenv("SOLANA_RECOVERY_WALLET")  # Optional: For recovering stuck funds
ENABLE_AUTO_SWEEP = True  # Automatically send funds to admin wallet after payment

logger = logging.getLogger(__name__)
client = Client(SOLANA_RPC_URL)

# ===== PRODUCTION-GRADE PRICE CACHING SYSTEM =====
_price_cache = {'price': None, 'timestamp': 0, 'last_api_used': None}
PRICE_CACHE_TTL = 300  # 5 minutes cache
STALE_CACHE_MAX_AGE = 3600  # Accept stale cache up to 1 hour if all APIs fail

def get_sol_price_from_db():
    """Get cached price from database (survives restarts)"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            SELECT setting_value, updated_at 
            FROM bot_settings 
            WHERE setting_key = 'sol_price_eur_cache'
        """)
        result = c.fetchone()
        conn.close()
        
        if result:
            price = Decimal(str(result['setting_value']))
            cache_age = time.time() - result['updated_at'].timestamp()
            if cache_age < 600:  # 10 minutes
                logger.info(f"📊 DB cached SOL price: {price} EUR (age: {int(cache_age)}s)")
                return price
    except Exception as e:
        logger.debug(f"Could not fetch DB price cache: {e}")
    return None

def save_sol_price_to_db(price):
    """Save price to database for persistence"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Use proper timestamp format for SQLite
        c.execute("""
            INSERT OR REPLACE INTO bot_settings (setting_key, setting_value, updated_at)
            VALUES ('sol_price_eur_cache', ?, datetime('now'))
        """, (str(price),))
        conn.commit()
        conn.close()
        logger.debug(f"💾 Saved SOL price to DB: {price} EUR")
    except Exception as e:
        logger.debug(f"Could not save price to DB: {e}")

def fetch_price_from_api(api_name, url, parser_func):
    """Generic API fetcher with timeout and error handling"""
    try:
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            price = parser_func(response.json())
            if price:
                logger.info(f"✅ {api_name} SOL price: {price} EUR")
                return price
        elif response.status_code == 429:
            logger.warning(f"⚠️ {api_name} rate limited (429)")
        else:
            logger.warning(f"⚠️ {api_name} returned status {response.status_code}")
    except requests.Timeout:
        logger.warning(f"⏱️ {api_name} timeout")
    except Exception as e:
        logger.debug(f"{api_name} error: {e}")
    return None

def get_sol_price_eur():
    """
    PRODUCTION-GRADE: Multi-layer caching + smart API rotation
    
    Strategy:
    1. Check memory cache (instant, 5 min TTL)
    2. Check DB cache (fast, 10 min TTL)
    3. Try APIs in rotation (avoid hammering one)
    4. Use stale cache up to 1 hour (last resort)
    """
    now = time.time()
    
    # Layer 1: Memory cache
    if _price_cache['price'] and (now - _price_cache['timestamp']) < PRICE_CACHE_TTL:
        cache_age = int(now - _price_cache['timestamp'])
        logger.info(f"💰 Memory cached SOL price: {_price_cache['price']} EUR (age: {cache_age}s)")
        return _price_cache['price']
    
    # Layer 2: Database cache
    db_price = get_sol_price_from_db()
    if db_price:
        _price_cache['price'] = db_price
        _price_cache['timestamp'] = now
        return db_price
    
    # Layer 3: Fetch from APIs (smart rotation)
    apis = [
        ('Binance', 'https://api.binance.com/api/v3/ticker/price?symbol=SOLEUR', 
         lambda data: Decimal(str(data['price'])) if 'price' in data else None),
        
        ('CryptoCompare', 'https://min-api.cryptocompare.com/data/price?fsym=SOL&tsyms=EUR',
         lambda data: Decimal(str(data['EUR'])) if 'EUR' in data else None),
        
        ('CoinGecko', 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=eur',
         lambda data: Decimal(str(data['solana']['eur'])) if 'solana' in data and 'eur' in data['solana'] else None),
    ]
    
    # Start with API that was NOT used last time (rotation)
    last_used = _price_cache.get('last_api_used')
    start_idx = ((last_used + 1) % len(apis)) if (last_used is not None) else 0
    
    # Try all APIs in rotated order
    for i in range(len(apis)):
        idx = (start_idx + i) % len(apis)
        api_name, url, parser = apis[idx]
        
        price = fetch_price_from_api(api_name, url, parser)
        
        if price:
            # Success! Update all caches
            _price_cache['price'] = price
            _price_cache['timestamp'] = now
            _price_cache['last_api_used'] = idx
            save_sol_price_to_db(price)
            return price
    
    # Layer 4: Stale cache (up to 1 hour old)
    if _price_cache['price']:
        age = int(now - _price_cache['timestamp'])
        if age < STALE_CACHE_MAX_AGE:
            logger.warning(f"⚠️ All APIs failed, using stale cache ({age}s old): {_price_cache['price']} EUR")
            return _price_cache['price']
        else:
            logger.error(f"❌ Stale cache too old ({age}s), cannot use")
    
    logger.error(f"❌ CRITICAL: All price sources failed!")
    return None

async def refresh_price_cache(context=None):
    """
    Background job: Proactively refresh price cache every 4 minutes
    """
    logger.info("🔄 Background price refresh triggered")
    
    old_timestamp = _price_cache['timestamp']
    _price_cache['timestamp'] = 0
    
    price = get_sol_price_eur()
    
    if price:
        logger.info(f"✅ Background refresh successful: {price} EUR")
    else:
        logger.warning(f"⚠️ Background refresh failed, restoring old cache")
        _price_cache['timestamp'] = old_timestamp

async def create_solana_payment(user_id, order_id, eur_amount):
    """
    Generates a unique SOL wallet for this transaction.
    Returns: dict with address, amount, and payment_id
    """
    price = get_sol_price_eur()
    if not price:
        logger.error("Could not fetch SOL price")
        return {'error': 'estimate_failed'}

    # Calculate SOL amount
    sol_amount = (Decimal(eur_amount) / price).quantize(Decimal("0.00001"))
    
    # Generate new Keypair
    kp = Keypair()
    pubkey = str(kp.pubkey())
    private_key_json = json.dumps(list(bytes(kp)))

    conn = get_db_connection()
    c = conn.cursor()
    try:
        # Check if order_id already exists
        c.execute("SELECT public_key, expected_amount FROM solana_wallets WHERE order_id = ?", (order_id,))
        existing = c.fetchone()
        
        if existing:
            logger.info(f"Found existing Solana wallet for order {order_id}")
            return {
                'pay_address': existing['public_key'],
                'pay_amount': str(existing['expected_amount']),
                'pay_currency': 'SOL',
                'exchange_rate': float(price),
                'payment_id': order_id
            }

        c.execute("""
            INSERT INTO solana_wallets (user_id, order_id, public_key, private_key, expected_amount, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
        """, (user_id, order_id, pubkey, private_key_json, float(sol_amount)))
        conn.commit()
    except Exception as e:
        logger.error(f"DB Error creating solana payment: {e}")
        return {'error': 'internal_server_error'}
    finally:
        conn.close()

    return {
        'pay_address': pubkey,
        'pay_amount': str(sol_amount),
        'pay_currency': 'SOL',
        'exchange_rate': float(price),
        'payment_id': order_id
    }

# =========================================================================
# HIGH-CONCURRENCY PAYMENT PROCESSING
# Designed to handle 200+ simultaneous payments with 100% reliability
# =========================================================================

# Processing semaphore to limit concurrent wallet checks
_WALLET_CHECK_SEMAPHORE = asyncio.Semaphore(10)  # Max 10 concurrent wallet checks
_PAYMENT_PROCESS_LOCK = asyncio.Lock()  # Serialize payment finalization

async def _check_single_wallet(wallet_dict, context):
    """
    Check a single wallet for payment - runs in parallel with other wallet checks.
    Uses atomic database operations to prevent race conditions.
    """
    async with _WALLET_CHECK_SEMAPHORE:
        try:
            pubkey_str = wallet_dict['public_key']
            expected = Decimal(str(wallet_dict['expected_amount']))
            wallet_id = wallet_dict['id']
            order_id = wallet_dict['order_id']
            user_id = wallet_dict['user_id']
            created_at_str = wallet_dict['created_at']
            
            # Parse created_at string to datetime
            try:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError):
                created_at = datetime.now(timezone.utc)
            
            # Check Balance via RPC
            try:
                balance_resp = client.get_balance(Pubkey.from_string(pubkey_str))
                lamports = balance_resp.value
                sol_balance = Decimal(lamports) / Decimal(10**9)
            except Exception as rpc_e:
                logger.warning(f"RPC Error checking wallet {pubkey_str[:16]}...: {rpc_e}")
                return None
            
            # 1. Check if Paid (allowing 3% underpayment tolerance - user pays at least 97%)
            if sol_balance > 0 and sol_balance >= (expected * Decimal("0.97")):
                return {
                    'action': 'paid',
                    'wallet_id': wallet_id,
                    'order_id': order_id,
                    'user_id': user_id,
                    'sol_balance': sol_balance,
                    'expected': expected,
                    'lamports': lamports,
                    'wallet_dict': wallet_dict
                }
            
            # 2. Check for Underpayment
            elif sol_balance > 0:
                return {
                    'action': 'underpaid',
                    'wallet_id': wallet_id,
                    'order_id': order_id,
                    'user_id': user_id,
                    'sol_balance': sol_balance,
                    'lamports': lamports,
                    'wallet_dict': wallet_dict
                }
            
            # 3. Check for Expiration (Empty) - 20 minutes
            elif datetime.now(timezone.utc) - created_at > timedelta(minutes=20):
                return {
                    'action': 'expired',
                    'wallet_id': wallet_id,
                    'order_id': order_id
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking wallet: {e}", exc_info=True)
            return None

async def _process_payment_result(result, context):
    """
    Process a payment result with proper locking to prevent race conditions.
    """
    if result is None:
        return
    
    action = result.get('action')
    
    # Use lock to serialize payment processing
    async with _PAYMENT_PROCESS_LOCK:
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            if action == 'paid':
                wallet_id = result['wallet_id']
                order_id = result['order_id']
                user_id = result['user_id']
                sol_balance = result['sol_balance']
                expected = result['expected']
                lamports = result['lamports']
                wallet_dict = result['wallet_dict']
                
                # ATOMIC: Check if already processed (prevent double-processing)
                c.execute("SELECT status FROM solana_wallets WHERE id = ? AND status = 'pending'", (wallet_id,))
                check = c.fetchone()
                if not check:
                    logger.info(f"Order {order_id} already processed, skipping")
                    return
                
                logger.info(f"✅ Payment detected for Order {order_id}: {sol_balance} SOL")
                
                # ATOMIC: Mark as Paid in DB
                c.execute("BEGIN IMMEDIATE")
                c.execute("UPDATE solana_wallets SET status = 'paid', amount_received = ?, updated_at = datetime('now') WHERE id = ? AND status = 'pending'", 
                         (float(sol_balance), wallet_id))
                if c.rowcount == 0:
                    conn.rollback()
                    logger.info(f"Order {order_id} was already processed by another worker")
                    return
                conn.commit()
                
                # Handle Overpayment
                surplus = sol_balance - expected
                if surplus > Decimal("0.0005"):
                    try:
                        price = get_sol_price_eur()
                        if price:
                            surplus_eur = (surplus * price).quantize(Decimal("0.01"))
                            if surplus_eur > 0:
                                logger.info(f"💰 Overpayment of {surplus} SOL ({surplus_eur} EUR) detected for {order_id}")
                                from payment import credit_user_balance
                                await credit_user_balance(user_id, surplus_eur, f"Overpayment bonus for order {order_id}", context)
                    except Exception as over_e:
                        logger.error(f"Error processing overpayment: {over_e}")
                
                # Trigger Payment Success Logic
                from payment import process_successful_crypto_purchase, process_successful_refill
                
                c.execute("SELECT is_purchase, basket_snapshot_json as basket_snapshot, discount_code_used as discount_code, target_eur_amount, bot_id FROM pending_deposits WHERE payment_id = ?", (order_id,))
                deposit_info = c.fetchone()
                
                if deposit_info:
                    is_purchase = deposit_info['is_purchase']
                    
                    # Robust bot_id retrieval - try multiple access methods
                    stored_bot_id = None
                    try:
                        # Try direct dict-style access
                        stored_bot_id = deposit_info['bot_id']
                        logger.info(f"📱 Retrieved bot_id from deposit: {stored_bot_id}")
                    except (KeyError, IndexError) as e:
                        logger.warning(f"Could not get bot_id from deposit_info: {e}")
                        # Try converting to dict first
                        try:
                            deposit_dict = dict(deposit_info)
                            stored_bot_id = deposit_dict.get('bot_id')
                            logger.info(f"📱 Retrieved bot_id from dict conversion: {stored_bot_id}")
                        except Exception as dict_e:
                            logger.warning(f"Dict conversion also failed: {dict_e}")
                    
                    if is_purchase:
                        basket_snapshot = None
                        try:
                            basket_snapshot = deposit_info['basket_snapshot']
                        except (KeyError, IndexError):
                            pass
                        if isinstance(basket_snapshot, str):
                            try:
                                basket_snapshot = json.loads(basket_snapshot)
                            except:
                                pass
                            
                        discount_code = None
                        try:
                            discount_code = deposit_info['discount_code']
                        except (KeyError, IndexError):
                            pass
                        
                        logger.info(f"📱 Calling process_successful_crypto_purchase with bot_id={stored_bot_id}")
                        await process_successful_crypto_purchase(user_id, basket_snapshot, discount_code, order_id, context, bot_id=stored_bot_id)
                        # CRITICAL: Remove pending_deposit to prevent recovery job from re-processing
                        from utils import remove_pending_deposit
                        remove_pending_deposit(order_id, trigger="crypto_payment_success")
                    else:
                        # Refill
                        amount_eur = Decimal(str(deposit_info['target_eur_amount'])) if deposit_info['target_eur_amount'] else Decimal("0.0")
                        logger.info(f"📱 Calling process_successful_refill with bot_id={stored_bot_id}")
                        await process_successful_refill(user_id, amount_eur, order_id, context, bot_id=stored_bot_id)
                        # CRITICAL: Remove pending_deposit to prevent recovery job from re-processing
                        from utils import remove_pending_deposit
                        remove_pending_deposit(order_id, trigger="refill_payment_success")
                else:
                    # CRITICAL FIX: No pending_deposit found - this means the payment was made
                    # but the order context was lost (e.g., bot crashed before creating pending_deposit)
                    # Instead of losing the funds, credit the user's balance
                    logger.error(f"🚨 ORPHANED PAYMENT: No pending_deposit record for order {order_id}. User {user_id} paid {sol_balance} SOL but order context lost!")
                    
                    try:
                        # Credit user's balance with the EUR value of the payment
                        price = get_sol_price_eur()
                        if price:
                            eur_value = (sol_balance * price).quantize(Decimal("0.01"))
                            if eur_value > Decimal("0"):
                                from payment import credit_user_balance
                                await credit_user_balance(
                                    user_id, 
                                    eur_value, 
                                    f"Orphaned payment recovery - Order {order_id} context lost. Original payment: {sol_balance} SOL",
                                    context
                                )
                                logger.info(f"✅ ORPHANED RECOVERY: Credited {eur_value} EUR to user {user_id} for orphaned payment {order_id}")
                                
                                # Notify user
                                try:
                                    orphan_msg = (
                                        f"⚠️ *Payment Received - Order Issue*\n\n"
                                        f"We received your payment of `{sol_balance:.6f}` SOL (~{eur_value} EUR), "
                                        f"but there was a technical issue processing your order.\n\n"
                                        f"✅ The amount has been credited to your balance.\n"
                                        f"You can use it to make a new purchase.\n\n"
                                        f"If you need assistance, please contact support."
                                    )
                                    await send_message_with_retry(context.bot, user_id, orphan_msg, parse_mode="Markdown")
                                except Exception as notify_e:
                                    logger.warning(f"Could not notify user {user_id} about orphaned payment recovery: {notify_e}")
                                
                                # Alert admin
                                try:
                                    from utils import get_first_primary_admin_id
                                    admin_id = get_first_primary_admin_id()
                                    if admin_id:
                                        admin_msg = (
                                            f"🚨 *Orphaned Payment Recovered*\n\n"
                                            f"User: {user_id}\n"
                                            f"Order: {order_id}\n"
                                            f"Amount: {sol_balance:.6f} SOL (~{eur_value} EUR)\n"
                                            f"Action: Credited to user balance\n\n"
                                            f"Cause: pending_deposit record was missing (likely bot crash during payment creation)"
                                        )
                                        await send_message_with_retry(context.bot, admin_id, admin_msg, parse_mode="Markdown")
                                except Exception as admin_e:
                                    logger.warning(f"Could not alert admin about orphaned payment: {admin_e}")
                    except Exception as orphan_e:
                        logger.error(f"❌ Failed to process orphaned payment {order_id} for user {user_id}: {orphan_e}", exc_info=True)
                
                # Sweep Funds (non-blocking) - sweep even for orphaned payments since we credited the balance
                if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
                    asyncio.create_task(sweep_wallet(wallet_dict, lamports))
                    
            elif action == 'underpaid':
                wallet_id = result['wallet_id']
                order_id = result['order_id']
                user_id = result['user_id']
                sol_balance = result['sol_balance']
                lamports = result['lamports']
                wallet_dict = result['wallet_dict']
                
                # ATOMIC: Check if already processed
                c.execute("SELECT status FROM solana_wallets WHERE id = ? AND status = 'pending'", (wallet_id,))
                check = c.fetchone()
                if not check:
                    return
                
                logger.info(f"📉 Underpayment detected for {order_id} ({sol_balance} SOL). Refunding immediately.")
                
                try:
                    price = get_sol_price_eur()
                    if price:
                        refund_eur = (sol_balance * price).quantize(Decimal("0.01"))
                        if refund_eur > 0:
                            from payment import credit_user_balance
                            msg = f"⚠️ Underpayment detected ({sol_balance} SOL). Refunded {refund_eur} EUR to balance. Please use Top Up."
                            await send_message_with_retry(context.bot, user_id, msg, parse_mode=None)
                            await credit_user_balance(user_id, refund_eur, f"Underpayment refund {order_id}", context)
                            
                            # ATOMIC: Mark as refunded
                            c.execute("BEGIN IMMEDIATE")
                            c.execute("UPDATE solana_wallets SET status = 'refunded', amount_received = ?, updated_at = datetime('now') WHERE id = ? AND status = 'pending'", 
                                     (float(sol_balance), wallet_id))
                            conn.commit()
                            
                            # Sweep the partial funds
                            if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
                                asyncio.create_task(sweep_wallet(wallet_dict, lamports))
                except Exception as refund_e:
                    logger.error(f"Error refunding underpayment {order_id}: {refund_e}")
                    
            elif action == 'expired':
                wallet_id = result['wallet_id']
                order_id = result['order_id']
                
                c.execute("BEGIN IMMEDIATE")
                c.execute("UPDATE solana_wallets SET status = 'expired', updated_at = datetime('now') WHERE id = ? AND status = 'pending'", (wallet_id,))
                conn.commit()
                logger.info(f"⏱️ Order {order_id} expired (no payment received)")
                
        except Exception as e:
            logger.error(f"Error processing payment result: {e}", exc_info=True)
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
        finally:
            if conn:
                conn.close()

async def check_solana_deposits(context):
    """
    HIGH-CONCURRENCY: Background task to check all pending wallets for deposits.
    Uses parallel checking + atomic processing for 100% reliability.
    """
    conn = None
    pending_list = []
    
    try:
        # Fetch all pending wallets
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM solana_wallets WHERE status = 'pending'")
        pending = c.fetchall()
        conn.close()
        conn = None
        
        if not pending:
            return
        
        # Convert to list of dicts for parallel processing
        pending_list = [dict(row) for row in pending]
        logger.info(f"🔍 Checking {len(pending_list)} pending wallets...")
        
        # PARALLEL: Check all wallets concurrently (with rate limiting via semaphore)
        tasks = [_check_single_wallet(wallet, context) for wallet in pending_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results (one at a time with proper locking)
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Wallet check failed: {result}")
                continue
            if result:
                await _process_payment_result(result, context)
                
    except Exception as e:
        logger.error(f"Error in check_solana_deposits: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
        
    # RECOVERY: Check for 'paid' wallets that haven't been swept
    if ENABLE_AUTO_SWEEP and ADMIN_WALLET:
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("SELECT * FROM solana_wallets WHERE status = 'paid'")
            paid_wallets = c.fetchall()
            conn.close()
            
            for wallet in paid_wallets:
                asyncio.create_task(sweep_wallet(dict(wallet)))
        except Exception as e:
            logger.error(f"Error in sweep recovery loop: {e}")

async def sweep_wallet(wallet_data, current_lamports=0):
    """Moves funds from temp wallet to ADMIN_WALLET"""
    try:
        # Fetch balance if not provided
        if current_lamports == 0:
            try:
                balance_resp = client.get_balance(Pubkey.from_string(wallet_data['public_key']))
                current_lamports = balance_resp.value
            except Exception as e:
                logger.error(f"Error fetching balance for sweep {wallet_data['public_key']}: {e}")
                return

        if current_lamports < 5000:  # Ignore dust
            if wallet_data['status'] == 'paid' and current_lamports < 5000:
                conn = get_db_connection()
                conn.cursor().execute("UPDATE solana_wallets SET status = 'swept' WHERE id = ?", (wallet_data['id'],))
                conn.commit()
                conn.close()
            return

        # Load Keypair
        priv_key_list = json.loads(wallet_data['private_key'])
        kp = Keypair.from_bytes(bytes(priv_key_list))
        
        # Calculate fee
        fee = 5000
        amount_to_send = current_lamports - fee
        
        if amount_to_send <= 0:
            return

        logger.info(f"🧹 Sweeping {amount_to_send} lamports from {wallet_data['public_key']} to {ADMIN_WALLET}...")

        # Create Transaction
        ix = transfer(
            TransferParams(
                from_pubkey=kp.pubkey(),
                to_pubkey=Pubkey.from_string(ADMIN_WALLET),
                lamports=int(amount_to_send)
            )
        )
        
        # Get blockhash
        latest_blockhash = client.get_latest_blockhash().value.blockhash
        
        # Construct and sign transaction
        transaction = Transaction.new_signed_with_payer(
            [ix],
            kp.pubkey(),
            [kp],
            latest_blockhash
        )
        
        # Send
        txn_sig = client.send_transaction(transaction)
        
        logger.info(f"✅ Swept funds. Sig: {txn_sig.value}")
        
        # Update DB
        conn = get_db_connection()
        conn.cursor().execute("UPDATE solana_wallets SET status = 'swept' WHERE id = ?", (wallet_data['id'],))
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to sweep wallet {wallet_data['public_key']}: {e}", exc_info=True)


# =========================================================================
# STUCK FUNDS RECOVERY SYSTEM
# Recovers funds from wallets that failed to sweep or were never processed
# =========================================================================

def _check_balance_with_retry(pubkey_str: str, max_retries: int = 3) -> int:
    """
    Check wallet balance with retry logic and exponential backoff.
    Returns lamports or -1 if failed.
    """
    for attempt in range(max_retries):
        try:
            balance_resp = client.get_balance(Pubkey.from_string(pubkey_str))
            return balance_resp.value
        except Exception as e:
            error_str = str(e).lower()
            # Check for rate limiting
            if '429' in error_str or 'rate' in error_str or 'too many' in error_str:
                wait_time = (2 ** attempt) * 0.5  # 0.5s, 1s, 2s
                logger.debug(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                time.sleep(wait_time)
            elif attempt < max_retries - 1:
                time.sleep(0.2 * (attempt + 1))  # Small delay before retry
            else:
                raise e
    return -1


async def find_stuck_wallets():
    """
    Finds all wallets in the database that have SOL balance but were never swept.
    Checks actual on-chain balance for each wallet with rate limiting.
    
    Returns: List of dicts with wallet info and current balance
    """
    stuck_wallets = []
    conn = None
    
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Find ALL wallets - check on-chain balance regardless of DB status
        # (a wallet might be marked 'swept' but sweep tx could have failed)
        c.execute("""
            SELECT id, user_id, order_id, public_key, private_key, expected_amount, 
                   status, amount_received, created_at
            FROM solana_wallets 
            ORDER BY created_at DESC
        """)
        
        wallets = c.fetchall()
        conn.close()
        conn = None
        
        if not wallets:
            logger.info("🔍 No potentially stuck wallets found in database.")
            return []
        
        total_wallets = len(wallets)
        
        # Generate detailed status report
        status_counts = {}
        for w in wallets:
            status = w['status'] or 'unknown'
            status_counts[status] = status_counts.get(status, 0) + 1
        
        logger.info(f"🔍 Checking {total_wallets} wallets for stuck funds (with rate limiting)...")
        logger.info(f"📊 Database Status Breakdown: {status_counts}")
        
        # Get price once for all calculations
        price = get_sol_price_eur()
        
        # Process in batches with delays to avoid rate limiting
        BATCH_SIZE = 10  # Check 10 wallets at a time
        BATCH_DELAY = 1.0  # 1 second between batches
        RPC_DELAY = 0.15  # 150ms between individual RPC calls
        
        checked = 0
        failed = 0
        
        for i in range(0, total_wallets, BATCH_SIZE):
            batch = wallets[i:i + BATCH_SIZE]
            
            for wallet in batch:
                wallet_dict = dict(wallet)
                pubkey_str = wallet_dict['public_key']
                
                try:
                    # VALIDATION: Verify private key can derive correct public key
                    try:
                        priv_key_list = json.loads(wallet_dict['private_key'])
                        kp = Keypair.from_bytes(bytes(priv_key_list))
                        derived_pubkey = str(kp.pubkey())
                        if derived_pubkey != pubkey_str:
                            logger.warning(f"⚠️ CORRUPTED KEY: {pubkey_str[:16]}... private key derives {derived_pubkey[:16]}... - SKIPPING")
                            failed += 1
                            continue
                    except Exception as key_e:
                        logger.warning(f"⚠️ INVALID KEY: {pubkey_str[:16]}... - Error: {str(key_e)[:50]} - SKIPPING")
                        failed += 1
                        continue
                    
                    # Check actual on-chain balance with retry logic
                    lamports = _check_balance_with_retry(pubkey_str)
                    
                    if lamports < 0:
                        failed += 1
                        continue
                    
                    sol_balance = Decimal(lamports) / Decimal(10**9)
                    
                    # If balance > dust threshold (0.0001 SOL), it's stuck
                    if sol_balance > Decimal("0.0001"):
                        eur_value = float(sol_balance * price) if price else 0
                        
                        stuck_wallets.append({
                            'id': wallet_dict['id'],
                            'user_id': wallet_dict['user_id'],
                            'order_id': wallet_dict['order_id'],
                            'public_key': pubkey_str,
                            'private_key': wallet_dict['private_key'],
                            'expected_amount': wallet_dict['expected_amount'],
                            'status': wallet_dict['status'],
                            'sol_balance': float(sol_balance),
                            'lamports': lamports,
                            'eur_value': eur_value,
                            'created_at': wallet_dict['created_at']
                        })
                        
                        logger.info(f"💰 Found stuck funds: {pubkey_str[:16]}... = {sol_balance:.6f} SOL (~{eur_value:.2f} EUR) [Status: {wallet_dict['status']}]")
                    
                    checked += 1
                    
                    # Small delay between RPC calls to avoid rate limiting
                    await asyncio.sleep(RPC_DELAY)
                        
                except Exception as rpc_e:
                    failed += 1
                    error_msg = str(rpc_e) if str(rpc_e) else "Empty RPC response (rate limited?)"
                    logger.warning(f"⚠️ Could not check balance for {pubkey_str[:16]}...: {error_msg}")
                    # Add extra delay on errors (likely rate limited)
                    await asyncio.sleep(0.5)
                    continue
            
            # Progress update every batch
            progress_pct = min(100, ((i + len(batch)) / total_wallets) * 100)
            logger.info(f"📊 Progress: {progress_pct:.0f}% ({i + len(batch)}/{total_wallets}) - Found {len(stuck_wallets)} stuck so far, {failed} failed checks")
            
            # Delay between batches (skip if last batch)
            if i + BATCH_SIZE < total_wallets:
                await asyncio.sleep(BATCH_DELAY)
        
        logger.info(f"✅ Scan complete: Found {len(stuck_wallets)} wallets with stuck funds. (Checked: {checked}, Failed: {failed})")
        return stuck_wallets
        
    except Exception as e:
        logger.error(f"❌ Error finding stuck wallets: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


async def check_single_wallet(wallet_address: str):
    """
    Check a single wallet's balance - useful for quick checks.
    Returns dict with balance info or None if failed.
    """
    try:
        lamports = _check_balance_with_retry(wallet_address)
        if lamports < 0:
            return None
        
        sol_balance = Decimal(lamports) / Decimal(10**9)
        price = get_sol_price_eur()
        eur_value = float(sol_balance * price) if price else 0
        
        return {
            'public_key': wallet_address,
            'sol_balance': float(sol_balance),
            'lamports': lamports,
            'eur_value': eur_value
        }
    except Exception as e:
        logger.error(f"❌ Failed to check wallet {wallet_address}: {e}")
        return None


async def recover_stuck_funds(target_wallet: str = None):
    """
    Recovers all stuck funds by sweeping them to the recovery wallet.
    
    Args:
        target_wallet: Optional. If not provided, uses RECOVERY_WALLET env var,
                      falling back to ADMIN_WALLET if RECOVERY_WALLET is not set.
    
    Returns: Dict with recovery results
    """
    # Determine target wallet
    if target_wallet is None:
        target_wallet = RECOVERY_WALLET or ADMIN_WALLET
    
    if not target_wallet:
        logger.error("❌ No recovery wallet configured! Set SOLANA_RECOVERY_WALLET or SOLANA_ADMIN_WALLET environment variable.")
        return {'success': False, 'error': 'No recovery wallet configured', 'recovered': []}
    
    logger.info(f"🔄 Starting stuck funds recovery to wallet: {target_wallet}")
    
    # Find all stuck wallets
    stuck_wallets = await find_stuck_wallets()
    
    if not stuck_wallets:
        logger.info("✅ No stuck funds to recover.")
        return {'success': True, 'message': 'No stuck funds found', 'recovered': []}
    
    total_sol_recovered = Decimal('0')
    total_eur_recovered = Decimal('0')
    recovered = []
    failed = []
    
    for wallet_info in stuck_wallets:
        try:
            pubkey_str = wallet_info['public_key']
            lamports = wallet_info['lamports']
            sol_balance = Decimal(str(wallet_info['sol_balance']))
            
            logger.info(f"🧹 Recovering {sol_balance:.6f} SOL from {pubkey_str[:16]}...")
            
            # Load Keypair from private key
            priv_key_list = json.loads(wallet_info['private_key'])
            kp = Keypair.from_bytes(bytes(priv_key_list))
            
            # Calculate fee and amount to send
            fee = 5000  # lamports
            amount_to_send = lamports - fee
            
            if amount_to_send <= 0:
                logger.warning(f"⚠️ Balance too low to recover from {pubkey_str[:16]}... (balance: {lamports} lamports)")
                continue
            
            # Create transfer instruction
            ix = transfer(
                TransferParams(
                    from_pubkey=kp.pubkey(),
                    to_pubkey=Pubkey.from_string(target_wallet),
                    lamports=int(amount_to_send)
                )
            )
            
            # Get latest blockhash
            latest_blockhash = client.get_latest_blockhash().value.blockhash
            
            # Construct and sign transaction
            transaction = Transaction.new_signed_with_payer(
                [ix],
                kp.pubkey(),
                [kp],
                latest_blockhash
            )
            
            # Send transaction
            txn_sig = client.send_transaction(transaction)
            
            logger.info(f"✅ Recovered {sol_balance:.6f} SOL from {pubkey_str[:16]}... Sig: {txn_sig.value}")
            
            # Update database
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("""
                UPDATE solana_wallets 
                SET status = 'swept', 
                    amount_received = ?,
                    updated_at = datetime('now')
                WHERE id = ?
            """, (float(sol_balance), wallet_info['id']))
            conn.commit()
            conn.close()
            
            total_sol_recovered += sol_balance
            total_eur_recovered += Decimal(str(wallet_info['eur_value']))
            
            recovered.append({
                'public_key': pubkey_str,
                'sol_amount': float(sol_balance),
                'eur_value': wallet_info['eur_value'],
                'user_id': wallet_info['user_id'],
                'order_id': wallet_info['order_id'],
                'tx_signature': str(txn_sig.value)
            })
            
            # Small delay between transactions to avoid rate limiting
            await asyncio.sleep(0.5)
            
        except Exception as e:
            logger.error(f"❌ Failed to recover from {wallet_info['public_key'][:16]}...: {e}")
            failed.append({
                'public_key': wallet_info['public_key'],
                'sol_amount': wallet_info['sol_balance'],
                'error': str(e)
            })
    
    result = {
        'success': True,
        'target_wallet': target_wallet,
        'total_sol_recovered': float(total_sol_recovered),
        'total_eur_recovered': float(total_eur_recovered),
        'wallets_recovered': len(recovered),
        'wallets_failed': len(failed),
        'recovered': recovered,
        'failed': failed
    }
    
    logger.info(f"🎉 Recovery complete! Recovered {total_sol_recovered:.6f} SOL (~{total_eur_recovered:.2f} EUR) from {len(recovered)} wallets.")
    if failed:
        logger.warning(f"⚠️ Failed to recover from {len(failed)} wallets.")
    
    return result


async def recover_single_wallet(wallet_address: str, target_wallet: str = None):
    """
    Recovers funds from a specific wallet address.
    
    Args:
        wallet_address: The public key of the wallet to recover from
        target_wallet: Optional. If not provided, uses RECOVERY_WALLET or ADMIN_WALLET
    
    Returns: Dict with recovery result
    """
    # Determine target wallet
    if target_wallet is None:
        target_wallet = RECOVERY_WALLET or ADMIN_WALLET
    
    if not target_wallet:
        return {'success': False, 'error': 'No recovery wallet configured'}
    
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Find the wallet in database
        c.execute("""
            SELECT id, user_id, order_id, public_key, private_key, expected_amount, status
            FROM solana_wallets 
            WHERE public_key = ?
        """, (wallet_address,))
        
        wallet = c.fetchone()
        conn.close()
        conn = None
        
        if not wallet:
            return {'success': False, 'error': f'Wallet {wallet_address} not found in database'}
        
        wallet_dict = dict(wallet)
        
        # Check on-chain balance
        balance_resp = client.get_balance(Pubkey.from_string(wallet_address))
        lamports = balance_resp.value
        sol_balance = Decimal(lamports) / Decimal(10**9)
        
        if sol_balance <= Decimal("0.0001"):
            return {'success': False, 'error': f'Wallet has no funds to recover (balance: {sol_balance} SOL)'}
        
        price = get_sol_price_eur()
        eur_value = float(sol_balance * price) if price else 0
        
        logger.info(f"🧹 Recovering {sol_balance:.6f} SOL (~{eur_value:.2f} EUR) from {wallet_address[:16]}...")
        
        # Load Keypair
        priv_key_list = json.loads(wallet_dict['private_key'])
        kp = Keypair.from_bytes(bytes(priv_key_list))
        
        # Calculate fee and amount
        fee = 5000
        amount_to_send = lamports - fee
        
        if amount_to_send <= 0:
            return {'success': False, 'error': 'Balance too low to cover transaction fee'}
        
        # Create and send transaction
        ix = transfer(
            TransferParams(
                from_pubkey=kp.pubkey(),
                to_pubkey=Pubkey.from_string(target_wallet),
                lamports=int(amount_to_send)
            )
        )
        
        latest_blockhash = client.get_latest_blockhash().value.blockhash
        
        transaction = Transaction.new_signed_with_payer(
            [ix],
            kp.pubkey(),
            [kp],
            latest_blockhash
        )
        
        txn_sig = client.send_transaction(transaction)
        
        logger.info(f"✅ Recovered {sol_balance:.6f} SOL! Sig: {txn_sig.value}")
        
        # Update database
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            UPDATE solana_wallets 
            SET status = 'swept', 
                amount_received = ?,
                updated_at = datetime('now')
            WHERE id = ?
        """, (float(sol_balance), wallet_dict['id']))
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'wallet_address': wallet_address,
            'target_wallet': target_wallet,
            'sol_recovered': float(sol_balance),
            'eur_value': eur_value,
            'user_id': wallet_dict['user_id'],
            'order_id': wallet_dict['order_id'],
            'tx_signature': str(txn_sig.value)
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to recover from {wallet_address}: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}
    finally:
        if conn:
            conn.close()


def get_recovery_status():
    """
    Returns current recovery configuration and stuck funds summary.
    Useful for admin dashboard or status checks.
    """
    return {
        'admin_wallet': ADMIN_WALLET,
        'recovery_wallet': RECOVERY_WALLET,
        'recovery_wallet_configured': bool(RECOVERY_WALLET),
        'auto_sweep_enabled': ENABLE_AUTO_SWEEP
    }

