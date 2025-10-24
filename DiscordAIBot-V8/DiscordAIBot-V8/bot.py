import discord
from discord.ext import commands
import os
from dotenv import load_dotenv
import sqlite3
import datetime
import threading
import secrets
import string
import asyncio
from flask import Flask, jsonify, request
from flask_cors import CORS

# Load environment variables
load_dotenv()

# Bot configuration
intents = discord.Intents.default()
# Note: message_content intent is privileged and needs to be enabled in Discord Developer Portal
# intents.message_content = True  # Uncomment after enabling in Discord Developer Portal

# Performance optimizations for better ping
bot = commands.Bot(
    command_prefix='!', 
    intents=intents,
    chunk_guilds_at_startup=False,  # Faster startup, reduces initial ping spikes
    member_cache_flags=discord.MemberCacheFlags.none(),  # Reduce memory usage
    max_messages=50,  # Limit message cache to reduce memory
    heartbeat_timeout=60.0,  # Increase heartbeat timeout for stability
    guild_ready_timeout=5.0  # Faster guild ready detection
)

# Database setup
DATABASE_PATH = 'islamic_economy.db'


def init_database():
    """Initialize the Islamic economy database"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        # Enable foreign key constraints
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # HALAL COMPLIANCE: Drop any existing stock-related tables to ensure 100% halal compliance
        cursor.execute('DROP TABLE IF EXISTS government_stocks')
        cursor.execute('DROP TABLE IF EXISTS share_listings') 
        cursor.execute('DROP TABLE IF EXISTS business_shares')
        print("✅ Removed any existing stock tables for halal compliance")

        # Users table - track user accounts
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                gold_dinars REAL DEFAULT 0.0,
                silver_dirhams REAL DEFAULT 0.0,
                last_zakat_payment TEXT,
                total_charity REAL DEFAULT 0.0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Transactions table - all financial activities
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                description TEXT,
                partner_id TEXT,
                timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                is_halal BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # Investments table - Shariah-compliant investments
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS investments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                investment_type TEXT NOT NULL,
                amount REAL NOT NULL,
                currency TEXT NOT NULL,
                profit_sharing_ratio REAL,
                start_date TEXT DEFAULT CURRENT_TIMESTAMP,
                end_date TEXT,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # Businesses table - Islamic businesses
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS businesses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                business_name TEXT NOT NULL,
                business_type TEXT NOT NULL,
                startup_cost REAL NOT NULL,
                daily_profit REAL NOT NULL,
                employees INTEGER DEFAULT 0,
                created_date TEXT DEFAULT CURRENT_TIMESTAMP,
                last_collection_date TEXT,
                status TEXT DEFAULT 'active',
                license_code TEXT UNIQUE,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Check if we need to migrate database schema
        try:
            cursor.execute("PRAGMA table_info(businesses)")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Migrate from monthly_profit to daily_profit
            if 'monthly_profit' in columns and 'daily_profit' not in columns:
                # Add daily_profit column
                cursor.execute("ALTER TABLE businesses ADD COLUMN daily_profit REAL DEFAULT 0")
                # Convert monthly to daily (divide by 30)
                cursor.execute("UPDATE businesses SET daily_profit = monthly_profit / 30")
                
                # SQLite doesn't support DROP COLUMN, so create new table without monthly_profit
                cursor.execute('''
                    CREATE TABLE businesses_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL,
                        business_name TEXT NOT NULL,
                        business_type TEXT NOT NULL,
                        startup_cost REAL NOT NULL,
                        daily_profit REAL NOT NULL,
                        employees INTEGER DEFAULT 0,
                        created_date TEXT DEFAULT CURRENT_TIMESTAMP,
                        last_collection_date TEXT,
                        status TEXT DEFAULT 'active',
                        FOREIGN KEY (user_id) REFERENCES users (user_id)
                    )
                ''')
                
                # Copy data to new table
                cursor.execute('''
                    INSERT INTO businesses_new (id, user_id, business_name, business_type, startup_cost, daily_profit, employees, created_date, status)
                    SELECT id, user_id, business_name, business_type, startup_cost, daily_profit, employees, created_date, status
                    FROM businesses
                ''')
                
                # Replace old table with new one
                cursor.execute("DROP TABLE businesses")
                cursor.execute("ALTER TABLE businesses_new RENAME TO businesses")
            
            # Add last_collection_date column if it doesn't exist
            elif 'last_collection_date' not in columns:
                cursor.execute("ALTER TABLE businesses ADD COLUMN last_collection_date TEXT")
            
            # Add license_code column if it doesn't exist
            if 'license_code' not in columns:
                cursor.execute("ALTER TABLE businesses ADD COLUMN license_code TEXT")
                # Add unique index for license codes
                cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_business_license_code ON businesses(license_code)")
                
        except sqlite3.OperationalError:
            # Table doesn't exist yet or other issue - let the CREATE TABLE handle it
            pass

        # Jobs table - Employment system
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                job_title TEXT NOT NULL,
                employer TEXT NOT NULL,
                salary REAL NOT NULL,
                currency TEXT NOT NULL,
                start_date TEXT DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # User Employment table - track employment between users
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_employment (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employer_id TEXT NOT NULL,
                employee_id TEXT NOT NULL,
                business_id INTEGER NOT NULL,
                job_title TEXT NOT NULL,
                salary REAL NOT NULL,
                currency TEXT NOT NULL,
                employment_start TEXT DEFAULT CURRENT_TIMESTAMP,
                employment_end TEXT,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (employer_id) REFERENCES users (user_id),
                FOREIGN KEY (employee_id) REFERENCES users (user_id),
                FOREIGN KEY (business_id) REFERENCES businesses (id)
            )
        ''')

        # Job Postings table - track job openings posted by business owners
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_postings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                employer_id TEXT NOT NULL,
                business_id INTEGER NOT NULL,
                job_title TEXT NOT NULL,
                salary REAL NOT NULL,
                currency TEXT NOT NULL,
                description TEXT,
                positions_available INTEGER DEFAULT 1,
                posted_date TEXT DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'open',
                FOREIGN KEY (employer_id) REFERENCES users (user_id),
                FOREIGN KEY (business_id) REFERENCES businesses (id)
            )
        ''')

        # Daily Tasks table - track daily income opportunities
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                completion_date TEXT NOT NULL,
                reward_amount REAL NOT NULL,
                currency TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # Marketplace table - for trading goods
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS marketplace (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id TEXT NOT NULL,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                price REAL NOT NULL,
                currency TEXT NOT NULL,
                description TEXT,
                quantity INTEGER DEFAULT 1,
                listed_date TEXT DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'available',
                FOREIGN KEY (seller_id) REFERENCES users (user_id)
            )
        ''')

        # User Skills table - track learned skills and expertise
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_skills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                skill_name TEXT NOT NULL,
                skill_level INTEGER DEFAULT 1,
                experience_points INTEGER DEFAULT 0,
                learned_date TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # Charity Work table - track community service
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS charity_work (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                work_type TEXT NOT NULL,
                hours_contributed REAL NOT NULL,
                completion_date TEXT DEFAULT CURRENT_TIMESTAMP,
                reward_amount REAL NOT NULL,
                currency TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # Achievements table - track user accomplishments
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                achievement_name TEXT NOT NULL,
                achievement_description TEXT,
                earned_date TEXT DEFAULT CURRENT_TIMESTAMP,
                reward_amount REAL DEFAULT 0,
                currency TEXT DEFAULT 'gold_dinars',
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # Pilgrimage Savings table - track Hajj/Umrah savings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pilgrimage_savings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                pilgrimage_type TEXT NOT NULL,
                target_amount REAL NOT NULL,
                saved_amount REAL DEFAULT 0,
                currency TEXT NOT NULL,
                created_date TEXT DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'saving',
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')

        # Stock trading tables removed for halal compliance
        # Replaced with Shariah-compliant investment and partnership models

        # Loan Applications table - users apply for loans
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS loan_applications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                borrower_id TEXT NOT NULL,
                borrower_name TEXT NOT NULL,
                loan_amount REAL NOT NULL,
                currency TEXT NOT NULL,
                repayment_days INTEGER NOT NULL,
                due_date TEXT NOT NULL,
                purpose TEXT NOT NULL,
                application_date TEXT DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending',
                funded_by TEXT,
                FOREIGN KEY (borrower_id) REFERENCES users (user_id),
                FOREIGN KEY (funded_by) REFERENCES users (user_id)
            )
        ''')

        # Active Loans table - funded loans
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS loans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lender_id TEXT NOT NULL,
                borrower_id TEXT NOT NULL,
                loan_amount REAL NOT NULL,
                currency TEXT NOT NULL,
                loan_date TEXT DEFAULT CURRENT_TIMESTAMP,
                due_date TEXT NOT NULL,
                repaid_amount REAL DEFAULT 0.0,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (lender_id) REFERENCES users (user_id),
                FOREIGN KEY (borrower_id) REFERENCES users (user_id)
            )
        ''')

        # Share listings table removed for halal compliance
        # Partnerships now follow Musharakah/Mudarabah principles

        # Business Mergers table - track business mergers
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS business_mergers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                primary_business_id INTEGER NOT NULL,
                merged_business_id INTEGER NOT NULL,
                merger_date TEXT DEFAULT CURRENT_TIMESTAMP,
                merger_value REAL NOT NULL,
                status TEXT DEFAULT 'completed',
                FOREIGN KEY (primary_business_id) REFERENCES businesses (id),
                FOREIGN KEY (merged_business_id) REFERENCES businesses (id)
            )
        ''')

        # === ISLAMIC BANKING SYSTEM TABLES ===
        
        # Bank Accounts table - Islamic bank accounts at finance businesses
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bank_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_number TEXT UNIQUE NOT NULL,
                institution_business_id INTEGER NOT NULL,
                owner_user_id TEXT NOT NULL,
                account_type TEXT NOT NULL CHECK (account_type IN ('wadiah', 'mudarabah')),
                currency TEXT NOT NULL CHECK (currency IN ('gold_dinars', 'silver_dirhams')),
                balance REAL DEFAULT 0.0 CHECK (balance >= 0),
                profit_share_ratio REAL CHECK (profit_share_ratio IS NULL OR (profit_share_ratio >= 0 AND profit_share_ratio <= 1)),
                status TEXT DEFAULT 'active' CHECK (status IN ('active', 'closed')),
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (institution_business_id) REFERENCES businesses (id),
                FOREIGN KEY (owner_user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Bank Ledger table - All banking transactions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bank_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                entry_type TEXT NOT NULL CHECK (entry_type IN ('deposit', 'withdrawal', 'transfer_in', 'transfer_out', 'profit_share', 'service_fee')),
                amount REAL NOT NULL CHECK (amount > 0),
                currency TEXT NOT NULL CHECK (currency IN ('gold_dinars', 'silver_dirhams')),
                description TEXT NOT NULL,
                counterparty_account_id INTEGER,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                created_by_user_id TEXT NOT NULL,
                FOREIGN KEY (account_id) REFERENCES bank_accounts (id),
                FOREIGN KEY (counterparty_account_id) REFERENCES bank_accounts (id),
                FOREIGN KEY (created_by_user_id) REFERENCES users (user_id)
            )
        ''')
        
        # Bank Account Permissions table - Who can access bank accounts
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bank_account_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                user_id TEXT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('owner', 'joint_owner', 'manager', 'viewer')),
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES bank_accounts (id),
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                UNIQUE(account_id, user_id)
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bank_accounts_owner ON bank_accounts (owner_user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bank_accounts_institution ON bank_accounts (institution_business_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bank_ledger_account ON bank_ledger (account_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bank_ledger_created_at ON bank_ledger (created_at)')
        
        # Government Shop table - NPC shop with always available essential items
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS government_shop (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                price REAL NOT NULL,
                currency TEXT NOT NULL,
                description TEXT,
                stock INTEGER DEFAULT -1,
                is_essential BOOLEAN DEFAULT FALSE
            )
        ''')

        # Initialize government shop with essential items if empty
        cursor.execute('SELECT COUNT(*) FROM government_shop')
        shop_count = cursor.fetchone()[0]
        
        if shop_count == 0:
            essential_items = [
                ('Bread', 'food', 2.0, 'gold_dinars', 'Fresh baked bread for daily sustenance', -1, True),
                ('Rice', 'food', 1.5, 'gold_dinars', 'High quality rice grains', -1, True),
                ('Dates', 'food', 3.0, 'gold_dinars', 'Sweet dates from Arabian palms', -1, True),
                ('Water', 'food', 0.5, 'silver_dirhams', 'Clean fresh water', -1, True),
                ('Milk', 'food', 2.5, 'gold_dinars', 'Fresh goat milk', -1, True),
                ('Basic Robe', 'clothing', 15.0, 'gold_dinars', 'Simple cotton robe', -1, True),
                ('Sandals', 'clothing', 8.0, 'gold_dinars', 'Comfortable leather sandals', -1, True),
                ('Prayer Mat', 'tools', 12.0, 'gold_dinars', 'High quality prayer mat', -1, True),
                ('Quran', 'books', 25.0, 'gold_dinars', 'Holy Quran with Arabic text', -1, True),
                ('Lantern', 'tools', 10.0, 'gold_dinars', 'Oil lantern for lighting', -1, True),
                ('Basic Tools', 'tools', 20.0, 'gold_dinars', 'Essential craftsman tools', -1, True),
                ('Cooking Pot', 'tools', 15.0, 'gold_dinars', 'Clay cooking pot', -1, True),
                ('Wool', 'materials', 5.0, 'gold_dinars', 'High quality sheep wool', -1, False),
                ('Cloth', 'materials', 8.0, 'gold_dinars', 'Fine woven cloth', -1, False),
                ('Wood', 'materials', 3.0, 'gold_dinars', 'Seasoned timber wood', -1, False),
                ('Honey', 'food', 6.0, 'gold_dinars', 'Pure natural honey', -1, False),
                ('Olive Oil', 'food', 4.0, 'gold_dinars', 'Extra virgin olive oil', -1, False),
                ('Incense', 'crafts', 7.0, 'gold_dinars', 'Fragrant incense sticks', -1, False),
                ('Leather', 'materials', 12.0, 'gold_dinars', 'Tanned leather hide', -1, False),
                ('Spices', 'food', 8.0, 'gold_dinars', 'Mixed aromatic spices', -1, False)
            ]
            
            cursor.executemany('''
                INSERT INTO government_shop (item_name, item_type, price, currency, description, stock, is_essential)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', essential_items)

        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        print(f"Database initialization error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error during database initialization: {e}")
        return False


# Flask Web API for Minecraft Integration
app = Flask(__name__)
CORS(app)

# API Authentication token (secure random default)
import secrets
API_TOKEN = os.getenv('MINECRAFT_API_TOKEN', f'secure_{secrets.token_hex(16)}')

def verify_api_token(token):
    """Simple API token verification"""
    return token == API_TOKEN

def validate_user_id(user_id):
    """Validate Discord user ID format"""
    if not user_id or not isinstance(user_id, str):
        return False
    # Discord user IDs are numeric strings of 17-19 digits
    return user_id.isdigit() and 17 <= len(user_id) <= 19

def validate_amount(amount):
    """Validate monetary amounts"""
    try:
        amount_float = float(amount)
        return 0.01 <= amount_float <= 999999.99  # Reasonable limits
    except (ValueError, TypeError):
        return False

def sanitize_username(username):
    """Sanitize username input"""
    if not username or not isinstance(username, str):
        return "Unknown User"
    # Remove potentially dangerous characters and limit length
    import re
    sanitized = re.sub(r'[<>"\';&\\]', '', username)
    return sanitized[:50]  # Limit length

@app.route('/api/balance/<user_id>', methods=['GET'])
def get_balance(user_id):
    """Get user balance for Minecraft integration"""
    auth_token = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not verify_api_token(auth_token):
        return jsonify({'error': 'Unauthorized'}), 401
    
    # Validate user ID
    if not validate_user_id(user_id):
        return jsonify({'error': 'Invalid user ID format'}), 400
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT gold_dinars, silver_dirhams FROM users WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return jsonify({
                'user_id': user_id,
                'gold_dinars': result[0],
                'silver_dirhams': result[1]
            })
        else:
            return jsonify({'error': 'User not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/pay', methods=['POST'])
def transfer_money():
    """Transfer money between users via Minecraft"""
    auth_token = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not verify_api_token(auth_token):
        return jsonify({'error': 'Unauthorized'}), 401
    
    data = request.json
    if not data:
        return jsonify({'error': 'No JSON data provided'}), 400
    
    from_user = data.get('from_user')
    to_user = data.get('to_user')
    amount = data.get('amount', 0)
    currency = data.get('currency', 'gold_dinars')
    
    # Security: Validate all inputs
    if not validate_user_id(from_user) or not validate_user_id(to_user):
        return jsonify({'error': 'Invalid user ID format'}), 400
    
    if not validate_amount(amount):
        return jsonify({'error': 'Invalid amount'}), 400
    
    amount = float(amount)  # Safe to convert after validation
    
    if currency not in ['gold_dinars', 'silver_dirhams']:
        return jsonify({'error': 'Invalid currency type'}), 400
    
    if from_user == to_user:
        return jsonify({'error': 'Cannot transfer to yourself'}), 400
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Begin immediate transaction to prevent race conditions
        cursor.execute('BEGIN IMMEDIATE')
        
        # Atomic balance check and update to prevent race conditions
        cursor.execute(f'UPDATE users SET {currency} = {currency} - ? WHERE user_id = ? AND {currency} >= ?', 
                      (amount, from_user, amount))
        
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Insufficient funds or user not found'}), 400
        
        # Add to recipient
        cursor.execute(f'UPDATE users SET {currency} = {currency} + ? WHERE user_id = ?', (amount, to_user))
        
        if cursor.rowcount == 0:
            conn.rollback()
            return jsonify({'error': 'Recipient user not found'}), 400
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (from_user, 'transfer_send', amount, currency, 'Minecraft transfer', to_user))
        
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (to_user, 'transfer_receive', amount, currency, 'Minecraft transfer', from_user))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': f'Transferred {amount:.2f} {currency}'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/businesses/<user_id>', methods=['GET'])
def get_businesses(user_id):
    """Get user businesses for Minecraft integration"""
    auth_token = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not verify_api_token(auth_token):
        return jsonify({'error': 'Unauthorized'}), 401
    
    if not validate_user_id(user_id):
        return jsonify({'error': 'Invalid user ID format'}), 400
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT business_name, business_type, daily_profit, status
            FROM businesses WHERE user_id = ?
        ''', (user_id,))
        
        businesses = []
        for row in cursor.fetchall():
            businesses.append({
                'name': row[0],
                'type': row[1],
                'daily_profit': row[2],
                'status': row[3]
            })
        
        conn.close()
        return jsonify({'businesses': businesses})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/collect_profit/<user_id>', methods=['POST'])
def collect_profit_api(user_id):
    """Collect business profits via Minecraft"""
    auth_token = request.headers.get('Authorization', '').replace('Bearer ', '')
    if not verify_api_token(auth_token):
        return jsonify({'error': 'Unauthorized'}), 401
    
    if not validate_user_id(user_id):
        return jsonify({'error': 'Invalid user ID format'}), 400
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, business_name, business_type, daily_profit, created_date, last_collection_date
            FROM businesses WHERE user_id = ? AND status = 'active'
        ''', (user_id,))
        
        businesses = cursor.fetchall()
        
        if not businesses:
            return jsonify({'error': 'No active businesses found'}), 404
        
        total_profit = 0.0
        business_details = []
        businesses_to_update = []
        
        for biz_id, name, biz_type, daily_profit, created_date, last_collection_date in businesses:
            current_time = datetime.datetime.now()
            
            if last_collection_date:
                last_time = datetime.datetime.fromisoformat(last_collection_date)
            else:
                last_time = datetime.datetime.fromisoformat(created_date)
            
            hours_passed = (current_time - last_time).total_seconds() / 3600
            
            period_profit = daily_profit / 6
            periods_passed = int(hours_passed // 4)
            available_profit = period_profit * periods_passed
            
            if available_profit > 0.05:
                total_profit += available_profit
                business_details.append({'name': name, 'profit': available_profit})
                businesses_to_update.append(biz_id)
        
        if total_profit < 0.05:
            return jsonify({'error': 'No profits available to collect'}), 400
        
        # Update user balance
        cursor.execute('UPDATE users SET gold_dinars = gold_dinars + ? WHERE user_id = ?', (total_profit, user_id))
        
        # Update last collection date
        current_time_str = datetime.datetime.now().isoformat()
        for business_id in businesses_to_update:
            cursor.execute('UPDATE businesses SET last_collection_date = ? WHERE id = ?', 
                         (current_time_str, business_id))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, 'business_profit', total_profit, 'gold_dinars', 'Minecraft profit collection'))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'total_profit': total_profit,
            'business_details': business_details
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def run_flask_app():
    """Run Flask app in a separate thread"""
    app.run(host='0.0.0.0', port=5000, debug=False)


# UptimeRobot Keep-Alive functionality
uptime_app = Flask('uptime')

@uptime_app.route('/')
def home():
    return "I'm alive!"

def run_uptime_server():
    """Run UptimeRobot keep-alive server"""
    uptime_app.run(host='0.0.0.0', port=8080, debug=False)

def keep_alive():
    """Start the UptimeRobot keep-alive server in a separate thread"""
    uptime_thread = threading.Thread(target=run_uptime_server, daemon=True)
    uptime_thread.start()


def get_user_account(user_id: str, username: str) -> dict:
    """Get or create user account"""
    try:
        # Validate inputs
        if not validate_user_id(user_id):
            return {
                'user_id': user_id,
                'username': 'Invalid User',
                'gold_dinars': 0.0,
                'silver_dirhams': 0.0,
                'last_zakat_payment': None,
                'total_charity': 0.0,
                'created_at': datetime.datetime.now().isoformat()
            }
        
        username = sanitize_username(username)
        
        conn = sqlite3.connect(DATABASE_PATH)
        # Enable foreign key constraints
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id, ))
        user = cursor.fetchone()

        if not user:
            cursor.execute(
                '''
                INSERT INTO users (user_id, username, gold_dinars, silver_dirhams)
                VALUES (?, ?, 100.0, 500.0)
            ''', (user_id, username))
            conn.commit()

            cursor.execute('SELECT * FROM users WHERE user_id = ?',
                           (user_id, ))
            user = cursor.fetchone()

        conn.close()

        return {
            'user_id': user[0],
            'username': user[1],
            'gold_dinars': user[2],
            'silver_dirhams': user[3],
            'last_zakat_payment': user[4],
            'total_charity': user[5],
            'created_at': user[6]
        }
    except sqlite3.Error as e:
        print(f"Database error in get_user_account: {e}")
        # Return default user data if database fails
        return {
            'user_id': user_id,
            'username': username,
            'gold_dinars': 0.0,
            'silver_dirhams': 0.0,
            'last_zakat_payment': None,
            'total_charity': 0.0,
            'created_at': datetime.datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Unexpected error in get_user_account: {e}")
        # Return default user data if unexpected error occurs
        return {
            'user_id': user_id,
            'username': username,
            'gold_dinars': 0.0,
            'silver_dirhams': 0.0,
            'last_zakat_payment': None,
            'total_charity': 0.0,
            'created_at': datetime.datetime.now().isoformat()
        }


def calculate_zakat(gold_dinars: float, silver_dirhams: float) -> dict:
    """Calculate Zakat (Islamic obligatory charity)"""
    # Nisab thresholds
    gold_nisab = 85.0  # 85 grams of gold equivalent
    silver_nisab = 595.0  # 595 grams of silver equivalent
    zakat_rate = 0.025  # 2.5%

    gold_zakat = 0.0
    silver_zakat = 0.0

    if gold_dinars >= gold_nisab:
        gold_zakat = gold_dinars * zakat_rate

    if silver_dirhams >= silver_nisab:
        silver_zakat = silver_dirhams * zakat_rate

    return {
        'gold_zakat': gold_zakat,
        'silver_zakat': silver_zakat,
        'total_zakat': gold_zakat + silver_zakat,
        'eligible': gold_dinars >= gold_nisab or silver_dirhams >= silver_nisab
    }

def calculate_taxes(income: float, currency: str) -> float:
    """Calculate taxes - 5% on income above thresholds"""
    if currency == 'gold_dinars':
        threshold = 100.0
    else:  # silver_dirhams
        threshold = 150.0
    
    if income > threshold:
        return (income - threshold) * 0.05
    return 0.0

def get_exchange_rate() -> float:
    """Get current exchange rate: 1 gold dinar = X silver dirhams"""
    return 12.0  # 1 gold = 12 silver (based on historical ratios)

def calculate_job_experience_bonus(job_title: str, user_id: str) -> float:
    """Calculate experience bonus for returning to same job"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Count previous jobs of same type
        cursor.execute('''
            SELECT COUNT(*) FROM jobs 
            WHERE user_id = ? AND job_title = ? AND status = 'completed'
        ''', (user_id, job_title))
        
        experience_count = cursor.fetchone()[0]
        conn.close()
        
        # 10% bonus per previous job, capped at 50%
        return min(experience_count * 0.1, 0.5)
    except:
        return 0.0

def get_agricultural_usher_bonus() -> float:
    """Get usher bonus for agricultural businesses/jobs"""
    return 0.15  # 15% bonus for agricultural sector


def generate_business_license_code() -> str:
    """Generate a unique business license code"""
    # Format: BLI-XXXXX-XXXXX (Business License Islamic)
    prefix = "BLI"
    part1 = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(5))
    part2 = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(5))
    return f"{prefix}-{part1}-{part2}"


def is_license_code_unique(license_code: str) -> bool:
    """Check if a license code is unique in the database"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM businesses WHERE license_code = ?', (license_code,))
        count = cursor.fetchone()[0]
        conn.close()
        return count == 0
    except:
        return False


def generate_unique_license_code() -> str:
    """Generate a unique business license code that doesn't exist in database"""
    max_attempts = 50  # Increased attempts
    for _ in range(max_attempts):
        code = generate_business_license_code()
        if is_license_code_unique(code):
            return code
    # If we still can't find unique code after many attempts, raise error
    raise Exception("Unable to generate unique license code after 50 attempts")


@bot.event
async def on_ready():
    print(
        f'{bot.user.display_name if bot.user else "Bot"} has connected to Discord!'
    )
    print(f'Bot is in {len(bot.guilds)} guilds')

    # Initialize database
    if init_database():
        print('Islamic Economy Database initialized successfully!')
    else:
        print('Failed to initialize database! Bot may not function properly.')
        return

    # Sync slash commands
    try:
        synced = await bot.tree.sync()
        print(f'Synced {len(synced)} command(s)')
        
        # Start the overdue loan checking loop
        bot.loop.create_task(overdue_loan_checker())
        print('Started overdue loan monitoring system')
    except Exception as e:
        print(f'Failed to sync commands: {e}')
        print(
            'Bot will continue running, but slash commands may not work properly.'
        )

async def overdue_loan_checker():
    """Background task to check for overdue loans every 24 hours"""
    while True:
        try:
            await asyncio.sleep(86400)  # Wait 24 hours
            await process_overdue_loans()
            print("Completed overdue loan check")
        except Exception as e:
            print(f"Error in overdue loan checker: {e}")
            await asyncio.sleep(3600)  # Wait 1 hour on error before retrying


@bot.tree.command(name="bank",
                  description="View your Islamic economy account")
async def bank(interaction: discord.Interaction):
    """Display user's Islamic economy account"""
    user_data = get_user_account(
        str(interaction.user.id), interaction.user.display_name
        or interaction.user.name)

    zakat_info = calculate_zakat(user_data['gold_dinars'],
                                 user_data['silver_dirhams'])

    embed = discord.Embed(title=f"🕌 DigiCap Account - {user_data['username']}",
                          color=0x00AA00)

    embed.add_field(
        name="💰 Assets",
        value=
        f"₯ Gold Dinars: {user_data['gold_dinars']:.2f}\n₯ Silver Dirhams: {user_data['silver_dirhams']:.2f}",
        inline=False)

    embed.add_field(
        name="📿 Zakat Status",
        value=
        f"Due: {zakat_info['total_zakat']:.2f} combined\nEligible: {'Yes' if zakat_info['eligible'] else 'No'}",
        inline=True)

    embed.add_field(name="💝 Charity Given",
                    value=f"{user_data['total_charity']:.2f} total",
                    inline=True)

    embed.set_footer(
        text="Assets are backed by real precious metals • Riba-free system")

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="help", description="Show all available commands")
async def help_command(interaction: discord.Interaction):
    """Display all available commands and their descriptions"""
    
    embed = discord.Embed(
        title="🕌 DigiCap - Islamic Economy Bot Commands",
        description="All available commands for the Islamic digital economy system",
        color=0x0066CC
    )
    
    # Account & Banking
    embed.add_field(
        name="💰 Account & Banking",
        value=(
            "`/bank` - View your Islamic economy account\n"
            "`/exchange` - Exchange between gold dinars and silver dirhams"
        ),
        inline=False
    )
    
    # Islamic Obligations
    embed.add_field(
        name="📿 Islamic Obligations",
        value=(
            "`/zakat` - Calculate your Zakat obligation\n"
            "`/pay_zakat` - Pay your Zakat (religious charity)\n"
            "`/taxes` - View tax information and calculations"
        ),
        inline=False
    )
    
    # Halal Earning Methods
    embed.add_field(
        name="🌟 Halal Earning Methods",
        value=(
            "`/community_service` - Earn rewards through community service\n"
            "`/recite_quran` - Earn spiritual and monetary rewards from Quran recitation\n"
            "`/skill_development` - Learn new skills while earning rewards\n"
            "`/mentor_someone` - Mentor others and earn through knowledge sharing"
        ),
        inline=False
    )
    
    # Investments & Trading  
    embed.add_field(
        name="📈 Shariah-Compliant Investments",
        value=(
            "`/invest` - Make Shariah-compliant investments\n"
            "`/withdraw_investment` - Withdraw from active investments\n"
            "`/my_investments` - View your investment portfolio\n"
            "`/my_portfolio` - View your complete halal portfolio"
        ),
        inline=False
    )
    
    # Employment
    embed.add_field(
        name="💼 Employment",
        value=(
            "`/work` - Work at your current job or freelance\n"
            "`/get_job` - Get hired for a specific job\n"
            "`/current_job` - View your current employment\n"
            "`/quit_job` - Leave your current job\n"
            "`/job_history` - View your work experience"
        ),
        inline=False
    )
    
    # Business Management
    embed.add_field(
        name="🏢 Business",
        value=(
            "`/start_business` - Start a halal business\n"
            "`/collect_profit` - Collect profits from your businesses\n"
            "`/my_businesses` - View and manage your businesses\n"
            "`/post_job` - Post job openings at your business\n"
            "`/my_employees` - View your employees"
        ),
        inline=False
    )
    
    # User Employment System
    embed.add_field(
        name="👥 User Employment",
        value=(
            "`/job_openings` - View jobs posted by other users\n"
            "`/apply_job` - Apply for a job posted by another user\n"
            "`/work_for_user` - Work your shift for your user employer\n"
            "`/quit_user_job` - Leave your current user employment"
        ),
        inline=False
    )
    
    # Religious Leadership
    embed.add_field(
        name="🕌 Religious Leadership",
        value=(
            "`/become_imam` - Become an imam of a mosque (special requirements)\n"
            "Higher salary with charity bonuses and community responsibilities"
        ),
        inline=False
    )
    
    # Daily Tasks & Activities
    embed.add_field(
        name="📅 Daily Tasks & Activities",
        value=(
            "`/daily_tasks` - View available daily tasks for rewards\n"
            "`/complete_task` - Complete daily tasks for income\n"
            "`/volunteer` - Do volunteer work for community rewards"
        ),
        inline=False
    )
    
    # Marketplace Trading
    embed.add_field(
        name="🏪 Marketplace Trading",
        value=(
            "`/list_item` - List items for sale in marketplace\n"
            "`/marketplace` - Browse available items for purchase\n"
            "`/buy_item` - Purchase items from other users\n"
            "`/my_listings` - Manage your marketplace listings"
        ),
        inline=False
    )
    
    # Skills & Freelancing
    embed.add_field(
        name="📚 Skills & Freelancing",
        value=(
            "`/learn_skill` - Learn new valuable skills\n"
            "`/freelance_work` - Use skills to earn money\n"
            "`/my_skills` - View your skill portfolio and levels"
        ),
        inline=False
    )
    
    # Pilgrimage Savings
    embed.add_field(
        name="🕋 Pilgrimage Savings",
        value=(
            "`/start_hajj_savings` - Begin saving for Hajj pilgrimage\n"
            "`/save_for_hajj` - Add money to pilgrimage fund\n"
            "`/pilgrimage_status` - Check savings progress"
        ),
        inline=False
    )
    
    # Progress & Events
    embed.add_field(
        name="🏆 Progress & Events",
        value=(
            "`/achievements` - View your accomplishments\n"
            "`/islamic_calendar` - See current Islamic events\n"
            "`/seasonal_work` - Special seasonal opportunities"
        ),
        inline=False
    )
    
    # Information & Bonuses
    embed.add_field(
        name="ℹ️ Information & Bonuses",
        value=(
            "`/usher` - Learn about agricultural bonuses\n"
            "`/islamic_finance_info` - Learn about Islamic finance principles"
        ),
        inline=False
    )
    
    embed.set_footer(
        text="🌟 Excellent Islamic Economy • Comprehensive halal income streams • All Shariah-compliant"
    )
    
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="zakat",
                  description="Calculate and pay your Zakat obligation")
async def zakat_command(interaction: discord.Interaction):
    """Calculate and optionally pay Zakat"""
    user_data = get_user_account(
        str(interaction.user.id), interaction.user.display_name
        or interaction.user.name)
    zakat_info = calculate_zakat(user_data['gold_dinars'],
                                 user_data['silver_dirhams'])

    embed = discord.Embed(title="📿 Zakat Calculation", color=0x0066CC)

    if zakat_info['eligible']:
        embed.add_field(
            name="💰 Zakat Due",
            value=
            f"₯ Gold: {zakat_info['gold_zakat']:.2f} dinars\n₯ Silver: {zakat_info['silver_zakat']:.2f} dirhams\n**Total: ₯{zakat_info['total_zakat']:.2f}**",
            inline=False)

        embed.add_field(
            name="📊 Calculation",
            value="Based on 2.5% of eligible wealth above Nisab threshold",
            inline=False)

        embed.add_field(
            name="🤲 Pay Zakat?",
            value="Use `/pay_zakat` to fulfill your religious obligation",
            inline=False)
    else:
        embed.add_field(
            name="ℹ️ Not Eligible",
            value=
            "Your wealth is below the Nisab threshold.\nMinimum: 85 gold dinars or 595 silver dirhams",
            inline=False)

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="pay_zakat", description="Pay your Zakat obligation")
async def pay_zakat(interaction: discord.Interaction):
    """Pay Zakat obligation"""
    user_data = get_user_account(
        str(interaction.user.id), interaction.user.display_name
        or interaction.user.name)
    zakat_info = calculate_zakat(user_data['gold_dinars'],
                                 user_data['silver_dirhams'])

    if not zakat_info['eligible']:
        await interaction.response.send_message(
            "You are not currently eligible for Zakat payment (below Nisab threshold)."
        )
        return

    # Process Zakat payment
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        # Enable foreign key constraints
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()

        new_gold = user_data['gold_dinars'] - zakat_info['gold_zakat']
        new_silver = user_data['silver_dirhams'] - zakat_info['silver_zakat']
        new_charity_total = user_data['total_charity'] + zakat_info[
            'total_zakat']

        cursor.execute(
            '''
            UPDATE users SET 
            gold_dinars = ?, 
            silver_dirhams = ?, 
            total_charity = ?,
            last_zakat_payment = ?
            WHERE user_id = ?
        ''', (new_gold, new_silver, new_charity_total,
              datetime.datetime.now().isoformat(), user_data['user_id']))

        # Record transaction
        cursor.execute(
            '''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'zakat', zakat_info['total_zakat'],
              'mixed', 'Zakat payment - religious obligation'))

        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"Database error during zakat payment: {e}")
        await interaction.response.send_message(
            "❌ Database error occurred during Zakat payment. Please try again later."
        )
        return
    except Exception as e:
        print(f"Unexpected error during zakat payment: {e}")
        await interaction.response.send_message(
            "❌ An unexpected error occurred. Please try again later.")
        return

    embed = discord.Embed(
        title="🤲 Zakat Paid Successfully",
        description=
        "May Allah accept your Zakat and bless your remaining wealth",
        color=0x00AA00)

    embed.add_field(
        name="💸 Amount Paid",
        value=f"{zakat_info['total_zakat']:.2f} (combined currencies)",
        inline=False)

    embed.add_field(
        name="💰 Remaining Wealth",
        value=
        f"₯ {new_gold:.2f} gold dinars\n₯ {new_silver:.2f} silver dirhams",
        inline=False)

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="invest",
                  description="Make a Shariah-compliant investment")
async def invest(interaction: discord.Interaction, amount: float,
                 investment_type: str):
    """Make halal investments following Islamic principles"""

    halal_investments = [
        'agriculture', 'manufacturing', 'technology', 'healthcare',
        'education', 'renewable_energy', 'real_estate', 'halal_food'
    ]

    if investment_type.lower() not in halal_investments:
        await interaction.response.send_message(
            f"❌ Investment type '{investment_type}' is not available.\n"
            f"Halal options: {', '.join(halal_investments)}")
        return

    if amount <= 0:
        await interaction.response.send_message(
            "❌ Investment amount must be positive.")
        return

    user_data = get_user_account(
        str(interaction.user.id), interaction.user.display_name
        or interaction.user.name)

    if user_data['gold_dinars'] < amount:
        await interaction.response.send_message(
            f"❌ Insufficient gold dinars. You have {user_data['gold_dinars']:.2f}"
        )
        return

    # Process investment
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        # Enable foreign key constraints
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()

        # Deduct from user account
        new_gold = user_data['gold_dinars'] - amount
        cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?',
                       (new_gold, user_data['user_id']))

        # Record investment (profit-sharing basis) - More profitable now!
        profit_sharing_ratio = 0.80  # 80% to investor, 20% to investment manager
        cursor.execute(
            '''
            INSERT INTO investments (user_id, investment_type, amount, currency, profit_sharing_ratio)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], investment_type, amount, 'gold_dinars',
              profit_sharing_ratio))

        # Record transaction
        cursor.execute(
            '''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'investment', amount, 'gold_dinars',
              f'Shariah-compliant investment in {investment_type}'))

        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"Database error during investment: {e}")
        await interaction.response.send_message(
            "❌ Database error occurred during investment. Please try again later."
        )
        return
    except Exception as e:
        print(f"Unexpected error during investment: {e}")
        await interaction.response.send_message(
            "❌ An unexpected error occurred during investment. Please try again later."
        )
        return

    embed = discord.Embed(
        title="📈 Investment Successful",
        description=
        f"Your {amount:.2f} gold dinars have been invested in {investment_type}",
        color=0x0066CC)

    embed.add_field(
        name="💼 Investment Details",
        value=
        f"Type: {investment_type.title()}\nAmount: ₯{amount:.2f} gold dinars\nProfit Share: {profit_sharing_ratio*100}%",
        inline=False)

    embed.add_field(
        name="⚖️ Islamic Principles",
        value=
        "✅ Asset-backed\n✅ Profit-sharing (Mudarabah)\n✅ No interest (Riba-free)\n✅ Halal business",
        inline=False)

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="withdraw_investment",
                  description="Withdraw from an active investment")
async def withdraw_investment(interaction: discord.Interaction, investment_id: int):
    """Withdraw from an active Shariah-compliant investment"""
    
    user_data = get_user_account(
        str(interaction.user.id), interaction.user.display_name
        or interaction.user.name)
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check if investment exists and belongs to user
        cursor.execute('''
            SELECT id, investment_type, amount, currency, profit_sharing_ratio, start_date
            FROM investments 
            WHERE id = ? AND user_id = ? AND status = 'active'
        ''', (investment_id, user_data['user_id']))
        
        investment = cursor.fetchone()
        if not investment:
            await interaction.response.send_message(
                "❌ Investment not found or already withdrawn. Use `/my_investments` to see active investments."
            )
            conn.close()
            return
        
        inv_id, inv_type, amount, currency, profit_ratio, start_date = investment
        
        # Calculate investment duration (for profit calculation)
        start_time = datetime.datetime.fromisoformat(start_date)
        current_time = datetime.datetime.now()
        days_invested = (current_time - start_time).days
        
        # Calculate profit (5% annual return, proportional to days)
        annual_return_rate = 0.05
        profit = amount * annual_return_rate * (days_invested / 365.0) * profit_ratio
        total_return = amount + profit
        
        # Calculate taxes on profit
        taxes = calculate_taxes(profit, currency) if profit > 0 else 0.0
        net_return = total_return - taxes
        
        # Update user balance
        if currency == 'gold_dinars':
            new_gold = user_data['gold_dinars'] + net_return
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?',
                         (new_gold, user_data['user_id']))
        else:
            new_silver = user_data['silver_dirhams'] + net_return
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?',
                         (new_silver, user_data['user_id']))
        
        # Mark investment as withdrawn
        cursor.execute('''
            UPDATE investments SET status = 'withdrawn', end_date = ?
            WHERE id = ?
        ''', (current_time.isoformat(), inv_id))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'investment_withdrawal', net_return, currency,
              f'Withdrawal from {inv_type} investment (ID: {inv_id})'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="💰 Investment Withdrawn",
            description=f"Successfully withdrew from {inv_type} investment",
            color=0x00AA00
        )
        
        embed.add_field(
            name="📊 Withdrawal Details",
            value=f"Principal: ₯{amount:.2f}\nProfit: ₯{profit:.2f}\nTaxes: ₯{taxes:.2f}\n**Net Return: ₯{net_return:.2f}**",
            inline=False
        )
        
        embed.add_field(
            name="📅 Investment Period",
            value=f"Duration: {days_invested} days\nAnnual Return: {annual_return_rate*100}%\nYour Share: {profit_ratio*100}%",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except sqlite3.Error as e:
        print(f"Database error during investment withdrawal: {e}")
        await interaction.response.send_message(
            "❌ Database error occurred during withdrawal. Please try again later."
        )
    except Exception as e:
        print(f"Unexpected error during investment withdrawal: {e}")
        await interaction.response.send_message(
            "❌ An unexpected error occurred during withdrawal. Please try again later."
        )


@bot.tree.command(name="my_investments",
                  description="View your active and past investments")
async def my_investments(interaction: discord.Interaction):
    """View all user investments"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, investment_type, amount, currency, profit_sharing_ratio, start_date, status
            FROM investments 
            WHERE user_id = ?
            ORDER BY start_date DESC
        ''', (str(interaction.user.id),))
        
        investments = cursor.fetchall()
        conn.close()
        
        if not investments:
            await interaction.response.send_message("📈 You haven't made any investments yet! Use `/invest` to start building wealth.")
            return
        
        embed = discord.Embed(
            title="📈 Your Investment Portfolio",
            description="Your Shariah-compliant investments",
            color=0x0066CC
        )
        
        active_investments = []
        completed_investments = []
        
        for inv_id, inv_type, amount, currency, profit_ratio, start_date, status in investments:
            investment_info = f"ID: {inv_id} | ₯{amount:.2f} {currency.replace('_', ' ')} in {inv_type}"
            
            if status == 'active':
                active_investments.append(investment_info)
            else:
                completed_investments.append(f"{investment_info} ({status})")
        
        if active_investments:
            embed.add_field(
                name="🟢 Active Investments",
                value="\n".join(active_investments[:10]),  # Limit to first 10
                inline=False
            )
            embed.add_field(
                name="💡 Withdrawal",
                value="Use `/withdraw_investment [id]` to withdraw from an active investment",
                inline=False
            )
        
        if completed_investments:
            embed.add_field(
                name="📋 Past Investments",
                value="\n".join(completed_investments[:5]),  # Limit to first 5
                inline=False
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving investment portfolio.")
        print(f"Investment portfolio error: {e}")


@bot.tree.command(name="taxes", description="View tax information and calculations")
async def taxes_command(interaction: discord.Interaction):
    """Display tax information and personal tax calculations"""
    
    user_data = get_user_account(
        str(interaction.user.id), interaction.user.display_name
        or interaction.user.name)
    
    # Calculate potential taxes on current wealth
    gold_income_tax = calculate_taxes(user_data['gold_dinars'], 'gold_dinars')
    silver_income_tax = calculate_taxes(user_data['silver_dirhams'], 'silver_dirhams')
    
    embed = discord.Embed(
        title="📊 Islamic Tax System",
        description="Tax calculations and information for the Islamic economy",
        color=0xFF9900
    )
    
    embed.add_field(
        name="💰 Tax Rates & Thresholds",
        value=(
            "**Tax Rate:** 5% on income above thresholds\n"
            "**Gold Dinars:** ₯100.0 threshold\n"
            "**Silver Dirhams:** ₯150.0 threshold\n"
            "**Purpose:** Fund community infrastructure and welfare"
        ),
        inline=False
    )
    
    embed.add_field(
        name="📈 Your Current Tax Assessment",
        value=(
            f"Gold Holdings: ₯{user_data['gold_dinars']:.2f}\n"
            f"Potential Tax: ₯{gold_income_tax:.2f}\n\n"
            f"Silver Holdings: ₯{user_data['silver_dirhams']:.2f}\n"
            f"Potential Tax: ₯{silver_income_tax:.2f}"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🏢 When Taxes Apply",
        value=(
            "• **Work Income:** Automatically deducted from job payments\n"
            "• **Business Profits:** Deducted when collecting profits\n"
            "• **Investment Returns:** Applied to profit portions only\n"
            "• **Wealth Holdings:** Not taxed (use Zakat instead)"
        ),
        inline=False
    )
    
    embed.add_field(
        name="📿 Tax vs Zakat",
        value=(
            "**Taxes:** 5% on active income (work, business, investments)\n"
            "**Zakat:** 2.5% on total wealth above Nisab threshold\n"
            "Both follow Islamic principles of social responsibility"
        ),
        inline=False
    )
    
    embed.set_footer(
        text="Taxes fund community development • Zakat helps the needy • Both are Islamic obligations"
    )
    
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="usher", description="Learn about agricultural bonuses and benefits")
async def usher_command(interaction: discord.Interaction):
    """Display information about agricultural usher bonuses"""
    
    embed = discord.Embed(
        title="🌾 Agricultural Usher System",
        description="Special bonuses for agricultural sector participation",
        color=0x4CAF50
    )
    
    embed.add_field(
        name="🎁 Usher Bonus Benefits",
        value=(
            "**Work Bonus:** +15% extra pay for agricultural jobs\n"
            "**Business Discount:** 15% reduced startup costs\n"
            "**Profit Boost:** +15% increased daily profits\n"
            "**Applies to:** All agriculture-related activities"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🌱 Agricultural Jobs (with Usher)",
        value=(
            "• **Farmer** - ₯25.0 silver dirhams (+15% usher)\n"
            "• **Agricultural Engineer** - ₯30.0 gold dinars (+15% usher)\n"
            "• **Fisherman** - ₯22.0 silver dirhams (+15% usher)\n"
            "• **Butcher** - ₯26.0 silver dirhams (+15% usher)"
        ),
        inline=False
    )
    
    embed.add_field(
        name="🏭 Agricultural Businesses (with Usher)",
        value=(
            "• **Agriculture** - ₯42,500 startup (15% discount), ₯34.5 daily profit (+15%)\n"
            "• **Organic Farming** - ₯42,500 startup (15% discount), ₯37.4 daily profit (+15%)\n"
            "Lower costs, higher returns!"
        ),
        inline=False
    )
    
    embed.add_field(
        name="📚 Islamic Basis",
        value=(
            "Agricultural work is highly valued in Islam as it:\n"
            "• Provides sustenance for the community\n"
            "• Follows prophetic traditions of farming\n"
            "• Promotes self-sufficiency and food security\n"
            "• Encourages stewardship of Allah's earth"
        ),
        inline=False
    )
    
    embed.add_field(
        name="💡 How to Benefit",
        value=(
            "The usher bonus is **automatically applied** when you:\n"
            "• Work agricultural jobs (`/work` as farmer, etc.)\n"
            "• Start agricultural businesses (`/start_business`)\n"
            "• Collect profits from agricultural businesses"
        ),
        inline=False
    )
    
    embed.set_footer(
        text="🌾 Blessed work provides blessed rewards • Agriculture feeds the Ummah"
    )
    
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="trade",
    description="Trade with another user using Islamic principles")
async def trade(interaction: discord.Interaction, partner: discord.Member,
                amount: float, currency: str):
    """Conduct halal trade transactions"""

    if currency.lower() not in ['gold', 'silver']:
        await interaction.response.send_message(
            "❌ Currency must be 'gold' or 'silver'")
        return

    if amount <= 0:
        await interaction.response.send_message(
            "❌ Trade amount must be positive.")
        return

    if partner.id == interaction.user.id:
        await interaction.response.send_message("❌ Cannot trade with yourself."
                                                )
        return

    user_data = get_user_account(
        str(interaction.user.id), interaction.user.display_name
        or interaction.user.name)
    partner_data = get_user_account(str(partner.id), partner.display_name
                                    or partner.name)

    # Check if user has sufficient funds
    if currency.lower() == 'gold' and user_data['gold_dinars'] < amount:
        await interaction.response.send_message(
            f"❌ Insufficient gold dinars. You have {user_data['gold_dinars']:.2f}"
        )
        return
    elif currency.lower() == 'silver' and user_data['silver_dirhams'] < amount:
        await interaction.response.send_message(
            f"❌ Insufficient silver dirhams. You have {user_data['silver_dirhams']:.2f}"
        )
        return

    # Simple trade confirmation
    embed = discord.Embed(
        title="🤝 Trade Proposal",
        description=
        f"{interaction.user.mention} wants to trade {amount:.2f} {currency} dinars/dirhams with {partner.mention}",
        color=0xFFAA00)

    embed.add_field(
        name="📋 Trade Details",
        value=
        f"Sender: {interaction.user.display_name or interaction.user.name}\nReceiver: {partner.display_name or partner.name}\nAmount: ₯{amount:.2f} {currency}",
        inline=False)

    embed.add_field(
        name="⚖️ Islamic Trade Principles",
        value=
        "✅ Voluntary exchange\n✅ Clear terms\n✅ No exploitation\n✅ Asset-backed currency",
        inline=False)

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="islamic_finance_info",
                  description="Learn about Islamic finance principles")
async def islamic_finance_info(interaction: discord.Interaction):
    """Educational content about Islamic finance"""
    embed = discord.Embed(
        title="🕌 DigiCap - Islamic Finance Principles",
        description="Learn about Shariah-compliant economic practices",
        color=0x006600)

    embed.add_field(
        name="🚫 Prohibited (Haram)",
        value=
        "• **Riba** - Interest-based lending\n• **Gharar** - Excessive uncertainty\n• **Maysir** - Gambling/speculation\n• Investing in alcohol, tobacco, gambling",
        inline=False)

    embed.add_field(
        name="✅ Encouraged (Halal)",
        value=
        "• **Mudarabah** - Profit-sharing partnerships\n• **Musharakah** - Joint ventures\n• **Murabaha** - Cost-plus financing\n• Asset-backed transactions",
        inline=False)

    embed.add_field(
        name="📿 Zakat",
        value=
        "• 2.5% of eligible wealth annually\n• Nisab: 85g gold or 595g silver\n• Purifies wealth and helps poor",
        inline=False)

    embed.set_footer(
        text="DigiCap implements these principles in its economy system")

    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="ping", description="Check if the bot is responsive")
async def ping(interaction: discord.Interaction):
    """Simple ping command to test bot responsiveness"""
    latency = round(bot.latency * 1000)
    await interaction.response.send_message(
        f"🏓 Pong! Latency: {latency}ms\nAsalamu Alaikum!")

@bot.tree.command(name="sync", description="Sync all slash commands (Admin only)")
async def sync_commands(interaction: discord.Interaction):
    """Sync slash commands - useful for updating commands"""
    # Check if user has admin permissions or is server owner
    if not interaction.guild:
        await interaction.response.send_message("❌ This command can only be used in a server.", ephemeral=True)
        return
    
    member = interaction.guild.get_member(interaction.user.id)
    is_owner = interaction.guild.owner_id == interaction.user.id
    has_admin = member and member.guild_permissions.administrator
    
    if not (is_owner or has_admin):
        await interaction.response.send_message("❌ Only server owners or administrators can sync commands.", ephemeral=True)
        return
    
    try:
        await interaction.response.defer(ephemeral=True)
        synced = await bot.tree.sync()
        await interaction.followup.send(f"✅ Successfully synced {len(synced)} command(s)!", ephemeral=True)
        print(f"Commands synced by {interaction.user}: {len(synced)} commands")
    except Exception as e:
        await interaction.followup.send(f"❌ Failed to sync commands: {str(e)}", ephemeral=True)
        print(f"Sync error: {e}")

@bot.tree.command(name="set_balance", description="Set any user's balance (Server Owner Only)")
async def set_balance(interaction: discord.Interaction, user: discord.Member, gold_dinars: float = 0.0, silver_dirhams: float = 0.0):
    """Server owner command to set any user's balance"""
    
    # Check if user is server owner
    if not interaction.guild or interaction.guild.owner_id != interaction.user.id:
        await interaction.response.send_message("❌ Only the server owner can use this command.", ephemeral=True)
        return
    
    if gold_dinars is None and silver_dirhams is None:
        await interaction.response.send_message("❌ Please specify at least one currency amount to set.", ephemeral=True)
        return
    
    if gold_dinars is not None and gold_dinars < 0:
        await interaction.response.send_message("❌ Gold dinars amount cannot be negative.", ephemeral=True)
        return
        
    if silver_dirhams is not None and silver_dirhams < 0:
        await interaction.response.send_message("❌ Silver dirhams amount cannot be negative.", ephemeral=True)
        return
    
    try:
        # Get current user data
        user_data = get_user_account(str(user.id), user.display_name or user.name)
        
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Set new balances (keep existing if not specified)
        new_gold = gold_dinars if gold_dinars is not None else user_data['gold_dinars']
        new_silver = silver_dirhams if silver_dirhams is not None else user_data['silver_dirhams']
        
        # Update user balance
        cursor.execute('''
            UPDATE users SET gold_dinars = ?, silver_dirhams = ? WHERE user_id = ?
        ''', (new_gold, new_silver, str(user.id)))
        
        # Record admin transaction for audit trail
        if gold_dinars is not None:
            difference_gold = new_gold - user_data['gold_dinars']
            if difference_gold != 0:
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (str(user.id), 'admin_adjustment', abs(difference_gold), 'gold_dinars', 
                     f'Balance {"increased" if difference_gold > 0 else "decreased"} by server owner', str(interaction.user.id)))
        
        if silver_dirhams is not None:
            difference_silver = new_silver - user_data['silver_dirhams']
            if difference_silver != 0:
                cursor.execute('''
                    INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (str(user.id), 'admin_adjustment', abs(difference_silver), 'silver_dirhams',
                     f'Balance {"increased" if difference_silver > 0 else "decreased"} by server owner', str(interaction.user.id)))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="👑 Balance Updated by Server Owner",
            description=f"Successfully updated {user.display_name or user.name}'s balance",
            color=0xFF6600
        )
        
        embed.add_field(
            name="💰 New Balances",
            value=f"Gold Dinars: ₯{new_gold:.2f}\nSilver Dirhams: ₯{new_silver:.2f}",
            inline=False
        )
        
        changes = []
        if gold_dinars is not None:
            diff_gold = new_gold - user_data['gold_dinars']
            changes.append(f"Gold: {user_data['gold_dinars']:.2f} → {new_gold:.2f} ({diff_gold:+.2f})")
        if silver_dirhams is not None:
            diff_silver = new_silver - user_data['silver_dirhams']
            changes.append(f"Silver: {user_data['silver_dirhams']:.2f} → {new_silver:.2f} ({diff_silver:+.2f})")
        
        if changes:
            embed.add_field(
                name="📊 Changes Made",
                value="\n".join(changes),
                inline=False
            )
        
        embed.set_footer(text="🔒 Admin action logged for audit purposes")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error updating user balance. Please try again later.")
        print(f"Set balance error: {e}")

@bot.tree.command(name="give_money", description="Give money to any user (Server Owner Only)")
async def give_money(interaction: discord.Interaction, user: discord.Member, amount: float, currency: str):
    """Server owner command to give money to users (adds to existing balance)"""
    
    # Check if user is server owner
    if not interaction.guild or interaction.guild.owner_id != interaction.user.id:
        await interaction.response.send_message("❌ Only the server owner can use this command.", ephemeral=True)
        return
    
    if currency.lower() not in ['gold', 'silver']:
        await interaction.response.send_message("❌ Currency must be 'gold' or 'silver'", ephemeral=True)
        return
    
    if amount <= 0:
        await interaction.response.send_message("❌ Amount must be positive.", ephemeral=True)
        return
    
    try:
        # Get current user data
        user_data = get_user_account(str(user.id), user.display_name or user.name)
        
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Add to existing balance
        if currency.lower() == 'gold':
            new_balance = user_data['gold_dinars'] + amount
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_balance, str(user.id)))
            currency_full = 'gold_dinars'
            currency_display = 'Gold Dinars'
        else:
            new_balance = user_data['silver_dirhams'] + amount
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_balance, str(user.id)))
            currency_full = 'silver_dirhams'
            currency_display = 'Silver Dirhams'
        
        # Record admin transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (str(user.id), 'admin_gift', amount, currency_full, 
             f'Money gift from server owner', str(interaction.user.id)))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🎁 Money Gift from Server Owner",
            description=f"Successfully gave ₯{amount:.2f} {currency_display} to {user.display_name or user.name}!",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💰 Gift Details",
            value=f"Amount: ₯{amount:.2f} {currency_display}\nRecipient: {user.display_name or user.name}\nNew Balance: ₯{new_balance:.2f}",
            inline=False
        )
        
        embed.set_footer(text="🔒 Admin gift logged for audit purposes")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error giving money. Please try again later.")
        print(f"Give money error: {e}")

# === JOB SYSTEM ===
@bot.tree.command(name="work", description="Work at your current job to earn money")
async def work_job(interaction: discord.Interaction, job_title: str = ""):
    """Work at your current job or a specific job with experience bonuses"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # If no job_title provided, check for active job
        if not job_title:
            cursor.execute('''
                SELECT job_title, salary, currency FROM jobs 
                WHERE user_id = ? AND status = 'active'
            ''', (str(interaction.user.id),))
            
            active_job = cursor.fetchone()
            if not active_job:
                await interaction.response.send_message(
                    "❌ You don't have an active job!\n"
                    "Use `/get_job [job_title]` to get hired first, or use `/work [job_title]` for freelance work."
                )
                conn.close()
                return
            
            job_title, current_salary, currency = active_job
            
            # Calculate taxes on current salary
            taxes = calculate_taxes(current_salary, currency)
            net_pay = current_salary - taxes
            
            # Work the active job
            user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
            
            # Update user balance
            if currency == 'gold_dinars':
                new_gold = user_data['gold_dinars'] + net_pay
                cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                             (new_gold, user_data['user_id']))
            else:
                new_silver = user_data['silver_dirhams'] + net_pay
                cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                             (new_silver, user_data['user_id']))
            
            # Record transaction
            cursor.execute('''
                INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_data['user_id'], 'job_income', net_pay, currency, f'Worked as {job_title} (employed)'))
            
            conn.commit()
            conn.close()
            
            embed = discord.Embed(
                title="💼 Work Shift Completed!",
                description=f"You completed a work shift as {job_title}!",
                color=0x00AA00
            )
            
            embed.add_field(
                name="💰 Pay Summary",
                value=f"Gross Pay: ₯{current_salary:.2f}\nTaxes: ₯{taxes:.2f}\n**Net Pay: ₯{net_pay:.2f}**",
                inline=False
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        conn.close()
        
        # Freelance work system (original system)
        available_jobs = {
            # Agricultural & Food
            'farmer': {'base_pay': 25.0, 'currency': 'silver_dirhams', 'is_agricultural': True},
            'agricultural_engineer': {'base_pay': 30.0, 'currency': 'gold_dinars', 'is_agricultural': True},
            'fisherman': {'base_pay': 22.0, 'currency': 'silver_dirhams', 'is_agricultural': True},
            'chef': {'base_pay': 24.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
            'butcher': {'base_pay': 26.0, 'currency': 'silver_dirhams', 'is_agricultural': True},
            
            # Medical & Healthcare
            'doctor': {'base_pay': 45.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'nurse': {'base_pay': 28.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'pharmacist': {'base_pay': 35.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'dentist': {'base_pay': 40.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            
            # Education & Religious
            'teacher': {'base_pay': 18.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'imam': {'base_pay': 20.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'scholar': {'base_pay': 25.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'translator': {'base_pay': 22.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            
            # Business & Trade
            'merchant': {'base_pay': 15.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'accountant': {'base_pay': 32.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'lawyer': {'base_pay': 38.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'banker': {'base_pay': 35.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            
            # Crafts & Construction
            'craftsman': {'base_pay': 20.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
            'carpenter': {'base_pay': 27.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
            'builder': {'base_pay': 29.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
            'architect': {'base_pay': 42.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'engineer': {'base_pay': 40.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'blacksmith': {'base_pay': 31.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
            
            # Services & Arts
            'tailor': {'base_pay': 23.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
            'jeweler': {'base_pay': 33.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'calligrapher': {'base_pay': 28.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'librarian': {'base_pay': 19.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            
            # Technology & Modern
            'programmer': {'base_pay': 44.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'web_designer': {'base_pay': 36.0, 'currency': 'gold_dinars', 'is_agricultural': False},
            'data_analyst': {'base_pay': 39.0, 'currency': 'gold_dinars', 'is_agricultural': False}
        }
        
        if job_title.lower() not in available_jobs:
            await interaction.response.send_message(
                f"❌ Job '{job_title}' not available.\n"
                f"Available jobs: {', '.join(available_jobs.keys())}"
            )
            return
    
    except Exception as e:
        await interaction.response.send_message("❌ Error checking job status.")
        print(f"Work job check error: {e}")
        return
    
    user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
    job_info = available_jobs[job_title.lower()]
    
    # Calculate pay with experience bonus
    base_pay = job_info['base_pay']
    experience_bonus = calculate_job_experience_bonus(job_title.lower(), str(interaction.user.id))
    
    # Agricultural usher bonus
    agricultural_bonus = get_agricultural_usher_bonus() if job_info['is_agricultural'] else 0.0
    
    total_bonus = experience_bonus + agricultural_bonus
    final_pay = base_pay * (1 + total_bonus)
    
    # Calculate taxes
    taxes = calculate_taxes(final_pay, job_info['currency'])
    net_pay = final_pay - taxes
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Update user balance
        if job_info['currency'] == 'gold_dinars':
            new_gold = user_data['gold_dinars'] + net_pay
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_gold, user_data['user_id']))
        else:
            new_silver = user_data['silver_dirhams'] + net_pay
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_silver, user_data['user_id']))
        
        # Record job completion
        cursor.execute('''
            INSERT INTO jobs (user_id, job_title, employer, salary, currency, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (user_data['user_id'], job_title.lower(), 'Islamic Economy Corp', net_pay, job_info['currency'], 'completed'))
        
        # Record transaction
        description = f"Work as {job_title}"
        if job_info['is_agricultural']:
            description += " (Agricultural Usher Bonus)"
        
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'job_income', net_pay, job_info['currency'], description))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="💼 Work Completed!",
            description=f"You worked as a {job_title} and earned money!",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💰 Earnings Breakdown",
            value=f"Base Pay: ₯{base_pay:.2f}\nExperience Bonus: {experience_bonus*100:.0f}%\n"
                  f"{'Agricultural Usher: +15%' if job_info['is_agricultural'] else ''}\n"
                  f"**Gross Pay: ₯{final_pay:.2f}**\nTaxes: ₯{taxes:.2f}\n**Net Pay: ₯{net_pay:.2f}**",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Database error occurred during work. Please try again later.")
        print(f"Work error: {e}")

@bot.tree.command(name="job_history", description="View your job history and experience")
async def job_history(interaction: discord.Interaction):
    """View job history and experience levels"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT job_title, COUNT(*) as times_worked, AVG(salary) as avg_salary, currency
            FROM jobs WHERE user_id = ? AND status = 'completed'
            GROUP BY job_title, currency
            ORDER BY times_worked DESC
        ''', (str(interaction.user.id),))
        
        job_stats = cursor.fetchall()
        conn.close()
        
        if not job_stats:
            await interaction.response.send_message("📝 You haven't worked any jobs yet! Use `/work` to start earning.")
            return
        
        embed = discord.Embed(
            title="📊 Your Job Experience",
            description="Your work history and experience bonuses",
            color=0x0066CC
        )
        
        for job_title, times_worked, avg_salary, currency in job_stats:
            bonus_percent = min(times_worked * 10, 50)
            embed.add_field(
                name=f"👔 {job_title.title()}",
                value=f"Times Worked: {times_worked}\nExperience Bonus: {bonus_percent}%\nAvg Salary: ₯{avg_salary:.2f} {currency.replace('_', ' ')}",
                inline=True
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving job history.")
        print(f"Job history error: {e}")

@bot.tree.command(name="get_job", description="Get hired for a job (become employed)")
async def get_job(interaction: discord.Interaction, job_title: str):
    """Get hired for a job - creates an active employment"""
    
    available_jobs = {
        # Agricultural & Food
        'farmer': {'base_salary': 25.0, 'currency': 'silver_dirhams', 'is_agricultural': True},
        'agricultural_engineer': {'base_salary': 30.0, 'currency': 'gold_dinars', 'is_agricultural': True},
        'fisherman': {'base_salary': 22.0, 'currency': 'silver_dirhams', 'is_agricultural': True},
        'chef': {'base_salary': 24.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
        'butcher': {'base_salary': 26.0, 'currency': 'silver_dirhams', 'is_agricultural': True},
        
        # Medical & Healthcare
        'doctor': {'base_salary': 45.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'nurse': {'base_salary': 28.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'pharmacist': {'base_salary': 35.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'dentist': {'base_salary': 40.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        
        # Education & Religious
        'teacher': {'base_salary': 18.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'imam': {'base_salary': 20.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'scholar': {'base_salary': 25.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'translator': {'base_salary': 22.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        
        # Business & Trade
        'merchant': {'base_salary': 15.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'accountant': {'base_salary': 32.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'lawyer': {'base_salary': 38.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'banker': {'base_salary': 35.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        
        # Crafts & Construction
        'craftsman': {'base_salary': 20.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
        'carpenter': {'base_salary': 27.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
        'builder': {'base_salary': 29.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
        'architect': {'base_salary': 42.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'engineer': {'base_salary': 40.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'blacksmith': {'base_salary': 31.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
        
        # Services & Arts
        'tailor': {'base_salary': 23.0, 'currency': 'silver_dirhams', 'is_agricultural': False},
        'jeweler': {'base_salary': 33.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'calligrapher': {'base_salary': 28.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'librarian': {'base_salary': 19.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        
        # Technology & Modern
        'programmer': {'base_salary': 44.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'web_designer': {'base_salary': 36.0, 'currency': 'gold_dinars', 'is_agricultural': False},
        'data_analyst': {'base_salary': 39.0, 'currency': 'gold_dinars', 'is_agricultural': False}
    }
    
    if job_title.lower() not in available_jobs:
        await interaction.response.send_message(
            f"❌ Job '{job_title}' not available.\n"
            f"Available jobs: {', '.join(available_jobs.keys())}"
        )
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check if user already has an active job
        cursor.execute('''
            SELECT job_title FROM jobs 
            WHERE user_id = ? AND status = 'active'
        ''', (str(interaction.user.id),))
        
        current_job = cursor.fetchone()
        if current_job:
            await interaction.response.send_message(
                f"❌ You already have an active job as {current_job[0]}!\n"
                f"Use `/quit_job` to leave your current position first."
            )
            conn.close()
            return
        
        # Calculate salary with experience bonus
        job_info = available_jobs[job_title.lower()]
        base_salary = job_info['base_salary']
        experience_bonus = calculate_job_experience_bonus(job_title.lower(), str(interaction.user.id))
        
        # Agricultural usher bonus
        agricultural_bonus = get_agricultural_usher_bonus() if job_info['is_agricultural'] else 0.0
        
        total_bonus = experience_bonus + agricultural_bonus
        final_salary = base_salary * (1 + total_bonus)
        
        # Create active job
        cursor.execute('''
            INSERT INTO jobs (user_id, job_title, employer, salary, currency, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), job_title.lower(), 'Islamic Economy Corp', final_salary, job_info['currency'], 'active'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🎉 Job Offer Accepted!",
            description=f"Congratulations! You're now employed as a {job_title}!",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💼 Employment Details",
            value=f"Position: {job_title.title()}\nBase Salary: ₯{base_salary:.2f}\nExperience Bonus: {experience_bonus*100:.0f}%\n"
                  f"{'Agricultural Usher: +15%' if job_info['is_agricultural'] else ''}\n"
                  f"**Final Salary: ₯{final_salary:.2f} per work session**",
            inline=False
        )
        
        embed.add_field(
            name="📋 Next Steps",
            value="• Use `/work` (without job title) to work your current job\n• Use `/current_job` to check your employment status\n• Use `/quit_job` when you want to leave",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error during job application. Please try again later.")
        print(f"Get job error: {e}")

@bot.tree.command(name="current_job", description="View your current employment status")
async def current_job(interaction: discord.Interaction):
    """View current active job"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT job_title, employer, salary, currency, start_date
            FROM jobs WHERE user_id = ? AND status = 'active'
        ''', (str(interaction.user.id),))
        
        job = cursor.fetchone()
        conn.close()
        
        if not job:
            await interaction.response.send_message(
                "💼 You're currently unemployed.\nUse `/get_job [job_title]` to find employment!"
            )
            return
        
        job_title, employer, salary, currency, start_date = job
        
        embed = discord.Embed(
            title="💼 Current Employment",
            description="Your current job status",
            color=0x0066CC
        )
        
        embed.add_field(
            name="📋 Job Details",
            value=f"Position: {job_title.title()}\nEmployer: {employer}\nSalary: ₯{salary:.2f} {currency.replace('_', ' ')}\nHired: {start_date}",
            inline=False
        )
        
        embed.add_field(
            name="🔧 Actions Available",
            value="• `/work` - Work your current job\n• `/quit_job` - Leave this position",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error checking employment status.")
        print(f"Current job error: {e}")

@bot.tree.command(name="quit_job", description="Leave your current job")
async def quit_job(interaction: discord.Interaction):
    """Quit current active job"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check if user has an active job
        cursor.execute('''
            SELECT job_title, salary, currency FROM jobs 
            WHERE user_id = ? AND status = 'active'
        ''', (str(interaction.user.id),))
        
        job = cursor.fetchone()
        if not job:
            await interaction.response.send_message("❌ You don't have an active job to quit!")
            conn.close()
            return
        
        job_title, salary, currency = job
        
        # Update job status to 'quit'
        cursor.execute('''
            UPDATE jobs SET status = 'quit'
            WHERE user_id = ? AND status = 'active'
        ''', (str(interaction.user.id),))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="👋 Job Resignation",
            description=f"You have successfully quit your job as {job_title}",
            color=0xFF6600
        )
        
        embed.add_field(
            name="📋 Final Details",
            value=f"Former Position: {job_title.title()}\nFinal Salary: ₯{salary:.2f} {currency.replace('_', ' ')}",
            inline=False
        )
        
        embed.add_field(
            name="🔍 What's Next?",
            value="• Use `/get_job [job_title]` to find new employment\n• Your experience from this job will give bonuses if you return!",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error quitting job. Please try again later.")
        print(f"Quit job error: {e}")

# === BUSINESS SYSTEM ===
@bot.tree.command(name="start_business", description="Start a halal business")
async def start_business(interaction: discord.Interaction, business_name: str, business_type: str):
    """Start a profitable halal business"""
    
    business_types = {
        'agriculture': {'startup_cost': 50000.0, 'daily_profit': 30.0, 'is_agricultural': True},
        'halal_restaurant': {'startup_cost': 50000.0, 'daily_profit': 45.0, 'is_agricultural': False},
        'islamic_finance': {'startup_cost': 50000.0, 'daily_profit': 50.0, 'is_agricultural': False},
        'tech_consulting': {'startup_cost': 50000.0, 'daily_profit': 37.5, 'is_agricultural': False},
        'renewable_energy': {'startup_cost': 50000.0, 'daily_profit': 57.5, 'is_agricultural': False},
        'organic_farming': {'startup_cost': 50000.0, 'daily_profit': 32.5, 'is_agricultural': True}
    }
    
    if business_type.lower() not in business_types:
        await interaction.response.send_message(
            f"❌ Business type '{business_type}' not available.\n"
            f"Available types: {', '.join(business_types.keys())}"
        )
        return
    
    user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
    biz_info = business_types[business_type.lower()]
    startup_cost = biz_info['startup_cost']
    
    # Agricultural usher bonus reduces startup cost
    if biz_info['is_agricultural']:
        startup_cost *= (1 - get_agricultural_usher_bonus())
    
    if user_data['gold_dinars'] < startup_cost:
        await interaction.response.send_message(
            f"❌ Insufficient funds. Need ₯{startup_cost:.2f} gold dinars, you have ₯{user_data['gold_dinars']:.2f}"
        )
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Deduct startup cost
        new_gold = user_data['gold_dinars'] - startup_cost
        cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                     (new_gold, user_data['user_id']))
        
        # Create business with license code
        daily_profit = biz_info['daily_profit']
        if biz_info['is_agricultural']:
            daily_profit *= (1 + get_agricultural_usher_bonus())
        
        # Generate unique license code with retry logic
        license_code = None  # Initialize to prevent unbound variable
        max_insert_attempts = 5
        for attempt in range(max_insert_attempts):
            try:
                license_code = generate_unique_license_code()
                
                cursor.execute('''
                    INSERT INTO businesses (user_id, business_name, business_type, startup_cost, daily_profit, license_code)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (user_data['user_id'], business_name, business_type.lower(), startup_cost, daily_profit, license_code))
                break  # Success, exit retry loop
            except sqlite3.IntegrityError:
                # License code collision, retry
                if attempt == max_insert_attempts - 1:
                    raise Exception("Unable to generate unique license code after multiple attempts")
                continue
        
        # Ensure license_code was generated successfully
        if not license_code:
            raise Exception("Failed to generate license code")
        
        # Get the business ID for share ownership
        business_id = cursor.lastrowid
        
        # Business ownership follows Islamic partnership principles (Musharakah)
        # No share trading - owner maintains full Islamic partnership
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'business_startup', startup_cost, 'gold_dinars', f'Started {business_type} business: {business_name}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🏢 Business Started!",
            description=f"Congratulations! You've started '{business_name}'",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💼 Business Details",
            value=f"Type: {business_type.title()}\nStartup Cost: ₯{startup_cost:.2f}\nDaily Profit: ₯{daily_profit:.2f}\n"
                  f"{'🌾 Agricultural Usher Benefits Applied!' if biz_info['is_agricultural'] else ''}",
            inline=False
        )
        
        embed.add_field(
            name="🤝 Islamic Ownership",
            value="You now own this business following Islamic principles!\nNo share trading - this is halal business ownership.",
            inline=False
        )
        
        embed.add_field(
            name="🔐 Business License",
            value=f"License Code: `{license_code[:7]}*****`\nFull license sent privately. Use `/view_license` anytime.",
            inline=False
        )
        
        embed.add_field(
            name="📈 Next Steps",
            value="Use `/collect_profit` to collect daily profits!\nUse `/my_businesses` to manage your businesses.\nUse `/close_business` to close a business for 30% compensation.\nBusiness follows Islamic partnership principles.",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
        # Send full license code privately
        license_embed = discord.Embed(
            title="🔐 Your Business License (Private)",
            description=f"Full license for '{business_name}'",
            color=0x4CAF50
        )
        license_embed.add_field(
            name="🔑 Complete License Code",
            value=f"`{license_code}`",
            inline=False
        )
        license_embed.set_footer(text="⚠️ Keep this code secure and confidential")
        
        try:
            await interaction.user.send(embed=license_embed)
        except:
            # Fallback if DM fails - send ephemeral
            await interaction.followup.send(embed=license_embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error starting business. Please try again later.")
        print(f"Business startup error: {e}")

@bot.tree.command(name="collect_profit", description="Collect profits from your businesses")
async def collect_profit(interaction: discord.Interaction):
    """Collect daily profits from businesses"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, business_name, business_type, daily_profit, created_date, last_collection_date
            FROM businesses WHERE user_id = ? AND status = 'active'
        ''', (str(interaction.user.id),))
        
        businesses = cursor.fetchall()
        
        if not businesses:
            await interaction.response.send_message("🏢 You don't have any active businesses! Use `/start_business` to begin.")
            return
        
        total_profit = 0.0
        business_details = []
        businesses_to_update = []
        
        for biz_id, name, biz_type, daily_profit, created_date, last_collection_date in businesses:
            # Calculate time-based profit (allow collection every 4 hours)
            current_time = datetime.datetime.now()
            
            # Use last collection date if available, otherwise use creation date
            if last_collection_date:
                last_time = datetime.datetime.fromisoformat(last_collection_date)
            else:
                last_time = datetime.datetime.fromisoformat(created_date)
            
            hours_passed = (current_time - last_time).total_seconds() / 3600
            
            # Calculate 4-hourly profit (daily profit / 6 periods per day)
            period_profit = daily_profit / 6
            periods_passed = int(hours_passed // 4)
            available_profit = period_profit * periods_passed
            
            if available_profit > 0.05:  # Minimum 0.05 to collect
                total_profit += available_profit
                business_details.append(f"• {name}: ₯{available_profit:.2f}")
                businesses_to_update.append(biz_id)
        
        if total_profit < 0.05:
            await interaction.response.send_message("⏰ No profits available to collect yet. Try again in a few hours!")
            return
        
        # Calculate taxes on business income
        taxes = calculate_taxes(total_profit, 'gold_dinars')
        net_profit = total_profit - taxes
        
        # Update user balance
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        new_gold = user_data['gold_dinars'] + net_profit
        
        cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                     (new_gold, user_data['user_id']))
        
        # Update last collection date for businesses that generated profit
        current_time_str = datetime.datetime.now().isoformat()
        for business_id in businesses_to_update:
            cursor.execute('UPDATE businesses SET last_collection_date = ? WHERE id = ?', 
                         (current_time_str, business_id))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'business_profit', net_profit, 'gold_dinars', 'Business profit collection'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="💰 Profits Collected!",
            description="You've successfully collected business profits!",
            color=0x00AA00
        )
        
        embed.add_field(
            name="📊 Profit Breakdown",
            value=f"Gross Profit: ₯{total_profit:.2f}\nTaxes (5%): ₯{taxes:.2f}\n**Net Profit: ₯{net_profit:.2f}**",
            inline=False
        )
        
        embed.add_field(
            name="🏢 Business Contributions",
            value="\n".join(business_details) if business_details else "No profits available",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error collecting profits. Please try again later.")
        print(f"Profit collection error: {e}")

@bot.tree.command(name="my_businesses", description="View and manage your businesses")
async def my_businesses(interaction: discord.Interaction):
    """View all user businesses"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT business_name, business_type, daily_profit, created_date, status, license_code
            FROM businesses WHERE user_id = ?
            ORDER BY created_date DESC
        ''', (str(interaction.user.id),))
        
        businesses = cursor.fetchall()
        conn.close()
        
        if not businesses:
            await interaction.response.send_message("🏢 You don't own any businesses yet! Use `/start_business` to begin your entrepreneurial journey.")
            return
        
        embed = discord.Embed(
            title="🏢 Your Business Empire",
            description="Overview of all your businesses",
            color=0x0066CC
        )
        
        total_daily_profit = 0.0
        for name, biz_type, daily_profit, created_date, status, license_code in businesses:
            if status == 'active':
                total_daily_profit += daily_profit
            
            status_emoji = "✅" if status == 'active' else "❌"
            license_text = f"\nLicense: {license_code[:7]}*****" if license_code else "\nLicense: Not Generated"
            embed.add_field(
                name=f"{status_emoji} {name}",
                value=f"Type: {biz_type.title()}\nDaily Profit: ₯{daily_profit:.2f}\nStatus: {status.title()}{license_text}",
                inline=True
            )
        
        embed.add_field(
            name="📈 Total Daily Profit",
            value=f"₯{total_daily_profit:.2f} from active businesses",
            inline=False
        )
        
        embed.add_field(
            name="🔐 License Management",
            value="Use `/view_license [business_name]` to see a specific license\nUse `/generate_license [business_name]` for businesses without licenses",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving businesses.")
        print(f"Business retrieval error: {e}")

@bot.tree.command(name="close_business", description="Close a business and receive 30% compensation")
async def close_business(interaction: discord.Interaction, business_name: str):
    """Close a business and receive 30% of startup cost as compensation"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check if user owns the business and it's active
        cursor.execute('''
            SELECT id, business_type, startup_cost, daily_profit FROM businesses 
            WHERE user_id = ? AND business_name = ? AND status = 'active'
        ''', (str(interaction.user.id), business_name))
        
        business = cursor.fetchone()
        if not business:
            await interaction.response.send_message(f"❌ Business '{business_name}' not found or already closed. Use `/my_businesses` to see your active businesses.")
            conn.close()
            return
        
        business_id, business_type, startup_cost, daily_profit = business
        
        # Calculate 30% compensation
        compensation = startup_cost * 0.30
        
        # Get user data to update gold
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        new_gold = user_data['gold_dinars'] + compensation
        
        # Update user gold
        cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                     (new_gold, str(interaction.user.id)))
        
        # Update business status to closed
        cursor.execute('UPDATE businesses SET status = ? WHERE id = ?', 
                     ('closed', business_id))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'business_closure', compensation, 'gold_dinars', f'Closed {business_type} business: {business_name}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🏢 Business Closed",
            description=f"You have successfully closed '{business_name}'",
            color=0xFF6600
        )
        
        embed.add_field(
            name="💼 Business Details",
            value=f"Type: {business_type.title()}\nOriginal Startup Cost: ₯{startup_cost:.2f}\nFormer Daily Profit: ₯{daily_profit:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="💰 Compensation Received",
            value=f"₯{compensation:.2f} (30% of startup cost)\nNew Balance: ₯{new_gold:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="📈 What's Next?",
            value="• Use `/start_business` to start a new business\n• Your experience may provide benefits for future ventures!",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error closing business. Please try again later.")
        print(f"Business closure error: {e}")


# === BUSINESS LICENSE MANAGEMENT ===

@bot.tree.command(name="view_license", description="View the license code for your business")
async def view_license(interaction: discord.Interaction, business_name: str):
    """View the license code for a specific business (owner only)"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if user owns the business
        cursor.execute('''
            SELECT business_name, business_type, license_code, created_date, status
            FROM businesses WHERE user_id = ? AND business_name = ?
        ''', (str(interaction.user.id), business_name))
        
        business = cursor.fetchone()
        conn.close()
        
        if not business:
            await interaction.response.send_message(f"❌ Business '{business_name}' not found. Use `/my_businesses` to see your businesses.")
            return
        
        name, biz_type, license_code, created_date, status = business
        
        if not license_code:
            await interaction.response.send_message(
                f"❌ Business '{business_name}' doesn't have a license code yet.\n"
                f"Use `/generate_license {business_name}` to create one."
            )
            return
        
        embed = discord.Embed(
            title="🔐 Business License",
            description=f"License information for '{business_name}'",
            color=0x4CAF50
        )
        
        embed.add_field(
            name="💼 Business Details",
            value=f"Name: {name}\nType: {biz_type.title()}\nStatus: {status.title()}\nEstablished: {created_date.split(' ')[0]}",
            inline=False
        )
        
        embed.add_field(
            name="🔑 License Code",
            value=f"`{license_code}`",
            inline=False
        )
        
        embed.add_field(
            name="📋 Usage",
            value="This license code can be used for:\n• Business banking applications\n• Official registrations\n• Tax documentation\n• Islamic finance compliance",
            inline=False
        )
        
        embed.set_footer(text="⚠️ Keep this license code secure and confidential")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving license information.")
        print(f"License view error: {e}")


@bot.tree.command(name="generate_license", description="Generate a license code for an existing business")
async def generate_license(interaction: discord.Interaction, business_name: str):
    """Generate a license code for an existing business that doesn't have one"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if user owns the business and it's active
        cursor.execute('''
            SELECT id, business_name, business_type, license_code, status
            FROM businesses WHERE user_id = ? AND business_name = ? AND status = 'active'
        ''', (str(interaction.user.id), business_name))
        
        business = cursor.fetchone()
        
        if not business:
            await interaction.response.send_message(f"❌ Active business '{business_name}' not found. Use `/my_businesses` to see your businesses.")
            conn.close()
            return
        
        business_id, name, biz_type, existing_license, status = business
        
        if existing_license:
            await interaction.response.send_message(
                f"❌ Business '{business_name}' already has a license code.\n"
                f"Use `/view_license {business_name}` to see it."
            )
            conn.close()
            return
        
        # Generate unique license code with retry logic
        license_code = None  # Initialize to prevent unbound variable
        max_update_attempts = 5
        for attempt in range(max_update_attempts):
            try:
                license_code = generate_unique_license_code()
                
                # Update the business with the new license code
                cursor.execute('''
                    UPDATE businesses SET license_code = ? WHERE id = ?
                ''', (license_code, business_id))
                break  # Success, exit retry loop
            except sqlite3.IntegrityError:
                # License code collision, retry
                if attempt == max_update_attempts - 1:
                    raise Exception("Unable to generate unique license code after multiple attempts")
                continue
        
        # Ensure license_code was generated successfully
        if not license_code:
            raise Exception("Failed to generate license code")
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🔐 License Generated!",
            description=f"License code generated for '{business_name}'",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💼 Business Details",
            value=f"Name: {name}\nType: {biz_type.title()}\nStatus: {status.title()}",
            inline=False
        )
        
        embed.add_field(
            name="🔑 New License Code",
            value=f"`{license_code}`",
            inline=False
        )
        
        embed.add_field(
            name="📋 Next Steps",
            value="• Use this code for business banking\n• Keep it secure and confidential\n• Use `/view_license` to see it anytime",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error generating license code.")
        print(f"License generation error: {e}")


# === ENHANCED HALAL EARNING METHODS ===

@bot.tree.command(name="community_service", description="Perform community service to earn rewards")
async def community_service(interaction: discord.Interaction, service_type: str, hours: float):
    """Perform various types of community service for halal earnings"""
    
    service_types = {
        'teaching': {'rate': 3.0, 'description': 'Teaching children or adults valuable skills'},
        'cleaning_mosque': {'rate': 2.5, 'description': 'Helping clean and maintain the local mosque'},
        'elder_care': {'rate': 3.5, 'description': 'Assisting elderly community members'},
        'food_distribution': {'rate': 2.0, 'description': 'Distributing food to those in need'},
        'tutoring': {'rate': 4.0, 'description': 'Providing educational tutoring services'},
        'mosque_maintenance': {'rate': 2.5, 'description': 'General maintenance work at religious facilities'},
        'community_garden': {'rate': 2.0, 'description': 'Working in community gardens and food production'},
        'disaster_relief': {'rate': 5.0, 'description': 'Helping during community emergencies'}
    }
    
    if service_type.lower() not in service_types:
        await interaction.response.send_message(
            f"❌ Service type '{service_type}' not available.\n"
            f"Available services: {', '.join(service_types.keys())}"
        )
        return
    
    if hours <= 0 or hours > 8:
        await interaction.response.send_message("❌ Hours must be between 0.1 and 8 per day")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check if already done community service today
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        cursor.execute('''
            SELECT SUM(hours_contributed) FROM charity_work 
            WHERE user_id = ? AND completion_date = ?
        ''', (str(interaction.user.id), today))
        
        daily_hours = cursor.fetchone()[0] or 0
        if daily_hours + hours > 8:
            await interaction.response.send_message(f"❌ Daily limit exceeded. You can do {8 - daily_hours:.1f} more hours today")
            conn.close()
            return
        
        service_info = service_types[service_type.lower()]
        reward = hours * service_info['rate']
        
        # Record charity work
        cursor.execute('''
            INSERT INTO charity_work (user_id, work_type, hours_contributed, reward_amount, currency)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), service_type.lower(), hours, reward, 'gold_dinars'))
        
        # Add reward to user account
        cursor.execute('UPDATE users SET gold_dinars = gold_dinars + ? WHERE user_id = ?', 
                      (reward, str(interaction.user.id)))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'community_service', reward, 'gold_dinars', f'{hours} hours of {service_type} community service'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🤝 Community Service Completed!",
            description="May Allah reward your service to the community",
            color=0x00AA00
        )
        
        embed.add_field(
            name="📋 Service Details",
            value=f"Service: {service_type.replace('_', ' ').title()}\nHours: {hours}\nRate: ₯{service_info['rate']:.2f}/hour\nReward: ₯{reward:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="📝 Description",
            value=service_info['description'],
            inline=False
        )
        
        embed.add_field(
            name="🌟 Islamic Principle",
            value="\"The best of people are those who benefit others\" - Prophet Muhammad (peace be upon him)",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error recording community service. Please try again later.")
        print(f"Community service error: {e}")

@bot.tree.command(name="recite_quran", description="Earn rewards for Quran recitation and memorization")
async def recite_quran(interaction: discord.Interaction, activity_type: str, amount: int):
    """Earn rewards for various Quran-related activities"""
    
    quran_activities = {
        'verses_recited': {'rate': 0.5, 'description': 'Reciting verses from the Holy Quran', 'max_daily': 100},
        'verses_memorized': {'rate': 2.0, 'description': 'Memorizing new verses from the Holy Quran', 'max_daily': 20},
        'surahs_completed': {'rate': 10.0, 'description': 'Completing full chapters of the Quran', 'max_daily': 5},
        'quran_study_hours': {'rate': 5.0, 'description': 'Studying Quran translation and interpretation', 'max_daily': 4}
    }
    
    if activity_type.lower() not in quran_activities:
        await interaction.response.send_message(
            f"❌ Activity type '{activity_type}' not available.\n"
            f"Available activities: {', '.join(quran_activities.keys())}"
        )
        return
    
    activity_info = quran_activities[activity_type.lower()]
    
    if amount <= 0 or amount > activity_info['max_daily']:
        await interaction.response.send_message(f"❌ Amount must be between 1 and {activity_info['max_daily']}")
        return
    
    try:
        import datetime
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check daily limits
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        cursor.execute('''
            SELECT SUM(reward_amount) FROM daily_tasks 
            WHERE user_id = ? AND task_type = ? AND completion_date = ?
        ''', (str(interaction.user.id), f'quran_{activity_type}', today))
        
        daily_activity = cursor.fetchone()[0] or 0
        max_reward = activity_info['max_daily'] * activity_info['rate']
        
        if daily_activity >= max_reward:
            await interaction.response.send_message(f"❌ Daily limit reached for {activity_type}. Try again tomorrow!")
            conn.close()
            return
        
        reward = amount * activity_info['rate']
        if daily_activity + reward > max_reward:
            remaining = (max_reward - daily_activity) / activity_info['rate']
            await interaction.response.send_message(f"❌ You can only do {remaining:.0f} more {activity_type} today")
            conn.close()
            return
        
        # Record the activity
        cursor.execute('''
            INSERT INTO daily_tasks (user_id, task_type, completion_date, reward_amount, currency)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), f'quran_{activity_type}', today, reward, 'gold_dinars'))
        
        # Add reward to user account
        cursor.execute('UPDATE users SET gold_dinars = gold_dinars + ? WHERE user_id = ?', 
                      (reward, str(interaction.user.id)))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'quran_activity', reward, 'gold_dinars', f'{amount} {activity_type.replace("_", " ")}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="📖 Quran Activity Rewarded!",
            description="May Allah increase you in knowledge and righteousness",
            color=0x0066CC
        )
        
        embed.add_field(
            name="📋 Activity Details",
            value=f"Activity: {activity_type.replace('_', ' ').title()}\nAmount: {amount}\nReward: ₯{reward:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="📝 Description",
            value=activity_info['description'],
            inline=False
        )
        
        embed.add_field(
            name="🌟 Hadith",
            value="\"Whoever recites a letter from the Book of Allah, he will be credited with a good deed, and a good deed gets a ten-fold reward.\" - Prophet Muhammad (PBUH)",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error recording Quran activity. Please try again later.")
        print(f"Quran activity error: {e}")

@bot.tree.command(name="skill_development", description="Learn new skills and earn while doing so")
async def skill_development(interaction: discord.Interaction, skill_name: str, hours_studied: float):
    """Develop skills through study and practice while earning rewards"""
    
    halal_skills = {
        'arabic_language': {'rate': 3.0, 'description': 'Learning Arabic language and grammar'},
        'islamic_finance': {'rate': 4.0, 'description': 'Understanding Islamic banking and finance principles'},
        'programming': {'rate': 5.0, 'description': 'Learning computer programming and software development'},
        'business_management': {'rate': 3.5, 'description': 'Developing business and management skills'},
        'craftsmanship': {'rate': 3.0, 'description': 'Traditional crafts and artisan skills'},
        'agriculture': {'rate': 2.5, 'description': 'Modern and traditional farming techniques'},
        'medicine': {'rate': 6.0, 'description': 'Medical knowledge and healthcare skills'},
        'engineering': {'rate': 5.0, 'description': 'Engineering principles and applications'},
        'teaching': {'rate': 3.5, 'description': 'Educational methods and pedagogical skills'},
        'accounting': {'rate': 3.0, 'description': 'Halal accounting and bookkeeping practices'}
    }
    
    if skill_name.lower() not in halal_skills:
        await interaction.response.send_message(
            f"❌ Skill '{skill_name}' not available.\n"
            f"Available skills: {', '.join(halal_skills.keys())}"
        )
        return
    
    if hours_studied <= 0 or hours_studied > 6:
        await interaction.response.send_message("❌ Study hours must be between 0.1 and 6 per day")
        return
    
    try:
        import datetime
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check or create user skill record
        cursor.execute('''
            SELECT skill_level, experience_points FROM user_skills 
            WHERE user_id = ? AND skill_name = ?
        ''', (str(interaction.user.id), skill_name.lower()))
        
        skill_record = cursor.fetchone()
        
        if skill_record:
            current_level, current_xp = skill_record
        else:
            current_level, current_xp = 1, 0
            cursor.execute('''
                INSERT INTO user_skills (user_id, skill_name, skill_level, experience_points)
                VALUES (?, ?, ?, ?)
            ''', (str(interaction.user.id), skill_name.lower(), 1, 0))
        
        # Check daily limits
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        cursor.execute('''
            SELECT SUM(hours_contributed) FROM daily_tasks 
            WHERE user_id = ? AND task_type = ? AND completion_date = ?
        ''', (str(interaction.user.id), f'skill_{skill_name}', today))
        
        daily_hours = cursor.fetchone()[0] or 0
        if daily_hours + hours_studied > 6:
            await interaction.response.send_message(f"❌ You can only study {6 - daily_hours:.1f} more hours of {skill_name} today")
            conn.close()
            return
        
        skill_info = halal_skills[skill_name.lower()]
        base_reward = hours_studied * skill_info['rate']
        # Bonus based on current skill level
        level_bonus = current_level * 0.1
        total_reward = base_reward * (1 + level_bonus)
        
        # Calculate experience gain
        xp_gain = int(hours_studied * 10)
        new_xp = current_xp + xp_gain
        
        # Level up calculation (100 XP per level)
        new_level = (new_xp // 100) + 1
        level_up = new_level > current_level
        
        # Update skill record
        cursor.execute('''
            UPDATE user_skills SET skill_level = ?, experience_points = ? 
            WHERE user_id = ? AND skill_name = ?
        ''', (new_level, new_xp, str(interaction.user.id), skill_name.lower()))
        
        # Record the learning activity
        cursor.execute('''
            INSERT INTO daily_tasks (user_id, task_type, completion_date, reward_amount, currency)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), f'skill_{skill_name}', today, total_reward, 'gold_dinars'))
        
        # Add reward to user account
        cursor.execute('UPDATE users SET gold_dinars = gold_dinars + ? WHERE user_id = ?', 
                      (total_reward, str(interaction.user.id)))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'skill_development', total_reward, 'gold_dinars', f'{hours_studied} hours studying {skill_name}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🎓 Skill Development Rewarded!",
            description="Knowledge is the greatest investment",
            color=0x9932CC
        )
        
        embed.add_field(
            name="📋 Study Details",
            value=f"Skill: {skill_name.replace('_', ' ').title()}\nHours: {hours_studied}\nBase Rate: ₯{skill_info['rate']:.2f}/hour\nLevel Bonus: {level_bonus*100:.0f}%\nTotal Reward: ₯{total_reward:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="📈 Skill Progress",
            value=f"Level: {current_level} → {new_level}{'🌟 LEVEL UP!' if level_up else ''}\nExperience: {new_xp} XP (+{xp_gain})\nNext Level: {((new_level * 100) - new_xp)} XP needed",
            inline=False
        )
        
        embed.add_field(
            name="📝 Description",
            value=skill_info['description'],
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error recording skill development. Please try again later.")
        print(f"Skill development error: {e}")

@bot.tree.command(name="mentor_someone", description="Mentor others in your skills and earn rewards")
async def mentor_someone(interaction: discord.Interaction, student: discord.Member, skill: str, hours: float):
    """Mentor other users in skills you have developed"""
    try:
        if student.id == interaction.user.id:
            await interaction.response.send_message("❌ You cannot mentor yourself!")
            return
        
        if hours <= 0 or hours > 4:
            await interaction.response.send_message("❌ Mentoring hours must be between 0.1 and 4 per session")
            return
        
        import datetime
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check mentor's skill level
        cursor.execute('''
            SELECT skill_level FROM user_skills 
            WHERE user_id = ? AND skill_name = ?
        ''', (str(interaction.user.id), skill.lower()))
        
        mentor_skill = cursor.fetchone()
        if not mentor_skill or mentor_skill[0] < 3:
            await interaction.response.send_message("❌ You need at least level 3 in this skill to mentor others")
            conn.close()
            return
        
        mentor_level = mentor_skill[0]
        
        # Calculate rewards
        base_rate = 4.0  # Base rate for mentoring
        level_multiplier = mentor_level * 0.2
        mentor_reward = hours * base_rate * (1 + level_multiplier)
        
        # Give student some skill experience
        student_xp_gain = int(hours * 15)  # Students gain more XP when mentored
        
        # Check if student has the skill record
        cursor.execute('''
            SELECT skill_level, experience_points FROM user_skills 
            WHERE user_id = ? AND skill_name = ?
        ''', (str(student.id), skill.lower()))
        
        student_skill = cursor.fetchone()
        
        if student_skill:
            student_level, student_xp = student_skill
            new_student_xp = student_xp + student_xp_gain
            new_student_level = (new_student_xp // 100) + 1
            
            cursor.execute('''
                UPDATE user_skills SET skill_level = ?, experience_points = ? 
                WHERE user_id = ? AND skill_name = ?
            ''', (new_student_level, new_student_xp, str(student.id), skill.lower()))
        else:
            new_student_xp = student_xp_gain
            new_student_level = (new_student_xp // 100) + 1
            
            cursor.execute('''
                INSERT INTO user_skills (user_id, skill_name, skill_level, experience_points)
                VALUES (?, ?, ?, ?)
            ''', (str(student.id), skill.lower(), new_student_level, new_student_xp))
        
        # Record mentoring activity for mentor
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'mentoring', mentor_reward, 'gold_dinars', f'Mentored {student.display_name} in {skill} for {hours} hours', str(student.id)))
        
        # Record learning activity for student
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (str(student.id), 'mentoring_received', 0, 'gold_dinars', f'Received {hours} hours of {skill} mentoring from {interaction.user.display_name}', str(interaction.user.id)))
        
        # Pay mentor
        cursor.execute('UPDATE users SET gold_dinars = gold_dinars + ? WHERE user_id = ?', 
                      (mentor_reward, str(interaction.user.id)))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="👨‍🏫 Mentoring Session Completed!",
            description=f"You've successfully mentored {student.display_name}",
            color=0xFF6600
        )
        
        embed.add_field(
            name="📋 Session Details",
            value=f"Student: {student.display_name}\nSkill: {skill.replace('_', ' ').title()}\nHours: {hours}\nYour Level: {mentor_level}",
            inline=False
        )
        
        embed.add_field(
            name="💰 Your Reward",
            value=f"Base Rate: ₯{base_rate:.2f}/hour\nLevel Bonus: {level_multiplier*100:.0f}%\nTotal Earned: ₯{mentor_reward:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="🎓 Student Progress",
            value=f"{student.display_name} gained {student_xp_gain} XP in {skill.replace('_', ' ').title()}!",
            inline=False
        )
        
        embed.add_field(
            name="🌟 Islamic Teaching",
            value="\"The best of people are those who benefit others\" - Teaching is one of the most rewarded acts in Islam",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error recording mentoring session. Please try again later.")
        print(f"Mentoring error: {e}")

# All stock and share trading functionality has been removed for halal compliance
# Replaced with Islamic-compliant earning methods: community_service, recite_quran, skill_development, mentor_someone

# === UPDATED MY_PORTFOLIO COMMAND (STOCK REFERENCES REMOVED) ===

@bot.tree.command(name="my_portfolio", description="View your investments and loans")
async def my_portfolio(interaction: discord.Interaction):
    """View all your investments and loans (stocks removed for halal compliance)"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        user_id = str(interaction.user.id)
        
        # Get halal investments
        cursor.execute('''
            SELECT investment_type, SUM(amount), AVG(profit_sharing_ratio), status
            FROM investments
            WHERE user_id = ?
            GROUP BY investment_type, status
            ORDER BY status DESC, investment_type
        ''', (user_id,))
        investments = cursor.fetchall()
        
        # Get active loans given
        cursor.execute('''
            SELECT l.loan_amount, l.repaid_amount, l.currency, l.due_date, u.username
            FROM loans l
            JOIN users u ON l.borrower_id = u.user_id
            WHERE l.lender_id = ? AND l.status = 'active'
        ''', (user_id,))
        loans_given = cursor.fetchall()
        
        # Get active loans borrowed
        cursor.execute('''
            SELECT l.loan_amount, l.repaid_amount, l.currency, l.due_date, u.username, l.id
            FROM loans l
            JOIN users u ON l.lender_id = u.user_id
            WHERE l.borrower_id = ? AND l.status = 'active'
        ''', (user_id,))
        loans_borrowed = cursor.fetchall()
        
        # Get skills summary
        cursor.execute('''
            SELECT skill_name, skill_level, experience_points
            FROM user_skills
            WHERE user_id = ?
            ORDER BY skill_level DESC, experience_points DESC
            LIMIT 5
        ''', (user_id,))
        skills = cursor.fetchall()
        
        conn.close()
        
        embed = discord.Embed(
            title="📊 Your Halal Portfolio",
            description="Overview of all your Shariah-compliant investments and activities",
            color=0x0066CC
        )
        
        # Halal investments
        if investments:
            investment_text = ""
            for inv_type, amount, profit_ratio, status in investments:
                status_emoji = "✅" if status == "active" else "⏸️"
                investment_text += f"• **{inv_type.replace('_', ' ').title()}** {status_emoji}\n  Amount: ₯{amount:.2f}\n  Profit Share: {profit_ratio*100:.0f}%\n\n"
            
            embed.add_field(
                name="💰 Shariah-Compliant Investments",
                value=investment_text[:1024] if investment_text else "None",
                inline=False
            )
        
        # Top skills
        if skills:
            skills_text = ""
            for skill_name, level, xp in skills:
                skills_text += f"• **{skill_name.replace('_', ' ').title()}**: Level {level} ({xp} XP)\n"
            
            embed.add_field(
                name="🎓 Top Skills",
                value=skills_text[:1024] if skills_text else "None",
                inline=False
            )
        
        # Loans given
        if loans_given:
            loan_text = ""
            for amount, repaid, currency, due_date, borrower in loans_given:
                remaining = amount - repaid
                loan_text += f"• **{borrower}**: ₯{remaining:.2f} remaining\n  Due: {due_date}\n\n"
            
            embed.add_field(
                name="🤝 Qard Hassan (Interest-Free Loans Given)",
                value=loan_text[:1024] if loan_text else "None",
                inline=False
            )
        
        # Loans borrowed
        if loans_borrowed:
            borrowed_text = ""
            for amount, repaid, currency, due_date, lender, loan_id in loans_borrowed:
                remaining = amount - repaid
                borrowed_text += f"• **From {lender}**: ₯{remaining:.2f} remaining\n  Due: {due_date} (ID: {loan_id})\n\n"
            
            embed.add_field(
                name="📋 Loans Borrowed",
                value=borrowed_text[:1024] if borrowed_text else "None",
                inline=False
            )
        
        # If no activities
        if not investments and not loans_given and not loans_borrowed and not skills:
            embed.add_field(
                name="🚀 Get Started with Halal Earning",
                value="• Use `/invest` for Shariah-compliant investments\n• Use `/community_service` to help others\n• Use `/skill_development` to learn new skills\n• Use `/recite_quran` for spiritual rewards\n• Use `/browse_loan_applications` to help community members",
                inline=False
            )
        
        embed.add_field(
            name="🌟 Remember",
            value="All earning methods in this bot follow Islamic principles - no interest, no gambling, no prohibited activities",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving portfolio. Please try again later.")
        print(f"Portfolio error: {e}")

# All remaining stock trading functionality has been removed for halal compliance
        
# === LOAN APPLICATION SYSTEM (HALAL FINANCE) ===

# All stock trading functionality has been completely removed for halal compliance

@bot.tree.command(name="merge_business", description="Merge one of your businesses with another")
async def merge_business(interaction: discord.Interaction, primary_business: str, secondary_business: str):
    """Merge two of your businesses together"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check both businesses belong to user and are active
        cursor.execute('''
            SELECT id, business_type, startup_cost, daily_profit FROM businesses 
            WHERE user_id = ? AND business_name = ? AND status = 'active'
        ''', (str(interaction.user.id), primary_business))
        
        primary_biz = cursor.fetchone()
        if not primary_biz:
            await interaction.response.send_message(f"❌ Primary business '{primary_business}' not found or not active.")
            conn.close()
            return
        
        cursor.execute('''
            SELECT id, business_type, startup_cost, daily_profit FROM businesses 
            WHERE user_id = ? AND business_name = ? AND status = 'active'
        ''', (str(interaction.user.id), secondary_business))
        
        secondary_biz = cursor.fetchone()
        if not secondary_biz:
            await interaction.response.send_message(f"❌ Secondary business '{secondary_business}' not found or not active.")
            conn.close()
            return
        
        primary_id, primary_type, primary_startup, primary_profit = primary_biz
        secondary_id, secondary_type, secondary_startup, secondary_profit = secondary_biz
        
        # Calculate merger value and new business stats
        merger_value = secondary_startup * 0.75  # 75% of startup cost
        new_daily_profit = primary_profit + (secondary_profit * 0.8)  # 80% efficiency retained
        new_startup_cost = primary_startup + secondary_startup
        
        # Update primary business
        cursor.execute('''
            UPDATE businesses SET daily_profit = ?, startup_cost = ? WHERE id = ?
        ''', (new_daily_profit, new_startup_cost, primary_id))
        
        # Mark secondary business as merged
        cursor.execute('UPDATE businesses SET status = ? WHERE id = ?', ('merged', secondary_id))
        
        # Record merger
        cursor.execute('''
            INSERT INTO business_mergers (primary_business_id, merged_business_id, merger_value)
            VALUES (?, ?, ?)
        ''', (primary_id, secondary_id, merger_value))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'business_merger', merger_value, 'gold_dinars', f'Merged {secondary_business} into {primary_business}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🏢 Business Merger Completed!",
            description=f"Successfully merged {secondary_business} into {primary_business}",
            color=0x9932CC
        )
        
        embed.add_field(
            name="📋 Merger Details",
            value=f"Primary Business: {primary_business} ({primary_type})\nMerged Business: {secondary_business} ({secondary_type})\nMerger Value: ₯{merger_value:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="📈 New Business Stats",
            value=f"Combined Daily Profit: ₯{new_daily_profit:.2f}\nCombined Startup Value: ₯{new_startup_cost:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="💡 Benefits",
            value="• Increased daily profit from combined operations\n• Higher business value for future sales\n• Operational efficiency gains",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error merging businesses. Please try again later.")
        print(f"Business merger error: {e}")

# === NEW LOAN APPLICATION SYSTEM ===

@bot.tree.command(name="apply_for_loan", description="Apply for an interest-free loan from the community")
async def apply_for_loan(interaction: discord.Interaction, loan_amount: float, currency: str, repayment_days: int, purpose: str):
    """Apply for a Shariah-compliant interest-free loan"""
    
    if currency.lower() not in ['gold_dinars', 'silver_dirhams']:
        await interaction.response.send_message("❌ Currency must be 'gold_dinars' or 'silver_dirhams'")
        return
    
    if loan_amount <= 0 or repayment_days <= 0:
        await interaction.response.send_message("❌ Loan amount and repayment days must be positive")
        return
        
    if repayment_days > 365:
        await interaction.response.send_message("❌ Maximum loan term is 365 days")
        return
    
    if not purpose or len(purpose) < 10:
        await interaction.response.send_message("❌ Please provide a detailed purpose (minimum 10 characters)")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Check if user already has pending applications
        cursor.execute('''
            SELECT COUNT(*) FROM loan_applications 
            WHERE borrower_id = ? AND status = 'pending'
        ''', (str(interaction.user.id),))
        
        pending_count = cursor.fetchone()[0]
        if pending_count >= 3:
            await interaction.response.send_message("❌ You can only have 3 pending loan applications at a time.")
            conn.close()
            return
        
        # Calculate due date
        due_date = (datetime.datetime.now() + datetime.timedelta(days=repayment_days)).strftime('%Y-%m-%d')
        
        # Create loan application
        cursor.execute('''
            INSERT INTO loan_applications 
            (borrower_id, borrower_name, loan_amount, currency, repayment_days, due_date, purpose, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        ''', (str(interaction.user.id), interaction.user.display_name or interaction.user.name, 
              loan_amount, currency, repayment_days, due_date, purpose))
        
        application_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="📝 Loan Application Submitted!",
            description="Your loan application is now available for lenders to review",
            color=0x3498DB
        )
        
        embed.add_field(
            name="📋 Application Details",
            value=f"Application ID: {application_id}\nAmount: ₯{loan_amount:.2f} {currency.replace('_', ' ')}\nRepayment Term: {repayment_days} days\nDue Date: {due_date}",
            inline=False
        )
        
        embed.add_field(
            name="💡 Purpose",
            value=purpose[:200] + "..." if len(purpose) > 200 else purpose,
            inline=False
        )
        
        embed.add_field(
            name="⏳ What's Next?",
            value="Community members can review and fund your application using `/fund_loan`\nYou can check status with `/my_loan_status`",
            inline=False
        )
        
        embed.add_field(
            name="📋 Islamic Finance",
            value="This follows Qard Hassan principles - interest-free loans for community support",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error submitting loan application. Please try again later.")
        print(f"Loan application error: {e}")

@bot.tree.command(name="browse_loan_applications", description="Browse pending loan applications from community members")
async def browse_loan_applications(interaction: discord.Interaction):
    """View all pending loan applications that you can fund"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get pending applications (not from the current user)
        cursor.execute('''
            SELECT id, borrower_name, loan_amount, currency, repayment_days, purpose, application_date
            FROM loan_applications 
            WHERE status = 'pending' AND borrower_id != ?
            ORDER BY application_date DESC
            LIMIT 10
        ''', (str(interaction.user.id),))
        
        applications = cursor.fetchall()
        conn.close()
        
        if not applications:
            await interaction.response.send_message("📭 No pending loan applications available for funding at this time.")
            return
        
        embed = discord.Embed(
            title="📋 Available Loan Applications",
            description="Community members seeking Qard Hassan (interest-free loans)",
            color=0x9B59B6
        )
        
        for app_id, borrower_name, amount, currency, days, purpose, app_date in applications:
            # Truncate purpose for display
            short_purpose = purpose[:80] + "..." if len(purpose) > 80 else purpose
            
            try:
                date_obj = datetime.datetime.fromisoformat(app_date)
                formatted_date = date_obj.strftime("%m/%d")
            except:
                formatted_date = app_date[:5]
            
            embed.add_field(
                name=f"📌 Application #{app_id} - {borrower_name}",
                value=f"💰 Amount: ₯{amount:.2f} {currency.replace('_', ' ')}\n⏱️ Term: {days} days\n📝 Purpose: {short_purpose}\n📅 Applied: {formatted_date}",
                inline=False
            )
        
        embed.add_field(
            name="🤝 How to Help",
            value="Use `/fund_loan [application_id]` to fund any application\nAll loans are interest-free following Islamic principles",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving loan applications. Please try again later.")
        print(f"Browse applications error: {e}")

@bot.tree.command(name="fund_loan", description="Fund someone's loan application")
async def fund_loan(interaction: discord.Interaction, application_id: int):
    """Fund a community member's loan application"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Get application details
        cursor.execute('''
            SELECT borrower_id, borrower_name, loan_amount, currency, repayment_days, due_date, purpose
            FROM loan_applications 
            WHERE id = ? AND status = 'pending'
        ''', (application_id,))
        
        application = cursor.fetchone()
        if not application:
            await interaction.response.send_message("❌ Loan application not found or already funded.")
            conn.close()
            return
        
        borrower_id, borrower_name, loan_amount, currency, repayment_days, due_date, purpose = application
        
        if borrower_id == str(interaction.user.id):
            await interaction.response.send_message("❌ You cannot fund your own loan application!")
            conn.close()
            return
        
        # Check lender's funds
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        currency_balance = user_data['gold_dinars'] if currency == 'gold_dinars' else user_data['silver_dirhams']
        
        if currency_balance < loan_amount:
            await interaction.response.send_message(f"❌ Insufficient funds. Need ₯{loan_amount:.2f}, you have ₯{currency_balance:.2f} {currency.replace('_', ' ')}")
            conn.close()
            return
        
        # Begin transaction
        cursor.execute('BEGIN IMMEDIATE')
        
        # Update lender's balance
        if currency == 'gold_dinars':
            new_lender_balance = user_data['gold_dinars'] - loan_amount
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_lender_balance, str(interaction.user.id)))
        else:
            new_lender_balance = user_data['silver_dirhams'] - loan_amount
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_lender_balance, str(interaction.user.id)))
        
        # Update borrower's balance
        cursor.execute(f'UPDATE users SET {currency} = {currency} + ? WHERE user_id = ?', 
                     (loan_amount, borrower_id))
        
        # Create loan record
        cursor.execute('''
            INSERT INTO loans (lender_id, borrower_id, loan_amount, currency, due_date, status)
            VALUES (?, ?, ?, ?, ?, 'active')
        ''', (str(interaction.user.id), borrower_id, loan_amount, currency, due_date))
        
        loan_id = cursor.lastrowid
        
        # Update application status
        cursor.execute('UPDATE loan_applications SET status = ?, funded_by = ? WHERE id = ?', 
                     ('funded', str(interaction.user.id), application_id))
        
        # Record transactions
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'loan_given', loan_amount, currency, 
              f'Funded loan application #{application_id} for {borrower_name}', borrower_id))
        
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (borrower_id, 'loan_received', loan_amount, currency, 
              f'Received loan funding from {interaction.user.display_name or interaction.user.name}', str(interaction.user.id)))
        
        cursor.execute('COMMIT')
        conn.close()
        
        embed = discord.Embed(
            title="🤝 Loan Successfully Funded!",
            description=f"You've provided a Qard Hassan loan to {borrower_name}",
            color=0x27AE60
        )
        
        embed.add_field(
            name="💰 Loan Details",
            value=f"Loan ID: {loan_id}\nAmount: ₯{loan_amount:.2f} {currency.replace('_', ' ')}\nBorrower: {borrower_name}\nDue Date: {due_date}\nTerm: {repayment_days} days",
            inline=False
        )
        
        embed.add_field(
            name="📝 Purpose",
            value=purpose[:150] + "..." if len(purpose) > 150 else purpose,
            inline=False
        )
        
        embed.add_field(
            name="🌟 Islamic Finance Reward",
            value="May Allah reward your generosity! This interest-free loan follows Qard Hassan principles.",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error funding loan. Please try again later.")
        print(f"Loan funding error: {e}")

@bot.tree.command(name="repay_loan", description="Repay your active loan")
async def repay_loan(interaction: discord.Interaction, loan_id: int, repayment_amount: float):
    """Repay all or part of an active loan"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Get loan details
        cursor.execute('''
            SELECT lender_id, loan_amount, currency, repaid_amount, status, due_date
            FROM loans WHERE id = ? AND borrower_id = ?
        ''', (loan_id, str(interaction.user.id)))
        
        loan = cursor.fetchone()
        if not loan:
            await interaction.response.send_message("❌ Loan not found or you are not the borrower.")
            conn.close()
            return
        
        lender_id, loan_amount, currency, repaid_amount, status, due_date = loan
        
        if status != 'active':
            await interaction.response.send_message("❌ This loan is not active.")
            conn.close()
            return
        
        remaining_amount = loan_amount - repaid_amount
        if repayment_amount > remaining_amount:
            await interaction.response.send_message(f"❌ Repayment amount exceeds remaining balance of ₯{remaining_amount:.2f}")
            conn.close()
            return
        
        if repayment_amount <= 0:
            await interaction.response.send_message("❌ Repayment amount must be positive")
            conn.close()
            return
        
        # Check borrower's funds
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        currency_balance = user_data['gold_dinars'] if currency == 'gold_dinars' else user_data['silver_dirhams']
        
        if currency_balance < repayment_amount:
            await interaction.response.send_message(f"❌ Insufficient funds. You have ₯{currency_balance:.2f} {currency.replace('_', ' ')}")
            conn.close()
            return
        
        # Begin transaction
        cursor.execute('BEGIN IMMEDIATE')
        
        # Update borrower's balance
        if currency == 'gold_dinars':
            new_balance = user_data['gold_dinars'] - repayment_amount
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_balance, str(interaction.user.id)))
        else:
            new_balance = user_data['silver_dirhams'] - repayment_amount
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_balance, str(interaction.user.id)))
        
        # Update lender's balance
        cursor.execute(f'UPDATE users SET {currency} = {currency} + ? WHERE user_id = ?', 
                     (repayment_amount, lender_id))
        
        # Update loan record
        new_repaid_amount = repaid_amount + repayment_amount
        loan_status = 'completed' if new_repaid_amount >= loan_amount else 'active'
        
        cursor.execute('UPDATE loans SET repaid_amount = ?, status = ? WHERE id = ?', 
                     (new_repaid_amount, loan_status, loan_id))
        
        # Record transactions
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'loan_repayment', repayment_amount, currency, f'Loan repayment (ID: {loan_id})', lender_id))
        
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description, partner_id)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (lender_id, 'loan_repayment_received', repayment_amount, currency, f'Loan repayment received (ID: {loan_id})', str(interaction.user.id)))
        
        cursor.execute('COMMIT')
        conn.close()
        
        remaining_balance = loan_amount - new_repaid_amount
        
        embed = discord.Embed(
            title="✅ Loan Repayment Processed!",
            description="Your loan repayment has been successfully processed",
            color=0x27AE60
        )
        
        embed.add_field(
            name="💰 Payment Details",
            value=f"Loan ID: {loan_id}\nRepaid: ₯{repayment_amount:.2f} {currency.replace('_', ' ')}\nRemaining Balance: ₯{remaining_balance:.2f}\nStatus: {loan_status.title()}",
            inline=False
        )
        
        if loan_status == 'completed':
            embed.add_field(
                name="🎉 Loan Fully Repaid!",
                value="Congratulations! You have successfully repaid this loan in full.\nMay Allah reward your commitment and trustworthiness!",
                inline=False
            )
        else:
            embed.add_field(
                name="📅 Next Steps",
                value=f"Continue making payments until the loan is fully repaid by {due_date}",
                inline=False
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error processing loan repayment. Please try again later.")
        print(f"Loan repayment error: {e}")

@bot.tree.command(name="my_loan_status", description="Check status of your loans and applications")
async def my_loan_status(interaction: discord.Interaction):
    """Check your loan applications and active loans"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        user_id = str(interaction.user.id)
        
        # Get pending applications
        cursor.execute('''
            SELECT id, loan_amount, currency, repayment_days, purpose, application_date
            FROM loan_applications 
            WHERE borrower_id = ? AND status = 'pending'
            ORDER BY application_date DESC
        ''', (user_id,))
        
        pending_apps = cursor.fetchall()
        
        # Get active borrowed loans
        cursor.execute('''
            SELECT id, loan_amount, currency, repaid_amount, due_date
            FROM loans 
            WHERE borrower_id = ? AND status = 'active'
            ORDER BY due_date ASC
        ''', (user_id,))
        
        active_loans = cursor.fetchall()
        
        # Get active lent loans
        cursor.execute('''
            SELECT l.id, l.loan_amount, l.currency, l.repaid_amount, l.due_date, u.username
            FROM loans l
            JOIN users u ON l.borrower_id = u.user_id
            WHERE l.lender_id = ? AND l.status = 'active'
            ORDER BY l.due_date ASC
        ''', (user_id,))
        
        lent_loans = cursor.fetchall()
        
        conn.close()
        
        embed = discord.Embed(
            title="📊 Your Loan Status",
            description="Overview of your loan applications and active loans",
            color=0x3498DB
        )
        
        # Pending applications
        if pending_apps:
            apps_text = ""
            for app_id, amount, currency, days, purpose, app_date in pending_apps:
                short_purpose = purpose[:30] + "..." if len(purpose) > 30 else purpose
                try:
                    date_obj = datetime.datetime.fromisoformat(app_date)
                    formatted_date = date_obj.strftime("%m/%d")
                except:
                    formatted_date = app_date[:5]
                apps_text += f"#{app_id}: ₯{amount:.2f} {currency.replace('_', ' ')} ({days}d) - {short_purpose} ({formatted_date})\n"
            
            embed.add_field(
                name="⏳ Pending Applications",
                value=apps_text[:1024],
                inline=False
            )
        
        # Active borrowed loans
        if active_loans:
            loans_text = ""
            for loan_id, amount, currency, repaid, due_date in active_loans:
                remaining = amount - repaid
                loans_text += f"ID {loan_id}: ₯{remaining:.2f} remaining of ₯{amount:.2f} {currency.replace('_', ' ')} (due: {due_date[:10]})\n"
            
            embed.add_field(
                name="💳 Your Active Loans",
                value=loans_text[:1024],
                inline=False
            )
        
        # Active lent loans
        if lent_loans:
            lent_text = ""
            for loan_id, amount, currency, repaid, due_date, borrower in lent_loans:
                remaining = amount - repaid
                lent_text += f"ID {loan_id}: ₯{remaining:.2f} owed by {borrower} (due: {due_date[:10]})\n"
            
            embed.add_field(
                name="🤝 Loans You've Given",
                value=lent_text[:1024],
                inline=False
            )
        
        if not any([pending_apps, active_loans, lent_loans]):
            embed.add_field(
                name="📭 No Active Loans",
                value="You have no pending applications or active loans.\nUse `/apply_for_loan` to request funding or `/browse_loan_applications` to help others.",
                inline=False
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving loan status. Please try again later.")
        print(f"Loan status error: {e}")

# Gift shares command removed for halal compliance
# Use halal earning methods instead: /community_service, /recite_quran, /skill_development, /mentor_someone

# All business share gifting functionality removed for halal compliance
# Use the new halal earning methods and charity systems instead

# All share gifting functionality has been completely removed for halal compliance
# Use the halal earning methods: community_service, recite_quran, skill_development, mentor_someone

@bot.tree.command(name="leaderboard", description="View community leaderboard")
async def leaderboard(interaction: discord.Interaction):
    """View community leaderboard based on halal earnings and contributions"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        embed = discord.Embed(
            title="🏆 Islamic Economy Leaderboard",
            description="Top contributors in our halal economy",
            color=0xFFD700
        )
        
        # All stock gifting functionality removed for halal compliance
        embed.add_field(
            name="🚧 Under Development",
            value="Halal leaderboard system coming soon!\nWill rank based on charity, skill development, and community service.",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
        conn.close()
        
    except Exception as e:
        await interaction.response.send_message("❌ Error loading leaderboard. Please try again later.")
        print(f"Leaderboard error: {e}")

# All remaining stock gifting functionality has been completely removed for halal compliance

# === ISLAMIC BANKING SYSTEM ===

def generate_account_number() -> str:
    """Generate a unique bank account number"""
    # Format: IBK-XXXXXXXX (Islamic Bank + 8 random characters)
    chars = string.ascii_uppercase + string.digits
    account_suffix = ''.join(secrets.choice(chars) for _ in range(8))
    return f"IBK-{account_suffix}"

def generate_unique_account_number() -> str:
    """Generate a unique bank account number that doesn't exist in database"""
    max_attempts = 50
    for _ in range(max_attempts):
        account_number = generate_account_number()
        if is_account_number_unique(account_number):
            return account_number
    raise Exception("Unable to generate unique account number after 50 attempts")

def is_account_number_unique(account_number: str) -> bool:
    """Check if account number is unique in database"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM bank_accounts WHERE account_number = ?', (account_number,))
        count = cursor.fetchone()[0]
        conn.close()
        return count == 0
    except:
        return False

def get_finance_businesses():
    """Get all active Islamic finance businesses that can offer banking services"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT b.id, b.user_id, b.business_name, b.license_code, u.username
            FROM businesses b
            JOIN users u ON b.user_id = u.user_id
            WHERE b.business_type = 'islamic_finance' 
            AND b.status = 'active' 
            AND b.license_code IS NOT NULL
            ORDER BY b.business_name
        ''')
        
        businesses = cursor.fetchall()
        conn.close()
        return businesses
        
    except Exception as e:
        print(f"Error getting finance businesses: {e}")
        return []

def can_user_manage_institution(user_id: str, business_id: int) -> bool:
    """Check if user can manage a banking institution (owner or staff)"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if user owns the business
        cursor.execute('SELECT user_id FROM businesses WHERE id = ?', (business_id,))
        business = cursor.fetchone()
        if business and business[0] == user_id:
            conn.close()
            return True
        
        # Check if user is employed as banking staff
        cursor.execute('''
            SELECT COUNT(*) FROM user_employment 
            WHERE user_id = ? AND business_id = ? 
            AND status = 'active'
            AND job_title IN ('banker', 'manager', 'finance_manager')
        ''', (user_id, business_id))
        
        is_staff = cursor.fetchone()[0] > 0
        conn.close()
        return is_staff
        
    except Exception as e:
        print(f"Error checking institution management rights: {e}")
        return False

def get_user_account_count(user_id: str, institution_id: int) -> int:
    """Get number of accounts a user has at a specific institution"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT COUNT(*) FROM bank_accounts 
            WHERE owner_user_id = ? AND institution_business_id = ? AND status = 'active'
        ''', (user_id, institution_id))
        
        count = cursor.fetchone()[0]
        conn.close()
        return count
        
    except Exception as e:
        print(f"Error getting user account count: {e}")
        return 0

async def create_bank_account(owner_user_id: str, institution_business_id: int, account_type: str, 
                            currency: str, profit_share_ratio: float = None, created_by_user_id: str = None) -> dict:
    """Create a new bank account with Islamic finance compliance"""
    try:
        # Validate account type
        if account_type not in ['wadiah', 'mudarabah']:
            return {'success': False, 'error': 'Invalid account type. Must be wadiah or mudarabah.'}
        
        # Validate currency
        if currency not in ['gold_dinars', 'silver_dirhams']:
            return {'success': False, 'error': 'Invalid currency. Must be gold_dinars or silver_dirhams.'}
        
        # Validate profit share ratio for Mudarabah accounts
        if account_type == 'mudarabah':
            if profit_share_ratio is None or profit_share_ratio < 0 or profit_share_ratio > 1:
                return {'success': False, 'error': 'Mudarabah accounts require profit_share_ratio between 0 and 1.'}
        elif profit_share_ratio is not None:
            return {'success': False, 'error': 'Wadiah accounts cannot have profit sharing.'}
        
        # Check account limit per institution (max 5 accounts per user per institution)
        account_count = get_user_account_count(owner_user_id, institution_business_id)
        if account_count >= 5:
            return {'success': False, 'error': 'Maximum 5 accounts per institution reached.'}
        
        # Verify institution exists and is active Islamic finance business
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT business_name, user_id FROM businesses 
            WHERE id = ? AND business_type = 'islamic_finance' 
            AND status = 'active' AND license_code IS NOT NULL
        ''', (institution_business_id,))
        
        institution = cursor.fetchone()
        if not institution:
            conn.close()
            return {'success': False, 'error': 'Institution not found or not a valid Islamic finance business.'}
        
        business_name, institution_owner_id = institution
        
        # Generate unique account number
        account_number = generate_unique_account_number()
        
        # Create account
        cursor.execute('''
            INSERT INTO bank_accounts 
            (account_number, institution_business_id, owner_user_id, account_type, currency, profit_share_ratio, status)
            VALUES (?, ?, ?, ?, ?, ?, 'active')
        ''', (account_number, institution_business_id, owner_user_id, account_type, currency, profit_share_ratio))
        
        account_id = cursor.lastrowid
        
        # Create owner permission
        cursor.execute('''
            INSERT INTO bank_account_permissions (account_id, user_id, role)
            VALUES (?, ?, 'owner')
        ''', (account_id, owner_user_id))
        
        # Log account creation
        created_by = created_by_user_id or owner_user_id
        cursor.execute('''
            INSERT INTO bank_ledger 
            (account_id, entry_type, amount, currency, description, created_by_user_id)
            VALUES (?, 'deposit', 0, ?, 'Account opened', ?)
        ''', (account_id, currency, created_by))
        
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'account_id': account_id,
            'account_number': account_number,
            'institution_name': business_name,
            'account_type': account_type,
            'currency': currency,
            'profit_share_ratio': profit_share_ratio
        }
        
    except Exception as e:
        try:
            conn.close()
        except:
            pass
        print(f"Error creating bank account: {e}")
        return {'success': False, 'error': 'Database error occurred during account creation.'}

async def bank_deposit(account_id: int, user_id: str, amount: float) -> dict:
    """Deposit money from user wallet to bank account"""
    try:
        if amount <= 0:
            return {'success': False, 'error': 'Deposit amount must be positive.'}
        
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Get account details and verify ownership
        cursor.execute('''
            SELECT ba.account_number, ba.currency, ba.balance, ba.status, ba.owner_user_id,
                   b.business_name
            FROM bank_accounts ba
            JOIN businesses b ON ba.institution_business_id = b.id
            WHERE ba.id = ? AND ba.status = 'active'
        ''', (account_id,))
        
        account = cursor.fetchone()
        if not account:
            conn.close()
            return {'success': False, 'error': 'Account not found or inactive.'}
        
        account_number, currency, current_balance, status, owner_id, institution_name = account
        
        # Check if user owns account or has permission
        if owner_id != user_id:
            cursor.execute('''
                SELECT role FROM bank_account_permissions 
                WHERE account_id = ? AND user_id = ? AND role IN ('owner', 'joint_owner', 'manager')
            ''', (account_id, user_id))
            
            permission = cursor.fetchone()
            if not permission:
                conn.close()
                return {'success': False, 'error': 'You do not have permission to deposit to this account.'}
        
        # Get user data and check wallet balance
        user_data = get_user_account(user_id, '')
        wallet_balance = user_data['gold_dinars'] if currency == 'gold_dinars' else user_data['silver_dirhams']
        
        if wallet_balance < amount:
            conn.close()
            return {'success': False, 'error': f'Insufficient wallet balance. You have ₯{wallet_balance:.2f} {currency.replace("_", " ")}.'}
        
        # Begin transaction
        cursor.execute('BEGIN IMMEDIATE')
        
        # Update user wallet
        if currency == 'gold_dinars':
            new_wallet_balance = user_data['gold_dinars'] - amount
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', (new_wallet_balance, user_id))
        else:
            new_wallet_balance = user_data['silver_dirhams'] - amount
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', (new_wallet_balance, user_id))
        
        # Update bank account balance
        new_account_balance = current_balance + amount
        cursor.execute('UPDATE bank_accounts SET balance = ? WHERE id = ?', (new_account_balance, account_id))
        
        # Record in bank ledger
        cursor.execute('''
            INSERT INTO bank_ledger 
            (account_id, entry_type, amount, currency, description, created_by_user_id)
            VALUES (?, 'deposit', ?, ?, ?, ?)
        ''', (account_id, amount, currency, f'Deposit from wallet', user_id))
        
        # Record in user transactions
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, 'bank_deposit', amount, currency, f'Deposit to bank account {account_number}'))
        
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'account_number': account_number,
            'institution_name': institution_name,
            'amount': amount,
            'currency': currency,
            'new_account_balance': new_account_balance,
            'new_wallet_balance': new_wallet_balance
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        print(f"Error processing bank deposit: {e}")
        return {'success': False, 'error': 'Database error occurred during deposit.'}

async def bank_withdraw(account_id: int, user_id: str, amount: float) -> dict:
    """Withdraw money from bank account to user wallet"""
    try:
        if amount <= 0:
            return {'success': False, 'error': 'Withdrawal amount must be positive.'}
        
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Get account details and verify ownership
        cursor.execute('''
            SELECT ba.account_number, ba.currency, ba.balance, ba.status, ba.owner_user_id,
                   b.business_name
            FROM bank_accounts ba
            JOIN businesses b ON ba.institution_business_id = b.id
            WHERE ba.id = ? AND ba.status = 'active'
        ''', (account_id,))
        
        account = cursor.fetchone()
        if not account:
            conn.close()
            return {'success': False, 'error': 'Account not found or inactive.'}
        
        account_number, currency, current_balance, status, owner_id, institution_name = account
        
        # Check if user owns account or has permission
        if owner_id != user_id:
            cursor.execute('''
                SELECT role FROM bank_account_permissions 
                WHERE account_id = ? AND user_id = ? AND role IN ('owner', 'joint_owner', 'manager')
            ''', (account_id, user_id))
            
            permission = cursor.fetchone()
            if not permission:
                conn.close()
                return {'success': False, 'error': 'You do not have permission to withdraw from this account.'}
        
        # Check if account has sufficient balance
        if current_balance < amount:
            conn.close()
            return {'success': False, 'error': f'Insufficient account balance. Account has ₯{current_balance:.2f}.'}
        
        # Get user data
        user_data = get_user_account(user_id, '')
        
        # Begin transaction
        cursor.execute('BEGIN IMMEDIATE')
        
        # Update bank account balance
        new_account_balance = current_balance - amount
        cursor.execute('UPDATE bank_accounts SET balance = ? WHERE id = ?', (new_account_balance, account_id))
        
        # Update user wallet
        if currency == 'gold_dinars':
            new_wallet_balance = user_data['gold_dinars'] + amount
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', (new_wallet_balance, user_id))
        else:
            new_wallet_balance = user_data['silver_dirhams'] + amount
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', (new_wallet_balance, user_id))
        
        # Record in bank ledger
        cursor.execute('''
            INSERT INTO bank_ledger 
            (account_id, entry_type, amount, currency, description, created_by_user_id)
            VALUES (?, 'withdrawal', ?, ?, ?, ?)
        ''', (account_id, amount, currency, f'Withdrawal to wallet', user_id))
        
        # Record in user transactions
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, 'bank_withdrawal', amount, currency, f'Withdrawal from bank account {account_number}'))
        
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'account_number': account_number,
            'institution_name': institution_name,
            'amount': amount,
            'currency': currency,
            'new_account_balance': new_account_balance,
            'new_wallet_balance': new_wallet_balance
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        print(f"Error processing bank withdrawal: {e}")
        return {'success': False, 'error': 'Database error occurred during withdrawal.'}

async def bank_transfer(from_account_id: int, to_account_id: int, user_id: str, amount: float, description: str = None) -> dict:
    """Transfer money between bank accounts"""
    try:
        if amount <= 0:
            return {'success': False, 'error': 'Transfer amount must be positive.'}
        
        if from_account_id == to_account_id:
            return {'success': False, 'error': 'Cannot transfer to the same account.'}
        
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Get both account details
        cursor.execute('''
            SELECT ba.id, ba.account_number, ba.currency, ba.balance, ba.status, ba.owner_user_id,
                   b.business_name
            FROM bank_accounts ba
            JOIN businesses b ON ba.institution_business_id = b.id
            WHERE ba.id IN (?, ?) AND ba.status = 'active'
        ''', (from_account_id, to_account_id))
        
        accounts = cursor.fetchall()
        if len(accounts) != 2:
            conn.close()
            return {'success': False, 'error': 'One or both accounts not found or inactive.'}
        
        # Separate from and to account data
        from_account = next((acc for acc in accounts if acc[0] == from_account_id), None)
        to_account = next((acc for acc in accounts if acc[0] == to_account_id), None)
        
        if not from_account or not to_account:
            conn.close()
            return {'success': False, 'error': 'Account configuration error.'}
        
        from_id, from_number, from_currency, from_balance, from_status, from_owner_id, from_institution = from_account
        to_id, to_number, to_currency, to_balance, to_status, to_owner_id, to_institution = to_account
        
        # Check currency compatibility
        if from_currency != to_currency:
            conn.close()
            return {'success': False, 'error': 'Cannot transfer between different currencies.'}
        
        # Check if user has permission to transfer from source account
        if from_owner_id != user_id:
            cursor.execute('''
                SELECT role FROM bank_account_permissions 
                WHERE account_id = ? AND user_id = ? AND role IN ('owner', 'joint_owner', 'manager')
            ''', (from_account_id, user_id))
            
            permission = cursor.fetchone()
            if not permission:
                conn.close()
                return {'success': False, 'error': 'You do not have permission to transfer from the source account.'}
        
        # Check if source account has sufficient balance
        if from_balance < amount:
            conn.close()
            return {'success': False, 'error': f'Insufficient balance in source account. Account has ₯{from_balance:.2f}.'}
        
        # Begin transaction
        cursor.execute('BEGIN IMMEDIATE')
        
        # Update account balances
        new_from_balance = from_balance - amount
        new_to_balance = to_balance + amount
        
        cursor.execute('UPDATE bank_accounts SET balance = ? WHERE id = ?', (new_from_balance, from_account_id))
        cursor.execute('UPDATE bank_accounts SET balance = ? WHERE id = ?', (new_to_balance, to_account_id))
        
        # Create transfer description
        if not description:
            description = f'Transfer to {to_number}'
        
        # Record in bank ledger for both accounts
        cursor.execute('''
            INSERT INTO bank_ledger 
            (account_id, entry_type, amount, currency, description, counterparty_account_id, created_by_user_id)
            VALUES (?, 'transfer_out', ?, ?, ?, ?, ?)
        ''', (from_account_id, amount, from_currency, description, to_account_id, user_id))
        
        cursor.execute('''
            INSERT INTO bank_ledger 
            (account_id, entry_type, amount, currency, description, counterparty_account_id, created_by_user_id)
            VALUES (?, 'transfer_in', ?, ?, ?, ?, ?)
        ''', (to_account_id, amount, to_currency, f'Transfer from {from_number}', from_account_id, user_id))
        
        # Record in user transactions
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, 'bank_transfer', amount, from_currency, f'Transfer from {from_number} to {to_number}'))
        
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'from_account': from_number,
            'to_account': to_number,
            'from_institution': from_institution,
            'to_institution': to_institution,
            'amount': amount,
            'currency': from_currency,
            'new_from_balance': new_from_balance,
            'new_to_balance': new_to_balance
        }
        
    except Exception as e:
        try:
            conn.rollback()
            conn.close()
        except:
            pass
        print(f"Error processing bank transfer: {e}")
        return {'success': False, 'error': 'Database error occurred during transfer.'}

def get_user_bank_accounts(user_id: str) -> list:
    """Get all bank accounts for a user"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ba.id, ba.account_number, ba.account_type, ba.currency, ba.balance, 
                   ba.profit_share_ratio, ba.created_at, b.business_name, b.license_code
            FROM bank_accounts ba
            JOIN businesses b ON ba.institution_business_id = b.id
            WHERE ba.owner_user_id = ? AND ba.status = 'active'
            ORDER BY ba.created_at DESC
        ''', (user_id,))
        
        accounts = cursor.fetchall()
        conn.close()
        return accounts
        
    except Exception as e:
        print(f"Error getting user bank accounts: {e}")
        return []

# === ISLAMIC BANKING COMMANDS ===

@bot.tree.command(name="bank_open_account", description="Open a bank account at an Islamic finance business")
async def bank_open_account(interaction: discord.Interaction, business_license: str, account_type: str, currency: str, profit_share_ratio: float = None):
    """Open a new bank account at an Islamic finance business"""
    try:
        # Validate account type
        if account_type not in ['wadiah', 'mudarabah']:
            await interaction.response.send_message("❌ Invalid account type. Choose 'wadiah' (safekeeping) or 'mudarabah' (profit-sharing).", ephemeral=True)
            return
        
        # Validate currency
        if currency not in ['gold_dinars', 'silver_dirhams']:
            await interaction.response.send_message("❌ Invalid currency. Choose 'gold_dinars' or 'silver_dirhams'.", ephemeral=True)
            return
        
        # Find the business by license code
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, business_name, user_id FROM businesses 
            WHERE license_code = ? AND business_type = 'islamic_finance' 
            AND status = 'active'
        ''', (business_license,))
        
        business = cursor.fetchone()
        conn.close()
        
        if not business:
            await interaction.response.send_message("❌ Business not found. Please check the license code and ensure it's an active Islamic finance business.", ephemeral=True)
            return
        
        business_id, business_name, owner_id = business
        
        # Create the account
        result = await create_bank_account(
            str(interaction.user.id),
            business_id,
            account_type,
            currency,
            profit_share_ratio,
            str(interaction.user.id)
        )
        
        if not result['success']:
            await interaction.response.send_message(f"❌ {result['error']}", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="🏦 Bank Account Opened!",
            description=f"Successfully opened account at {result['institution_name']}",
            color=0x00AA00
        )
        
        account_type_desc = "Wadiah (Safekeeping)" if account_type == 'wadiah' else "Mudarabah (Profit-Sharing)"
        
        embed.add_field(
            name="🔢 Account Details",
            value=f"Account Number: `{result['account_number']}`\nType: {account_type_desc}\nCurrency: {currency.replace('_', ' ').title()}",
            inline=False
        )
        
        if account_type == 'mudarabah' and profit_share_ratio:
            embed.add_field(
                name="📈 Profit Sharing",
                value=f"Your share of profits: {profit_share_ratio*100:.1f}%\nComplies with Islamic Mudarabah principles",
                inline=False
            )
        
        embed.add_field(
            name="🕌 Islamic Compliance",
            value="• No interest (Riba) - fully Shariah compliant\n• Transparent profit sharing (if Mudarabah)\n• Ethical investment principles",
            inline=False
        )
        
        embed.add_field(
            name="💡 Next Steps",
            value="Use `/bank_deposit` to add funds\nUse `/bank_my_accounts` to view all accounts\nUse `/bank_transfer` to move money between accounts",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error opening bank account. Please try again later.", ephemeral=True)
        print(f"Bank account opening error: {e}")

@bot.tree.command(name="bank_my_accounts", description="View all your bank accounts")
async def bank_my_accounts(interaction: discord.Interaction):
    """View all user's bank accounts"""
    try:
        accounts = get_user_bank_accounts(str(interaction.user.id))
        
        if not accounts:
            embed = discord.Embed(
                title="🏦 Your Bank Accounts",
                description="You don't have any bank accounts yet.",
                color=0x3498DB
            )
            
            # Get available Islamic finance businesses
            finance_businesses = get_finance_businesses()
            if finance_businesses:
                businesses_text = ""
                for business_id, owner_id, business_name, license_code, owner_name in finance_businesses[:5]:
                    businesses_text += f"• **{business_name}** (License: `{license_code}`)\n"
                
                embed.add_field(
                    name="🏢 Available Islamic Banks",
                    value=businesses_text + "\nUse `/bank_open_account` to open an account",
                    inline=False
                )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        embed = discord.Embed(
            title="🏦 Your Bank Accounts",
            description="Overview of all your Islamic bank accounts",
            color=0x00AA00
        )
        
        total_gold = 0.0
        total_silver = 0.0
        
        for account_id, account_number, account_type, currency, balance, profit_ratio, created_at, institution_name, license_code in accounts:
            if currency == 'gold_dinars':
                total_gold += balance
            else:
                total_silver += balance
            
            account_type_display = "Wadiah (Safekeeping)" if account_type == 'wadiah' else "Mudarabah (Profit-Sharing)"
            
            profit_text = ""
            if account_type == 'mudarabah' and profit_ratio:
                profit_text = f"\nProfit Share: {profit_ratio*100:.1f}%"
            
            embed.add_field(
                name=f"🏛️ {institution_name}",
                value=f"Account: `{account_number}`\nType: {account_type_display}\nBalance: ₯{balance:.2f} {currency.replace('_', ' ')}{profit_text}\nOpened: {created_at.split(' ')[0]}",
                inline=True
            )
        
        embed.add_field(
            name="💰 Total Bank Holdings",
            value=f"Gold Dinars: ₯{total_gold:.2f}\nSilver Dirhams: ₯{total_silver:.2f}",
            inline=False
        )
        
        embed.set_footer(text="All accounts follow Islamic banking principles • No interest • Profit sharing where applicable")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving bank accounts. Please try again later.", ephemeral=True)
        print(f"Bank accounts retrieval error: {e}")

@bot.tree.command(name="bank_deposit", description="Deposit money from your wallet to a bank account")
async def bank_deposit_cmd(interaction: discord.Interaction, account_number: str, amount: float):
    """Deposit money from wallet to bank account"""
    try:
        # Find account by account number
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ba.id, ba.owner_user_id, ba.currency, b.business_name
            FROM bank_accounts ba
            JOIN businesses b ON ba.institution_business_id = b.id
            WHERE ba.account_number = ? AND ba.status = 'active'
        ''', (account_number,))
        
        account = cursor.fetchone()
        conn.close()
        
        if not account:
            await interaction.response.send_message("❌ Account not found. Please check the account number.", ephemeral=True)
            return
        
        account_id, owner_id, currency, institution_name = account
        
        # Check if user owns the account (for now, only owners can deposit)
        if owner_id != str(interaction.user.id):
            await interaction.response.send_message("❌ You can only deposit to your own accounts.", ephemeral=True)
            return
        
        # Process deposit
        result = await bank_deposit(account_id, str(interaction.user.id), amount)
        
        if not result['success']:
            await interaction.response.send_message(f"❌ {result['error']}", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="💰 Deposit Successful!",
            description=f"Deposited ₯{amount:.2f} to your account",
            color=0x00AA00
        )
        
        embed.add_field(
            name="🏦 Account Details",
            value=f"Bank: {result['institution_name']}\nAccount: `{result['account_number']}`\nCurrency: {result['currency'].replace('_', ' ').title()}",
            inline=False
        )
        
        embed.add_field(
            name="💸 Transaction Summary",
            value=f"Deposited: ₯{result['amount']:.2f}\nNew Account Balance: ₯{result['new_account_balance']:.2f}\nRemaining Wallet: ₯{result['new_wallet_balance']:.2f}",
            inline=False
        )
        
        embed.set_footer(text="Transaction recorded in Islamic banking ledger")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error processing deposit. Please try again later.", ephemeral=True)
        print(f"Bank deposit command error: {e}")

@bot.tree.command(name="bank_withdraw", description="Withdraw money from a bank account to your wallet")
async def bank_withdraw_cmd(interaction: discord.Interaction, account_number: str, amount: float):
    """Withdraw money from bank account to wallet"""
    try:
        # Find account by account number
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ba.id, ba.owner_user_id, ba.currency, b.business_name
            FROM bank_accounts ba
            JOIN businesses b ON ba.institution_business_id = b.id
            WHERE ba.account_number = ? AND ba.status = 'active'
        ''', (account_number,))
        
        account = cursor.fetchone()
        conn.close()
        
        if not account:
            await interaction.response.send_message("❌ Account not found. Please check the account number.", ephemeral=True)
            return
        
        account_id, owner_id, currency, institution_name = account
        
        # Check if user owns the account (for now, only owners can withdraw)
        if owner_id != str(interaction.user.id):
            await interaction.response.send_message("❌ You can only withdraw from your own accounts.", ephemeral=True)
            return
        
        # Process withdrawal
        result = await bank_withdraw(account_id, str(interaction.user.id), amount)
        
        if not result['success']:
            await interaction.response.send_message(f"❌ {result['error']}", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="💸 Withdrawal Successful!",
            description=f"Withdrew ₯{amount:.2f} from your account",
            color=0x00AA00
        )
        
        embed.add_field(
            name="🏦 Account Details",
            value=f"Bank: {result['institution_name']}\nAccount: `{result['account_number']}`\nCurrency: {result['currency'].replace('_', ' ').title()}",
            inline=False
        )
        
        embed.add_field(
            name="💸 Transaction Summary",
            value=f"Withdrawn: ₯{result['amount']:.2f}\nRemaining Account Balance: ₯{result['new_account_balance']:.2f}\nNew Wallet Balance: ₯{result['new_wallet_balance']:.2f}",
            inline=False
        )
        
        embed.set_footer(text="Transaction recorded in Islamic banking ledger")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error processing withdrawal. Please try again later.", ephemeral=True)
        print(f"Bank withdrawal command error: {e}")

@bot.tree.command(name="bank_transfer", description="Transfer money between bank accounts")
async def bank_transfer_cmd(interaction: discord.Interaction, from_account: str, to_account: str, amount: float, description: str = None):
    """Transfer money between bank accounts"""
    try:
        # Find both accounts by account numbers
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ba.id, ba.account_number, ba.owner_user_id, ba.currency, b.business_name
            FROM bank_accounts ba
            JOIN businesses b ON ba.institution_business_id = b.id
            WHERE ba.account_number IN (?, ?) AND ba.status = 'active'
        ''', (from_account, to_account))
        
        accounts = cursor.fetchall()
        conn.close()
        
        if len(accounts) != 2:
            await interaction.response.send_message("❌ One or both account numbers not found. Please check the account numbers.", ephemeral=True)
            return
        
        # Find which account is which
        from_acc = next((acc for acc in accounts if acc[1] == from_account), None)
        to_acc = next((acc for acc in accounts if acc[1] == to_account), None)
        
        if not from_acc or not to_acc:
            await interaction.response.send_message("❌ Error identifying accounts. Please try again.", ephemeral=True)
            return
        
        from_id, from_number, from_owner, from_currency, from_institution = from_acc
        to_id, to_number, to_owner, to_currency, to_institution = to_acc
        
        # Check if user owns the source account
        if from_owner != str(interaction.user.id):
            await interaction.response.send_message("❌ You can only transfer from your own accounts.", ephemeral=True)
            return
        
        # Process transfer
        result = await bank_transfer(from_id, to_id, str(interaction.user.id), amount, description)
        
        if not result['success']:
            await interaction.response.send_message(f"❌ {result['error']}", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="🔄 Transfer Successful!",
            description=f"Transferred ₯{amount:.2f} between accounts",
            color=0x00AA00
        )
        
        embed.add_field(
            name="📤 From Account",
            value=f"Bank: {result['from_institution']}\nAccount: `{result['from_account']}`\nNew Balance: ₯{result['new_from_balance']:.2f}",
            inline=True
        )
        
        embed.add_field(
            name="📥 To Account",
            value=f"Bank: {result['to_institution']}\nAccount: `{result['to_account']}`\nNew Balance: ₯{result['new_to_balance']:.2f}",
            inline=True
        )
        
        embed.add_field(
            name="💸 Transfer Details",
            value=f"Amount: ₯{result['amount']:.2f} {result['currency'].replace('_', ' ')}\nDescription: {description or 'No description'}",
            inline=False
        )
        
        embed.set_footer(text="Transfer recorded in Islamic banking ledger for both accounts")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error processing transfer. Please try again later.", ephemeral=True)
        print(f"Bank transfer command error: {e}")

@bot.tree.command(name="bank_list_institutions", description="View all available Islamic finance institutions")
async def bank_list_institutions(interaction: discord.Interaction):
    """List all available Islamic finance institutions for banking"""
    try:
        businesses = get_finance_businesses()
        
        if not businesses:
            embed = discord.Embed(
                title="🏢 Islamic Finance Institutions",
                description="No Islamic finance institutions are currently available for banking services.",
                color=0x3498DB
            )
            
            embed.add_field(
                name="💡 How to Create One",
                value="Start an Islamic Finance business using `/start_business` and choose 'islamic_finance' as the business type.",
                inline=False
            )
            
            await interaction.response.send_message(embed=embed, ephemeral=True)
            return
        
        embed = discord.Embed(
            title="🏢 Available Islamic Finance Institutions",
            description="Licensed institutions offering Shariah-compliant banking services",
            color=0x00AA00
        )
        
        for business_id, owner_id, business_name, license_code, owner_name in businesses:
            embed.add_field(
                name=f"🏛️ {business_name}",
                value=f"Owner: {owner_name}\nLicense: `{license_code}`\nServices: Wadiah & Mudarabah accounts",
                inline=True
            )
        
        embed.add_field(
            name="🔗 How to Open Account",
            value="Use `/bank_open_account` with the institution's license code\nChoose account type: 'wadiah' (safekeeping) or 'mudarabah' (profit-sharing)",
            inline=False
        )
        
        embed.set_footer(text="All institutions follow strict Islamic banking principles")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving institutions. Please try again later.", ephemeral=True)
        print(f"Bank institutions list error: {e}")

# === OVERDUE LOAN RESET SYSTEM ===

@bot.tree.command(name="check_overdue_loans", description="[ADMIN] Check for overdue loans and process resets")
async def check_overdue_loans_command(interaction: discord.Interaction):
    """Manual command to check overdue loans (admin only)"""
    # For now, anyone can use this. You could add admin checks here
    await interaction.response.defer()
    
    try:
        await process_overdue_loans()
        overdue_count = len(check_overdue_loans())
        
        embed = discord.Embed(
            title="🔍 Overdue Loan Check Complete",
            description=f"Processed overdue loan check",
            color=0x00AA00
        )
        
        embed.add_field(
            name="📊 Results",
            value=f"Currently {overdue_count} overdue loans found\nAutomated warnings and resets processed",
            inline=False
        )
        
        embed.set_footer(text="System runs automatically every 24 hours")
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.followup.send(f"❌ Error checking overdue loans: {str(e)}", ephemeral=True)
        print(f"Manual overdue loan check error: {e}")

def check_overdue_loans():
    """Check for loans that are overdue by more than 2 weeks"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Calculate cutoff date (2 weeks ago)
        cutoff_date = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
        
        # Find loans that are overdue by more than 2 weeks and not fully repaid
        cursor.execute('''
            SELECT DISTINCT borrower_id, due_date, loan_amount, repaid_amount, id
            FROM loans 
            WHERE status = 'active' 
            AND due_date < ? 
            AND repaid_amount < loan_amount
        ''', (cutoff_date,))
        
        overdue_loans = cursor.fetchall()
        conn.close()
        
        return overdue_loans
        
    except Exception as e:
        print(f"Error checking overdue loans: {e}")
        return []

def reset_user_data(user_id: str, reason: str):
    """Reset user's complete financial data due to unpaid loans"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Log the reset action before deletion
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, 'account_reset', 0, 'system', f'Account reset: {reason}'))
        
        # Reset financial balances to zero
        cursor.execute('''
            UPDATE users SET 
                gold_dinars = 0,
                silver_dirhams = 0,
                last_work_date = NULL,
                last_community_service = NULL,
                last_quran_recitation = NULL,
                last_skill_development = NULL,
                last_mentoring = NULL,
                current_job = NULL,
                daily_earnings = 0,
                skill_count = 0,
                charity_count = 0,
                quran_count = 0,
                mentoring_count = 0,
                service_count = 0
            WHERE user_id = ?
        ''', (user_id,))
        
        # Close all active businesses
        cursor.execute('''
            UPDATE businesses SET status = 'closed' WHERE user_id = ? AND status = 'active'
        ''', (user_id,))
        
        # Cancel all pending loan applications
        cursor.execute('''
            UPDATE loan_applications SET status = 'cancelled' 
            WHERE borrower_id = ? AND status = 'pending'
        ''', (user_id,))
        
        # Mark all borrowed loans as defaulted
        cursor.execute('''
            UPDATE loans SET status = 'defaulted' 
            WHERE borrower_id = ? AND status = 'active'
        ''', (user_id,))
        
        # Cancel all active investments
        cursor.execute('''
            UPDATE investments SET status = 'cancelled' 
            WHERE user_id = ? AND status = 'active'
        ''', (user_id,))
        
        # Delete marketplace listings
        cursor.execute('''
            DELETE FROM marketplace WHERE seller_id = ?
        ''', (user_id,))
        
        conn.commit()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error resetting user data for {user_id}: {e}")
        return False

async def send_overdue_warning(user_id: str, days_overdue: int):
    """Send warning to user about overdue loan"""
    try:
        user = bot.get_user(int(user_id))
        if not user:
            return False
            
        embed = discord.Embed(
            title="⚠️ URGENT: Overdue Loan Warning",
            description="Your loan payment is significantly overdue",
            color=0xFF0000
        )
        
        embed.add_field(
            name="🚨 Critical Notice",
            value=f"Your loan has been overdue for {days_overdue} days.\n"
                  f"**If not repaid within {14-days_overdue} days, your account will be completely reset.**",
            inline=False
        )
        
        embed.add_field(
            name="⏰ Account Reset Warning",
            value="Account reset includes:\n"
                  "• All gold dinars and silver dirhams set to 0\n"
                  "• All businesses closed\n"
                  "• All investments cancelled\n"
                  "• All marketplace listings removed\n"
                  "• Complete financial history preserved for transparency",
            inline=False
        )
        
        embed.add_field(
            name="💡 How to Avoid Reset",
            value="Use `/my_loan_status` to see your loans\n"
                  "Use `/repay_loan [loan_id] [amount]` to make payments\n"
                  "Contact lenders for payment arrangements if needed",
            inline=False
        )
        
        embed.set_footer(text="This follows Islamic finance principles of accountability and responsibility")
        
        await user.send(embed=embed)
        return True
        
    except Exception as e:
        print(f"Error sending overdue warning to user {user_id}: {e}")
        return False

async def process_overdue_loans():
    """Process all overdue loans and reset accounts if necessary"""
    overdue_loans = check_overdue_loans()
    
    if not overdue_loans:
        return
    
    processed_users = set()
    
    for user_id, due_date, loan_amount, repaid_amount, loan_id in overdue_loans:
        if user_id in processed_users:
            continue
            
        try:
            # Calculate days overdue
            due_datetime = datetime.datetime.strptime(due_date, '%Y-%m-%d')
            days_overdue = (datetime.datetime.now() - due_datetime).days
            
            if days_overdue >= 14:
                # Reset user account
                remaining = loan_amount - repaid_amount
                reset_reason = f"Loan default - ₯{remaining:.2f} unpaid for {days_overdue} days"
                
                if reset_user_data(user_id, reset_reason):
                    print(f"Reset account for user {user_id} due to loan default")
                    
                    # Notify user of reset
                    try:
                        user = bot.get_user(int(user_id))
                        if user:
                            embed = discord.Embed(
                                title="🔄 Account Reset Due to Loan Default",
                                description="Your account has been reset due to unpaid loans",
                                color=0xFF4444
                            )
                            
                            embed.add_field(
                                name="📋 Reset Details",
                                value=f"Reason: {reset_reason}\n"
                                      f"You can start fresh with a new account.\n"
                                      f"Your transaction history remains for transparency.",
                                inline=False
                            )
                            
                            embed.add_field(
                                name="🔄 Starting Over",
                                value="You can now:\n"
                                      "• Create new account with `/account`\n"
                                      "• Start earning with halal methods\n"
                                      "• Rebuild your Islamic economy presence\n"
                                      "• Learn from this experience",
                                inline=False
                            )
                            
                            embed.set_footer(text="This follows Islamic principles of accountability and second chances")
                            
                            await user.send(embed=embed)
                    except:
                        pass
                        
                processed_users.add(user_id)
                
            elif days_overdue >= 7:  # 1 week overdue - send warning
                await send_overdue_warning(user_id, days_overdue)
                processed_users.add(user_id)
                
        except Exception as e:
            print(f"Error processing overdue loan for user {user_id}: {e}")

# === PORTFOLIO MANAGEMENT (HALAL FOCUS) ===
# Portfolio functionality handled by the existing my_portfolio command on line 3117

@bot.tree.command(name="transaction_history", description="View your complete transaction history")
async def transaction_history(interaction: discord.Interaction, limit: int = 20):
    """View user's complete transaction history including wages, profits, and all financial activity"""
    
    if limit < 1 or limit > 50:
        await interaction.response.send_message("❌ Limit must be between 1 and 50 transactions")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get user's recent transactions
        cursor.execute('''
            SELECT transaction_type, amount, currency, description, partner_id, timestamp, is_halal
            FROM transactions 
            WHERE user_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
        ''', (str(interaction.user.id), limit))
        
        transactions = cursor.fetchall()
        conn.close()
        
        if not transactions:
            await interaction.response.send_message("📊 No transaction history found. Start earning and spending to build your financial record!")
            return
        
        # Transaction type emojis and descriptions
        transaction_icons = {
            'business_profit': '💼',
            'business_startup': '🏢',
            'job_income': '💰',
            'freelance_work': '💻',
            'daily_task': '✅',
            'volunteer_work': '🤝',
            'skill_learning': '📚',
            'transfer_send': '📤',
            'transfer_receive': '📥',
            'share_purchase': '📈',
            'share_sale': '📉',
            'government_stock_purchase': '🏛️',
            'government_stock_sale': '🏛️',
            'investment': '💎',
            'investment_withdrawal': '💵',
            'marketplace_purchase': '🛒',
            'marketplace_sale': '🏪',
            'loan_given': '🤝',
            'loan_received': '💳',
            'loan_repayment': '✅',
            'loan_repayment_received': '💰',
            'zakat': '🕌',
            'business_merger': '🔄',
            'admin_adjustment': '⚙️',
            'admin_gift': '🎁',
            'employee_expense': '👔'
        }
        
        # Format transactions for display
        transaction_list = []
        total_income = 0
        total_expenses = 0
        
        for trans_type, amount, currency, description, partner_id, timestamp, is_halal in transactions:
            icon = transaction_icons.get(trans_type, '💰')
            
            # Determine if this is income or expense
            is_income = trans_type in [
                'business_profit', 'job_income', 'freelance_work', 'daily_task', 
                'volunteer_work', 'skill_learning', 'transfer_receive', 'share_sale',
                'government_stock_sale', 'investment_withdrawal', 'marketplace_sale',
                'loan_received', 'loan_repayment_received', 'admin_gift'
            ]
            
            if is_income:
                total_income += amount
                amount_str = f"+₯{amount:.2f}"
                color = "🟢"
            else:
                total_expenses += amount
                amount_str = f"-₯{amount:.2f}"
                color = "🔴"
            
            # Format timestamp
            try:
                dt = datetime.datetime.fromisoformat(timestamp)
                time_str = dt.strftime("%m/%d %H:%M")
            except:
                time_str = timestamp[:10] if timestamp else "Unknown"
            
            # Truncate description if too long
            desc = description[:40] + "..." if description and len(description) > 40 else description or "No description"
            
            # Halal indicator
            halal_indicator = "✅" if is_halal else "⚠️"
            
            transaction_list.append(f"{icon} {color} {amount_str} {currency} - {desc} ({time_str}) {halal_indicator}")
        
        # Create embed
        embed = discord.Embed(
            title="📊 Transaction History",
            description=f"Your last {len(transactions)} financial transactions",
            color=0x00AA00
        )
        
        # Split transactions into pages if too many
        transactions_per_page = 10
        pages = [transaction_list[i:i + transactions_per_page] for i in range(0, len(transaction_list), transactions_per_page)]
        
        # Show first page
        if pages:
            embed.add_field(
                name="📋 Recent Transactions",
                value="\n".join(pages[0]),
                inline=False
            )
        
        # Add summary
        net_total = total_income - total_expenses
        summary_color = "🟢" if net_total >= 0 else "🔴"
        
        embed.add_field(
            name="📊 Summary",
            value=f"💰 Total Income: +₯{total_income:.2f}\n💸 Total Expenses: -₯{total_expenses:.2f}\n{summary_color} Net: {'+' if net_total >= 0 else ''}₯{net_total:.2f}",
            inline=False
        )
        
        # Legend
        embed.add_field(
            name="🔍 Legend",
            value="🟢 = Income | 🔴 = Expense | ✅ = Halal | ⚠️ = Review Needed",
            inline=False
        )
        
        # Add footer with pagination info if multiple pages
        if len(pages) > 1:
            embed.set_footer(text=f"Page 1 of {len(pages)} • Use command again for more recent transactions")
        else:
            embed.set_footer(text="Complete transaction history shown")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving transaction history. Please try again later.")
        print(f"Transaction history error: {e}")

# === USER-TO-USER EMPLOYMENT SYSTEM ===
@bot.tree.command(name="post_job", description="Post a job opening at your business")
async def post_job(interaction: discord.Interaction, business_name: str, job_title: str, salary: float, currency: str, description: str = ""):
    """Post a job opening at your active business"""
    
    if currency.lower() not in ['gold_dinars', 'silver_dirhams']:
        await interaction.response.send_message("❌ Currency must be 'gold_dinars' or 'silver_dirhams'")
        return
    
    if salary <= 0:
        await interaction.response.send_message("❌ Salary must be positive")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if user owns the business and it's active
        cursor.execute('''
            SELECT id, business_type FROM businesses 
            WHERE user_id = ? AND business_name = ? AND status = 'active'
        ''', (str(interaction.user.id), business_name))
        
        business = cursor.fetchone()
        if not business:
            await interaction.response.send_message(f"❌ Business '{business_name}' not found or not active. Use `/my_businesses` to see your businesses.")
            conn.close()
            return
        
        business_id, business_type = business
        
        # Create job posting
        cursor.execute('''
            INSERT INTO job_postings (employer_id, business_id, job_title, salary, currency, description)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), business_id, job_title, salary, currency, description or f"Work at {business_name}"))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="📢 Job Posted Successfully!",
            description=f"Job opening posted at {business_name}",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💼 Job Details",
            value=f"Position: {job_title}\nBusiness: {business_name} ({business_type})\nSalary: ₯{salary:.2f} {currency.replace('_', ' ')}\nDescription: {description or 'No description provided'}",
            inline=False
        )
        
        embed.add_field(
            name="📋 Next Steps",
            value="Other users can now apply using `/apply_job [posting_id]`\nUse `/my_job_postings` to manage your job openings",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error posting job. Please try again later.")
        print(f"Job posting error: {e}")


@bot.tree.command(name="job_openings", description="View available job openings from other users")
async def job_openings(interaction: discord.Interaction):
    """View all available job openings posted by business owners"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT jp.id, jp.job_title, jp.salary, jp.currency, jp.description, 
                   b.business_name, b.business_type, u.username
            FROM job_postings jp
            JOIN businesses b ON jp.business_id = b.id
            JOIN users u ON jp.employer_id = u.user_id
            WHERE jp.status = 'open' AND jp.employer_id != ? AND b.status = 'active'
            ORDER BY jp.posted_date DESC
        ''', (str(interaction.user.id),))
        
        job_openings = cursor.fetchall()
        conn.close()
        
        if not job_openings:
            await interaction.response.send_message("📋 No job openings available right now. Check back later!")
            return
        
        embed = discord.Embed(
            title="💼 Available Job Openings",
            description="Jobs posted by business owners in the Islamic economy",
            color=0x0066CC
        )
        
        for job_id, job_title, salary, currency, description, business_name, business_type, employer_name in job_openings[:10]:
            embed.add_field(
                name=f"🏢 {job_title} at {business_name}",
                value=f"Employer: {employer_name}\nSalary: ₯{salary:.2f} {currency.replace('_', ' ')}\nBusiness Type: {business_type}\nJob ID: {job_id}\n{description[:100]}{'...' if len(description) > 100 else ''}",
                inline=False
            )
        
        embed.add_field(
            name="💡 How to Apply",
            value="Use `/apply_job [job_id]` to apply for any position!",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving job openings.")
        print(f"Job openings error: {e}")


@bot.tree.command(name="apply_job", description="Apply for a job posted by another user")
async def apply_job(interaction: discord.Interaction, job_id: int):
    """Apply for a job opening"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if job posting exists and is open
        cursor.execute('''
            SELECT jp.employer_id, jp.job_title, jp.salary, jp.currency, jp.business_id,
                   b.business_name, u.username
            FROM job_postings jp
            JOIN businesses b ON jp.business_id = b.id
            JOIN users u ON jp.employer_id = u.user_id
            WHERE jp.id = ? AND jp.status = 'open' AND b.status = 'active'
        ''', (job_id,))
        
        job_posting = cursor.fetchone()
        if not job_posting:
            await interaction.response.send_message("❌ Job posting not found or no longer available.")
            conn.close()
            return
        
        employer_id, job_title, salary, currency, business_id, business_name, employer_name = job_posting
        
        if employer_id == str(interaction.user.id):
            await interaction.response.send_message("❌ You cannot apply to your own job posting.")
            conn.close()
            return
        
        # Check if user already works for someone else
        cursor.execute('''
            SELECT employer_id FROM user_employment 
            WHERE employee_id = ? AND status = 'active'
        ''', (str(interaction.user.id),))
        
        current_employment = cursor.fetchone()
        if current_employment:
            await interaction.response.send_message("❌ You are already employed by another user. Use `/quit_user_job` to leave your current position first.")
            conn.close()
            return
        
        # Create employment relationship
        cursor.execute('''
            INSERT INTO user_employment (employer_id, employee_id, business_id, job_title, salary, currency)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (employer_id, str(interaction.user.id), business_id, job_title, salary, currency))
        
        # Mark job posting as filled
        cursor.execute('''
            UPDATE job_postings SET status = 'filled' WHERE id = ?
        ''', (job_id,))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🎉 Job Application Successful!",
            description=f"You're now employed as {job_title} at {business_name}!",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💼 Employment Details",
            value=f"Position: {job_title}\nEmployer: {employer_name}\nBusiness: {business_name}\nSalary: ₯{salary:.2f} {currency.replace('_', ' ')} per work session",
            inline=False
        )
        
        embed.add_field(
            name="📋 Next Steps",
            value="• Use `/work_for_user` to work your shift and earn money\n• Your employer benefits from your productivity\n• Use `/quit_user_job` if you want to leave",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error applying for job. Please try again later.")
        print(f"Job application error: {e}")


@bot.tree.command(name="work_for_user", description="Work your shift for your user employer")
async def work_for_user(interaction: discord.Interaction):
    """Work a shift for your user employer"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check current employment
        cursor.execute('''
            SELECT ue.employer_id, ue.job_title, ue.salary, ue.currency, ue.business_id,
                   b.business_name, u.username
            FROM user_employment ue
            JOIN businesses b ON ue.business_id = b.id
            JOIN users u ON ue.employer_id = u.user_id
            WHERE ue.employee_id = ? AND ue.status = 'active' AND b.status = 'active'
        ''', (str(interaction.user.id),))
        
        employment = cursor.fetchone()
        if not employment:
            await interaction.response.send_message("❌ You don't have an active employment with another user. Use `/apply_job` to find work!")
            conn.close()
            return
        
        employer_id, job_title, salary, currency, business_id, business_name, employer_name = employment
        
        # Calculate taxes
        taxes = calculate_taxes(salary, currency)
        net_pay = salary - taxes
        
        # Get user data
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        
        # Update employee balance
        if currency == 'gold_dinars':
            new_gold = user_data['gold_dinars'] + net_pay
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_gold, user_data['user_id']))
        else:
            new_silver = user_data['silver_dirhams'] + net_pay
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_silver, user_data['user_id']))
        
        # Deduct cost from employer (business profit reduction)
        employer_data = get_user_account(employer_id, employer_name)
        if currency == 'gold_dinars':
            employer_new_gold = employer_data['gold_dinars'] - salary
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (employer_new_gold, employer_id))
        else:
            employer_new_silver = employer_data['silver_dirhams'] - salary
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (employer_new_silver, employer_id))
        
        # Record employee transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'employment_income', net_pay, currency, f'Worked as {job_title} for {employer_name}'))
        
        # Record employer transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (employer_id, 'employment_expense', salary, currency, f'Paid {user_data["username"]} for {job_title} work'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="💼 Work Shift Completed!",
            description=f"You completed a work shift as {job_title} for {employer_name}",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💰 Earnings",
            value=f"Gross Pay: ₯{salary:.2f}\nTaxes: ₯{taxes:.2f}\n**Net Pay: ₯{net_pay:.2f}**",
            inline=False
        )
        
        embed.add_field(
            name="🏢 Work Details",
            value=f"Position: {job_title}\nEmployer: {employer_name}\nBusiness: {business_name}",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error during work shift. Please try again later.")
        print(f"Work for user error: {e}")


@bot.tree.command(name="quit_user_job", description="Leave your current user employment")
async def quit_user_job(interaction: discord.Interaction):
    """Quit your current employment with another user"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check current employment
        cursor.execute('''
            SELECT ue.employer_id, ue.job_title, b.business_name, u.username
            FROM user_employment ue
            JOIN businesses b ON ue.business_id = b.id
            JOIN users u ON ue.employer_id = u.user_id
            WHERE ue.employee_id = ? AND ue.status = 'active'
        ''', (str(interaction.user.id),))
        
        employment = cursor.fetchone()
        if not employment:
            await interaction.response.send_message("❌ You don't have an active employment with another user.")
            conn.close()
            return
        
        employer_id, job_title, business_name, employer_name = employment
        
        # End employment
        cursor.execute('''
            UPDATE user_employment 
            SET status = 'quit', employment_end = ?
            WHERE employee_id = ? AND status = 'active'
        ''', (datetime.datetime.now().isoformat(), str(interaction.user.id)))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="👋 Employment Ended",
            description=f"You have quit your job as {job_title}",
            color=0xFF6600
        )
        
        embed.add_field(
            name="📋 Final Details",
            value=f"Former Position: {job_title}\nFormer Employer: {employer_name}\nBusiness: {business_name}",
            inline=False
        )
        
        embed.add_field(
            name="🔍 What's Next?",
            value="• Use `/job_openings` to find new employment opportunities\n• Use `/get_job` for traditional NPC employment\n• Start your own business with `/start_business`",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error quitting job. Please try again later.")
        print(f"Quit user job error: {e}")


@bot.tree.command(name="my_employees", description="View and manage your employees")
async def my_employees(interaction: discord.Interaction):
    """View employees working at your businesses"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ue.employee_id, ue.job_title, ue.salary, ue.currency, ue.employment_start,
                   b.business_name, u.username
            FROM user_employment ue
            JOIN businesses b ON ue.business_id = b.id
            JOIN users u ON ue.employee_id = u.user_id
            WHERE ue.employer_id = ? AND ue.status = 'active' AND b.status = 'active'
            ORDER BY ue.employment_start DESC
        ''', (str(interaction.user.id),))
        
        employees = cursor.fetchall()
        conn.close()
        
        if not employees:
            await interaction.response.send_message("👥 You don't have any employees yet. Use `/post_job` to hire workers for your businesses!")
            return
        
        embed = discord.Embed(
            title="👥 Your Employees",
            description="Workers employed at your businesses",
            color=0x0066CC
        )
        
        for employee_id, job_title, salary, currency, start_date, business_name, employee_name in employees:
            embed.add_field(
                name=f"👤 {employee_name}",
                value=f"Position: {job_title}\nBusiness: {business_name}\nSalary: ₯{salary:.2f} {currency.replace('_', ' ')}\nStarted: {start_date[:10]}",
                inline=True
            )
        
        embed.add_field(
            name="💡 Management",
            value="Your employees can work using `/work_for_user`\nTheir salary will be deducted from your account when they work",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error retrieving employee information.")
        print(f"My employees error: {e}")


@bot.tree.command(name="become_imam", description="Become an imam of a mosque (special religious position)")
async def become_imam(interaction: discord.Interaction, mosque_name: str):
    """Become an imam of a mosque - special religious leadership position"""
    
    user_data = get_user_account(
        str(interaction.user.id), interaction.user.display_name
        or interaction.user.name)
    
    # Requirements to become imam
    required_charity = 50.0  # Must have given significant charity
    required_wealth = 200.0  # Must demonstrate financial stability
    
    if user_data['total_charity'] < required_charity:
        await interaction.response.send_message(
            f"❌ To become an imam, you must have given at least ₯{required_charity:.0f} in charity.\n"
            f"You have given ₯{user_data['total_charity']:.2f}. Continue giving charity and paying Zakat to qualify."
        )
        return
    
    if (user_data['gold_dinars'] + user_data['silver_dirhams']) < required_wealth:
        total_wealth = user_data['gold_dinars'] + user_data['silver_dirhams']
        await interaction.response.send_message(
            f"❌ To become an imam, you must have at least ₯{required_wealth:.0f} total wealth.\n"
            f"You have ₯{total_wealth:.2f}. Build your wealth through halal means to qualify."
        )
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if user already has an active imam position
        cursor.execute('''
            SELECT job_title FROM jobs 
            WHERE user_id = ? AND job_title = 'imam' AND status = 'active'
        ''', (str(interaction.user.id),))
        
        existing_imam = cursor.fetchone()
        if existing_imam:
            await interaction.response.send_message("❌ You are already serving as an imam! You can only lead one mosque at a time.")
            conn.close()
            return
        
        # Check if user has any other active job
        cursor.execute('''
            SELECT job_title FROM jobs 
            WHERE user_id = ? AND status = 'active'
        ''', (str(interaction.user.id),))
        
        current_job = cursor.fetchone()
        if current_job:
            await interaction.response.send_message(
                f"❌ You currently work as {current_job[0]}. You must quit your current job first to serve as an imam.\n"
                f"Use `/quit_job` to leave your current position."
            )
            conn.close()
            return
        
        # Enhanced imam salary with spiritual bonuses
        base_salary = 35.0  # Higher than regular imam job
        charity_bonus = min(user_data['total_charity'] * 0.1, 20.0)  # Up to 20 bonus from charity
        final_salary = base_salary + charity_bonus
        
        # Create imam position
        cursor.execute('''
            INSERT INTO jobs (user_id, job_title, employer, salary, currency, status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), 'imam', f'Imam of {mosque_name}', final_salary, 'gold_dinars', 'active'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🕌 Appointed as Imam!",
            description=f"Barakallahu feek! You are now the imam of {mosque_name}",
            color=0x00AA00
        )
        
        embed.add_field(
            name="🤲 Religious Responsibilities",
            value=(
                "• Lead prayers (Salah) for the community\n"
                "• Provide Islamic guidance and education\n"
                "• Conduct marriages and funeral services\n"
                "• Give Friday sermons (Khutbah)\n"
                "• Help resolve community disputes"
            ),
            inline=False
        )
        
        embed.add_field(
            name="💰 Compensation",
            value=(
                f"Base Salary: ₯{base_salary:.2f} gold dinars\n"
                f"Charity Bonus: ₯{charity_bonus:.2f}\n"
                f"**Total Salary: ₯{final_salary:.2f} per work session**\n"
                f"(Bonus based on your charitable giving)"
            ),
            inline=False
        )
        
        embed.add_field(
            name="📿 Special Benefits",
            value=(
                "• Higher salary than regular imam position\n"
                "• Salary increases with your charitable deeds\n"
                "• Spiritual reward for serving the community\n"
                "• Respected leadership position in the Ummah"
            ),
            inline=False
        )
        
        embed.add_field(
            name="📋 How to Serve",
            value=(
                "• Use `/work` to conduct religious services\n"
                "• Continue giving charity to increase your bonus\n"
                "• Use `/quit_job` if you need to step down"
            ),
            inline=False
        )
        
        embed.set_footer(
            text="🕌 May Allah bless your service to the community • Leadership is a trust (Amanah)"
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error appointing imam position. Please try again later.")
        print(f"Become imam error: {e}")


# === DAILY TASKS SYSTEM ===
@bot.tree.command(name="daily_tasks", description="View and complete daily tasks for extra income")
async def daily_tasks(interaction: discord.Interaction):
    """Complete daily tasks for additional halal income"""
    
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check what tasks user has completed today
        cursor.execute('''
            SELECT task_type FROM daily_tasks 
            WHERE user_id = ? AND completion_date = ?
        ''', (str(interaction.user.id), today))
        
        completed_tasks = [row[0] for row in cursor.fetchall()]
        
        # Available daily tasks
        available_tasks = {
            'prayer_reminder': {'reward': 2.0, 'currency': 'gold_dinars', 'desc': 'Remember your 5 daily prayers'},
            'quran_reading': {'reward': 3.0, 'currency': 'gold_dinars', 'desc': 'Read Quran for spiritual growth'},
            'charity_giving': {'reward': 5.0, 'currency': 'silver_dirhams', 'desc': 'Give charity to those in need'},
            'islamic_learning': {'reward': 2.5, 'currency': 'gold_dinars', 'desc': 'Learn something new about Islam'},
            'community_help': {'reward': 4.0, 'currency': 'silver_dirhams', 'desc': 'Help someone in your community'},
            'business_check': {'reward': 1.5, 'currency': 'gold_dinars', 'desc': 'Check on your business operations'},
            'market_analysis': {'reward': 2.0, 'currency': 'gold_dinars', 'desc': 'Analyze market trends for investments'}
        }
        
        embed = discord.Embed(
            title="📅 Daily Tasks - Blessed Activities",
            description="Complete daily tasks to earn rewards and build good habits",
            color=0x4CAF50
        )
        
        pending_tasks = []
        for task_name, task_info in available_tasks.items():
            status = "✅ Completed" if task_name in completed_tasks else "⏳ Available"
            pending_tasks.append(f"{status} **{task_name.replace('_', ' ').title()}**\n   ₯{task_info['reward']:.1f} {task_info['currency'].replace('_', ' ')}\n   {task_info['desc']}")
        
        embed.add_field(
            name="🎯 Today's Tasks",
            value="\n\n".join(pending_tasks),
            inline=False
        )
        
        uncompleted_count = len(available_tasks) - len(completed_tasks)
        if uncompleted_count > 0:
            embed.add_field(
                name="💡 How to Complete",
                value=f"Use `/complete_task [task_name]` to complete remaining tasks!\n{uncompleted_count} tasks remaining today.",
                inline=False
            )
        else:
            embed.add_field(
                name="🎉 All Done!",
                value="Mashallah! You've completed all daily tasks. Come back tomorrow for new opportunities!",
                inline=False
            )
        
        embed.set_footer(text="🌙 Daily tasks reset every day • Build consistent Islamic habits")
        
        conn.close()
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error loading daily tasks.")
        print(f"Daily tasks error: {e}")


@bot.tree.command(name="complete_task", description="Complete a daily task for rewards")
async def complete_task(interaction: discord.Interaction, task_name: str):
    """Complete a daily task and earn rewards"""
    
    available_tasks = {
        'prayer_reminder': {'reward': 2.0, 'currency': 'gold_dinars'},
        'quran_reading': {'reward': 3.0, 'currency': 'gold_dinars'},
        'charity_giving': {'reward': 5.0, 'currency': 'silver_dirhams'},
        'islamic_learning': {'reward': 2.5, 'currency': 'gold_dinars'},
        'community_help': {'reward': 4.0, 'currency': 'silver_dirhams'},
        'business_check': {'reward': 1.5, 'currency': 'gold_dinars'},
        'market_analysis': {'reward': 2.0, 'currency': 'gold_dinars'}
    }
    
    if task_name.lower() not in available_tasks:
        await interaction.response.send_message(f"❌ Task '{task_name}' not found. Use `/daily_tasks` to see available tasks.")
        return
    
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if task already completed today
        cursor.execute('''
            SELECT id FROM daily_tasks 
            WHERE user_id = ? AND task_type = ? AND completion_date = ?
        ''', (str(interaction.user.id), task_name.lower(), today))
        
        if cursor.fetchone():
            await interaction.response.send_message(f"❌ You've already completed '{task_name}' today. Try again tomorrow!")
            conn.close()
            return
        
        task_info = available_tasks[task_name.lower()]
        reward = task_info['reward']
        currency = task_info['currency']
        
        # Calculate taxes
        taxes = calculate_taxes(reward, currency)
        net_reward = reward - taxes
        
        # Update user balance
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        
        if currency == 'gold_dinars':
            new_gold = user_data['gold_dinars'] + net_reward
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_gold, user_data['user_id']))
        else:
            new_silver = user_data['silver_dirhams'] + net_reward
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_silver, user_data['user_id']))
        
        # Record task completion
        cursor.execute('''
            INSERT INTO daily_tasks (user_id, task_type, completion_date, reward_amount, currency)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], task_name.lower(), today, net_reward, currency))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'daily_task', net_reward, currency, f'Completed daily task: {task_name}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="✅ Task Completed!",
            description=f"Barakallahu feek! You completed: **{task_name.replace('_', ' ').title()}**",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💰 Reward Earned",
            value=f"Gross: ₯{reward:.1f}\nTaxes: ₯{taxes:.2f}\n**Net: ₯{net_reward:.2f} {currency.replace('_', ' ')}**",
            inline=False
        )
        
        embed.set_footer(text="🌟 Keep up the good work! Daily consistency brings great rewards.")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error completing task.")
        print(f"Complete task error: {e}")


# === MARKETPLACE TRADING SYSTEM ===
@bot.tree.command(name="list_item", description="List an item for sale in the marketplace")
async def list_item(interaction: discord.Interaction, item_name: str, price: float, currency: str, description: str = "", quantity: int = 1):
    """List items for sale in the Islamic marketplace"""
    
    if currency.lower() not in ['gold_dinars', 'silver_dirhams']:
        await interaction.response.send_message("❌ Currency must be 'gold_dinars' or 'silver_dirhams'")
        return
    
    if price <= 0 or quantity <= 0:
        await interaction.response.send_message("❌ Price and quantity must be positive")
        return
    
    # Categorize items
    halal_categories = {
        'food': ['bread', 'rice', 'meat', 'fruit', 'vegetables', 'dates', 'honey', 'milk'],
        'clothing': ['robe', 'hijab', 'thobe', 'abaya', 'shoes', 'belt', 'hat'],
        'crafts': ['pottery', 'jewelry', 'carpets', 'calligraphy', 'woodwork', 'metalwork'],
        'books': ['quran', 'hadith', 'islamic', 'knowledge', 'education', 'learning'],
        'tools': ['hammer', 'saw', 'plow', 'fishing', 'weaving', 'cooking'],
        'materials': ['wood', 'cloth', 'leather', 'stone', 'metal', 'wool']
    }
    
    item_category = 'general'
    item_lower = item_name.lower()
    for category, keywords in halal_categories.items():
        if any(keyword in item_lower for keyword in keywords):
            item_category = category
            break
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # List the item
        cursor.execute('''
            INSERT INTO marketplace (seller_id, item_name, item_type, price, currency, description, quantity)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (str(interaction.user.id), item_name, item_category, price, currency, description, quantity))
        
        listing_id = cursor.lastrowid
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🏪 Item Listed Successfully!",
            description=f"Your {item_name} is now available in the marketplace",
            color=0x00AA00
        )
        
        embed.add_field(
            name="📦 Listing Details",
            value=f"Item: {item_name}\nCategory: {item_category.title()}\nPrice: ₯{price:.2f} {currency.replace('_', ' ')}\nQuantity: {quantity}\nListing ID: {listing_id}",
            inline=False
        )
        
        if description:
            embed.add_field(
                name="📝 Description",
                value=description,
                inline=False
            )
        
        embed.add_field(
            name="💡 Next Steps",
            value="Other users can now buy your item using `/buy_item [listing_id]`\nUse `/my_listings` to manage your marketplace items",
            inline=False
        )
        
        embed.set_footer(text="🤝 Fair trade builds strong communities • Honesty in business is blessed")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error listing item.")
        print(f"List item error: {e}")


@bot.tree.command(name="marketplace", description="Browse items available for purchase")
async def marketplace(interaction: discord.Interaction, category: str = "all"):
    """Browse the Islamic marketplace for goods"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        if category.lower() == "all":
            cursor.execute('''
                SELECT m.id, m.item_name, m.item_type, m.price, m.currency, m.quantity, u.username
                FROM marketplace m
                JOIN users u ON m.seller_id = u.user_id
                WHERE m.status = 'available' AND m.seller_id != ?
                ORDER BY m.listed_date DESC
                LIMIT 15
            ''', (str(interaction.user.id),))
        else:
            cursor.execute('''
                SELECT m.id, m.item_name, m.item_type, m.price, m.currency, m.quantity, u.username
                FROM marketplace m
                JOIN users u ON m.seller_id = u.user_id
                WHERE m.status = 'available' AND m.item_type = ? AND m.seller_id != ?
                ORDER BY m.listed_date DESC
                LIMIT 15
            ''', (category.lower(), str(interaction.user.id)))
        
        items = cursor.fetchall()
        conn.close()
        
        if not items:
            await interaction.response.send_message("🏪 No items available in the marketplace right now. Check back later or list your own items!")
            return
        
        embed = discord.Embed(
            title="🏪 Islamic Marketplace",
            description=f"Halal goods available for purchase {f'(Category: {category.title()})' if category != 'all' else ''}",
            color=0x0066CC
        )
        
        for item_id, item_name, item_type, price, currency, quantity, seller in items[:10]:
            embed.add_field(
                name=f"📦 {item_name} ({item_type})",
                value=f"Seller: {seller}\nPrice: ₯{price:.2f} {currency.replace('_', ' ')}\nQuantity: {quantity}\nID: {item_id}",
                inline=True
            )
        
        embed.add_field(
            name="💰 How to Purchase",
            value="Use `/buy_item [listing_id] [quantity]` to purchase any item!",
            inline=False
        )
        
        embed.add_field(
            name="📋 Categories",
            value="food, clothing, crafts, books, tools, materials, or 'all'",
            inline=False
        )
        
        embed.set_footer(text="🤝 Support fellow Muslims • Trade with fairness and honesty")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error browsing marketplace.")
        print(f"Marketplace error: {e}")


@bot.tree.command(name="buy_item", description="Purchase an item from the marketplace")
async def buy_item(interaction: discord.Interaction, listing_id: int, quantity: int = 1):
    """Buy items from other users in the marketplace"""
    
    if quantity <= 0:
        await interaction.response.send_message("❌ Quantity must be positive")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get item details
        cursor.execute('''
            SELECT m.seller_id, m.item_name, m.price, m.currency, m.quantity, u.username
            FROM marketplace m
            JOIN users u ON m.seller_id = u.user_id
            WHERE m.id = ? AND m.status = 'available'
        ''', (listing_id,))
        
        item = cursor.fetchone()
        if not item:
            await interaction.response.send_message("❌ Item not found or no longer available.")
            conn.close()
            return
        
        seller_id, item_name, price, currency, available_qty, seller_name = item
        
        if seller_id == str(interaction.user.id):
            await interaction.response.send_message("❌ You cannot buy your own items.")
            conn.close()
            return
        
        if quantity > available_qty:
            await interaction.response.send_message(f"❌ Only {available_qty} units available.")
            conn.close()
            return
        
        total_cost = price * quantity
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        
        # Check if buyer has enough money
        if currency == 'gold_dinars' and user_data['gold_dinars'] < total_cost:
            await interaction.response.send_message(f"❌ Insufficient gold dinars. Need ₯{total_cost:.2f}, you have ₯{user_data['gold_dinars']:.2f}")
            conn.close()
            return
        elif currency == 'silver_dirhams' and user_data['silver_dirhams'] < total_cost:
            await interaction.response.send_message(f"❌ Insufficient silver dirhams. Need ₯{total_cost:.2f}, you have ₯{user_data['silver_dirhams']:.2f}")
            conn.close()
            return
        
        # Process transaction
        # Deduct from buyer
        if currency == 'gold_dinars':
            new_buyer_gold = user_data['gold_dinars'] - total_cost
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_buyer_gold, user_data['user_id']))
        else:
            new_buyer_silver = user_data['silver_dirhams'] - total_cost
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_buyer_silver, user_data['user_id']))
        
        # Add to seller
        seller_data = get_user_account(seller_id, seller_name)
        if currency == 'gold_dinars':
            new_seller_gold = seller_data['gold_dinars'] + total_cost
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_seller_gold, seller_id))
        else:
            new_seller_silver = seller_data['silver_dirhams'] + total_cost
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_seller_silver, seller_id))
        
        # Update marketplace listing
        new_quantity = available_qty - quantity
        if new_quantity <= 0:
            cursor.execute('UPDATE marketplace SET status = "sold" WHERE id = ?', (listing_id,))
        else:
            cursor.execute('UPDATE marketplace SET quantity = ? WHERE id = ?', (new_quantity, listing_id))
        
        # Record transactions
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'marketplace_purchase', total_cost, currency, f'Bought {quantity}x {item_name} from {seller_name}'))
        
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (seller_id, 'marketplace_sale', total_cost, currency, f'Sold {quantity}x {item_name} to {user_data["username"]}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🛒 Purchase Successful!",
            description=f"You bought {quantity}x {item_name} from {seller_name}",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💰 Transaction Details",
            value=f"Item: {item_name}\nQuantity: {quantity}\nUnit Price: ₯{price:.2f}\n**Total Cost: ₯{total_cost:.2f} {currency.replace('_', ' ')}**",
            inline=False
        )
        
        embed.set_footer(text="🤝 May this trade bring barakah to both parties")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error purchasing item.")
        print(f"Buy item error: {e}")

@bot.tree.command(name="government_shop", description="Browse items in the government shop")
async def government_shop(interaction: discord.Interaction, category: str = "all"):
    """Browse the government shop for essential items"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        if category.lower() == "all":
            cursor.execute('''
                SELECT id, item_name, item_type, price, currency, description, is_essential
                FROM government_shop
                ORDER BY is_essential DESC, item_type, item_name
            ''')
        else:
            cursor.execute('''
                SELECT id, item_name, item_type, price, currency, description, is_essential
                FROM government_shop
                WHERE item_type = ?
                ORDER BY is_essential DESC, item_name
            ''', (category.lower(),))
        
        items = cursor.fetchall()
        conn.close()
        
        if not items:
            await interaction.response.send_message("🏛️ Government shop is currently empty. Check back later!")
            return
        
        embed = discord.Embed(
            title="🏛️ Government Shop",
            description=f"Essential items provided by the Islamic government {f'(Category: {category.title()})' if category != 'all' else ''}",
            color=0x0066CC
        )
        
        essential_items = [item for item in items if item[6]]  # is_essential = True
        regular_items = [item for item in items if not item[6]]  # is_essential = False
        
        # Show essential items first
        if essential_items:
            essential_text = ""
            for item_id, item_name, item_type, price, currency, description, is_essential in essential_items[:8]:
                essential_text += f"**{item_name}** ({item_type}) - ₯{price:.2f} {currency.replace('_', ' ')}\nID: {item_id}\n\n"
            
            embed.add_field(
                name="⭐ Essential Items (Always Available)",
                value=essential_text[:1024],
                inline=False
            )
        
        # Show regular items
        if regular_items:
            regular_text = ""
            for item_id, item_name, item_type, price, currency, description, is_essential in regular_items[:8]:
                regular_text += f"**{item_name}** ({item_type}) - ₯{price:.2f} {currency.replace('_', ' ')}\nID: {item_id}\n\n"
            
            embed.add_field(
                name="📦 Regular Items",
                value=regular_text[:1024],
                inline=False
            )
        
        embed.add_field(
            name="💰 How to Purchase",
            value="Use `/buy_from_gov [item_id] [quantity]` to purchase any item!",
            inline=False
        )
        
        embed.add_field(
            name="📋 Categories",
            value="food, clothing, crafts, books, tools, materials, or 'all'",
            inline=False
        )
        
        embed.set_footer(text="🏛️ Government-provided goods • Fair prices for all citizens")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error browsing government shop.")
        print(f"Government shop error: {e}")

@bot.tree.command(name="buy_from_gov", description="Purchase an item from the government shop")
async def buy_from_gov(interaction: discord.Interaction, item_id: int, quantity: int = 1):
    """Buy items from the government shop"""
    
    if quantity <= 0:
        await interaction.response.send_message("❌ Quantity must be positive")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get item details
        cursor.execute('''
            SELECT item_name, item_type, price, currency, description, stock
            FROM government_shop
            WHERE id = ?
        ''', (item_id,))
        
        item = cursor.fetchone()
        if not item:
            await interaction.response.send_message("❌ Item not found in government shop.")
            conn.close()
            return
        
        item_name, item_type, price, currency, description, stock = item
        
        # Check stock (if stock is -1, it's unlimited)
        if stock != -1 and quantity > stock:
            await interaction.response.send_message(f"❌ Only {stock} units available.")
            conn.close()
            return
        
        total_cost = price * quantity
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        
        # Check if buyer has enough money
        if currency == 'gold_dinars' and user_data['gold_dinars'] < total_cost:
            await interaction.response.send_message(f"❌ Insufficient gold dinars. Need ₯{total_cost:.2f}, you have ₯{user_data['gold_dinars']:.2f}")
            conn.close()
            return
        elif currency == 'silver_dirhams' and user_data['silver_dirhams'] < total_cost:
            await interaction.response.send_message(f"❌ Insufficient silver dirhams. Need ₯{total_cost:.2f}, you have ₯{user_data['silver_dirhams']:.2f}")
            conn.close()
            return
        
        # Process transaction
        # Deduct from buyer
        if currency == 'gold_dinars':
            new_balance = user_data['gold_dinars'] - total_cost
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_balance, user_data['user_id']))
        else:
            new_balance = user_data['silver_dirhams'] - total_cost
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_balance, user_data['user_id']))
        
        # Update stock if not unlimited
        if stock != -1:
            new_stock = stock - quantity
            cursor.execute('UPDATE government_shop SET stock = ? WHERE id = ?', (new_stock, item_id))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'government_purchase', total_cost, currency, f'Bought {quantity}x {item_name} from government shop'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🏛️ Government Purchase Successful!",
            description=f"You bought {quantity}x {item_name} from the government shop",
            color=0x0066CC
        )
        
        embed.add_field(
            name="💰 Transaction Details",
            value=f"Item: {item_name} ({item_type})\nQuantity: {quantity}\nUnit Price: ₯{price:.2f}\n**Total Cost: ₯{total_cost:.2f} {currency.replace('_', ' ')}**\nNew Balance: ₯{new_balance:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="📝 Description",
            value=description,
            inline=False
        )
        
        embed.set_footer(text="🏛️ Thank you for supporting the Islamic government")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error purchasing from government shop.")
        print(f"Government purchase error: {e}")


# === CHARITABLE WORK SYSTEM ===
@bot.tree.command(name="volunteer", description="Do volunteer work for community rewards")
async def volunteer(interaction: discord.Interaction, work_type: str, hours: float):
    """Volunteer for community service and earn spiritual + material rewards"""
    
    if hours <= 0 or hours > 8:
        await interaction.response.send_message("❌ Hours must be between 0.1 and 8.0 per session")
        return
    
    volunteer_work = {
        'mosque_cleaning': {'rate': 1.5, 'currency': 'silver_dirhams', 'desc': 'Clean and maintain mosque facilities'},
        'food_distribution': {'rate': 2.0, 'currency': 'silver_dirhams', 'desc': 'Help distribute food to the needy'},
        'elderly_care': {'rate': 2.5, 'currency': 'gold_dinars', 'desc': 'Care for elderly community members'},
        'teaching_children': {'rate': 3.0, 'currency': 'gold_dinars', 'desc': 'Teach Islamic studies to children'},
        'community_garden': {'rate': 1.8, 'currency': 'silver_dirhams', 'desc': 'Maintain community garden (usher bonus)'},
        'charity_organization': {'rate': 2.2, 'currency': 'gold_dinars', 'desc': 'Help organize charity events'},
        'disaster_relief': {'rate': 3.5, 'currency': 'gold_dinars', 'desc': 'Assist with emergency relief efforts'}
    }
    
    if work_type.lower() not in volunteer_work:
        available_work = ', '.join(volunteer_work.keys())
        await interaction.response.send_message(f"❌ Work type not available.\nAvailable: {available_work}")
        return
    
    try:
        work_info = volunteer_work[work_type.lower()]
        base_reward = work_info['rate'] * hours
        currency = work_info['currency']
        
        # Apply agricultural usher bonus if applicable
        if work_type.lower() == 'community_garden':
            base_reward *= (1 + get_agricultural_usher_bonus())
        
        # No taxes on volunteer work (it's charitable)
        reward = base_reward
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        
        # Update user balance
        if currency == 'gold_dinars':
            new_gold = user_data['gold_dinars'] + reward
            cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                         (new_gold, user_data['user_id']))
        else:
            new_silver = user_data['silver_dirhams'] + reward
            cursor.execute('UPDATE users SET silver_dirhams = ? WHERE user_id = ?', 
                         (new_silver, user_data['user_id']))
        
        # Record charity work
        cursor.execute('''
            INSERT INTO charity_work (user_id, work_type, hours_contributed, reward_amount, currency)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], work_type.lower(), hours, reward, currency))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'volunteer_work', reward, currency, f'Volunteer work: {work_type} ({hours}h)'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🤲 Volunteer Work Completed!",
            description=f"Barakallahu feek! Your service to the community is greatly appreciated",
            color=0x00AA00
        )
        
        embed.add_field(
            name="🛠️ Work Details",
            value=f"Type: {work_type.replace('_', ' ').title()}\nHours: {hours}\nDescription: {work_info['desc']}",
            inline=False
        )
        
        embed.add_field(
            name="💰 Community Appreciation",
            value=f"Rate: ₯{work_info['rate']:.1f} per hour\n{'🌾 Agricultural Usher Bonus Applied!' if work_type.lower() == 'community_garden' else ''}\n**Total Reward: ₯{reward:.2f} {currency.replace('_', ' ')}**",
            inline=False
        )
        
        embed.add_field(
            name="📿 Spiritual Benefits",
            value="✅ Building community bonds\n✅ Earning spiritual rewards\n✅ Following Islamic values\n✅ No taxes on volunteer work!",
            inline=False
        )
        
        embed.set_footer(text="🌟 Helping others is the best way to help yourself • Service is worship")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error recording volunteer work.")
        print(f"Volunteer error: {e}")


# === PILGRIMAGE SAVINGS SYSTEM ===
@bot.tree.command(name="start_hajj_savings", description="Start saving for Hajj pilgrimage")
async def start_hajj_savings(interaction: discord.Interaction, target_amount: float):
    """Begin saving for the sacred journey to Mecca"""
    
    if target_amount < 100:
        await interaction.response.send_message("❌ Hajj target amount should be at least ₯100.0 gold dinars")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if user already has an active Hajj savings
        cursor.execute('''
            SELECT id FROM pilgrimage_savings 
            WHERE user_id = ? AND pilgrimage_type = 'hajj' AND status = 'saving'
        ''', (str(interaction.user.id),))
        
        if cursor.fetchone():
            await interaction.response.send_message("❌ You already have an active Hajj savings plan. Use `/pilgrimage_status` to check progress.")
            conn.close()
            return
        
        # Create savings plan
        cursor.execute('''
            INSERT INTO pilgrimage_savings (user_id, pilgrimage_type, target_amount, currency)
            VALUES (?, ?, ?, ?)
        ''', (str(interaction.user.id), 'hajj', target_amount, 'gold_dinars'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🕋 Hajj Savings Started!",
            description="May Allah make it easy for you to complete this sacred journey",
            color=0x00AA00
        )
        
        embed.add_field(
            name="🎯 Savings Goal",
            value=f"Target: ₯{target_amount:.2f} gold dinars\nSaved: ₯0.00\nRemaining: ₯{target_amount:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="💡 How to Save",
            value="• Use `/save_for_hajj [amount]` to add money\n• 10% of all your earnings can be auto-saved\n• Special Hajj-related tasks will become available",
            inline=False
        )
        
        embed.add_field(
            name="📿 Spiritual Benefits",
            value="Saving for Hajj shows intention and commitment\nAllah will make the path easier for sincere pilgrims\nThis sacred goal brings barakah to your wealth",
            inline=False
        )
        
        embed.set_footer(text="🕋 Hajj Mabroor! • May your pilgrimage be accepted")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error starting Hajj savings.")
        print(f"Hajj savings error: {e}")


@bot.tree.command(name="save_for_hajj", description="Add money to your Hajj savings")
async def save_for_hajj(interaction: discord.Interaction, amount: float):
    """Add money to your pilgrimage savings"""
    
    if amount <= 0:
        await interaction.response.send_message("❌ Savings amount must be positive")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check for active Hajj savings
        cursor.execute('''
            SELECT id, target_amount, saved_amount FROM pilgrimage_savings 
            WHERE user_id = ? AND pilgrimage_type = 'hajj' AND status = 'saving'
        ''', (str(interaction.user.id),))
        
        savings = cursor.fetchone()
        if not savings:
            await interaction.response.send_message("❌ No active Hajj savings plan. Use `/start_hajj_savings` first.")
            conn.close()
            return
        
        savings_id, target, saved = savings
        
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        
        if user_data['gold_dinars'] < amount:
            await interaction.response.send_message(f"❌ Insufficient gold dinars. You have ₯{user_data['gold_dinars']:.2f}")
            conn.close()
            return
        
        # Transfer money to savings
        new_balance = user_data['gold_dinars'] - amount
        new_saved = saved + amount
        
        cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                     (new_balance, user_data['user_id']))
        
        cursor.execute('UPDATE pilgrimage_savings SET saved_amount = ? WHERE id = ?', 
                     (new_saved, savings_id))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'hajj_savings', amount, 'gold_dinars', f'Hajj savings contribution'))
        
        # Check if goal reached
        if new_saved >= target:
            cursor.execute('UPDATE pilgrimage_savings SET status = "ready" WHERE id = ?', (savings_id,))
            goal_reached = True
        else:
            goal_reached = False
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="🕋 Hajj Savings Updated!",
            description="May Allah accept your sincere intention and efforts",
            color=0x00AA00
        )
        
        progress_percent = min((new_saved / target) * 100, 100)
        remaining = max(target - new_saved, 0)
        
        embed.add_field(
            name="💰 Savings Progress",
            value=f"Added: ₯{amount:.2f}\nTotal Saved: ₯{new_saved:.2f}\nTarget: ₯{target:.2f}\nRemaining: ₯{remaining:.2f}\n**Progress: {progress_percent:.1f}%**",
            inline=False
        )
        
        if goal_reached:
            embed.add_field(
                name="🎉 Goal Achieved!",
                value="Alhamdulillah! You've reached your Hajj savings goal!\nYou're now ready to plan your sacred journey to Mecca.\nMay Allah make your pilgrimage easy and accepted.",
                inline=False
            )
            embed.color = 0xFFD700  # Gold color for achievement
        
        embed.set_footer(text="🌙 Every step towards Hajj is blessed • Tawfeeq from Allah")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error saving for Hajj.")
        print(f"Save for Hajj error: {e}")


# === SKILLS AND FREELANCING SYSTEM ===
@bot.tree.command(name="learn_skill", description="Learn a new skill to unlock freelancing opportunities")
async def learn_skill(interaction: discord.Interaction, skill_name: str):
    """Learn new skills to expand your income opportunities"""
    
    available_skills = {
        'arabic_calligraphy': {'cost': 15.0, 'desc': 'Beautiful Islamic art form', 'earnings': '3-8 gold/hour'},
        'quran_recitation': {'cost': 20.0, 'desc': 'Master proper Quranic pronunciation', 'earnings': '4-10 gold/hour'},
        'islamic_teaching': {'cost': 25.0, 'desc': 'Educate others about Islam', 'earnings': '5-12 gold/hour'},
        'halal_cooking': {'cost': 18.0, 'desc': 'Prepare delicious halal meals', 'earnings': '3-7 gold/hour'},
        'textile_weaving': {'cost': 22.0, 'desc': 'Create beautiful Islamic textiles', 'earnings': '4-9 gold/hour'},
        'gemstone_cutting': {'cost': 30.0, 'desc': 'Craft exquisite jewelry', 'earnings': '6-15 gold/hour'},
        'herbal_medicine': {'cost': 28.0, 'desc': 'Traditional healing knowledge', 'earnings': '5-13 gold/hour'},
        'islamic_law': {'cost': 35.0, 'desc': 'Understanding of Shariah principles', 'earnings': '7-18 gold/hour'},
        'architecture': {'cost': 40.0, 'desc': 'Design Islamic buildings', 'earnings': '8-20 gold/hour'},
        'translation': {'cost': 25.0, 'desc': 'Translate between languages', 'earnings': '4-11 gold/hour'}
    }
    
    if skill_name.lower() not in available_skills:
        skills_list = ', '.join(available_skills.keys())
        await interaction.response.send_message(f"❌ Skill not available.\nAvailable skills: {skills_list}")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if user already has this skill
        cursor.execute('''
            SELECT skill_level FROM user_skills 
            WHERE user_id = ? AND skill_name = ?
        ''', (str(interaction.user.id), skill_name.lower()))
        
        existing_skill = cursor.fetchone()
        if existing_skill:
            await interaction.response.send_message(f"❌ You already have {skill_name} (Level {existing_skill[0]}). Use `/practice_skill` to improve it.")
            conn.close()
            return
        
        skill_info = available_skills[skill_name.lower()]
        cost = skill_info['cost']
        
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        
        if user_data['gold_dinars'] < cost:
            await interaction.response.send_message(f"❌ Insufficient funds. Learning {skill_name} costs ₯{cost:.1f} gold dinars. You have ₯{user_data['gold_dinars']:.2f}")
            conn.close()
            return
        
        # Deduct cost and add skill
        new_balance = user_data['gold_dinars'] - cost
        cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                     (new_balance, user_data['user_id']))
        
        cursor.execute('''
            INSERT INTO user_skills (user_id, skill_name, skill_level, experience_points)
            VALUES (?, ?, ?, ?)
        ''', (user_data['user_id'], skill_name.lower(), 1, 0))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'skill_learning', cost, 'gold_dinars', f'Learned skill: {skill_name}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="📚 Skill Learned Successfully!",
            description=f"Barakallahu feek! You've learned: **{skill_name.replace('_', ' ').title()}**",
            color=0x00AA00
        )
        
        embed.add_field(
            name="🎯 Skill Details",
            value=f"Level: 1 (Beginner)\nDescription: {skill_info['desc']}\nEarning Potential: {skill_info['earnings']}",
            inline=False
        )
        
        embed.add_field(
            name="💰 Investment",
            value=f"Learning Cost: ₯{cost:.1f} gold dinars\nRemaining Balance: ₯{new_balance:.2f}",
            inline=False
        )
        
        embed.add_field(
            name="💡 Next Steps",
            value="• Use `/practice_skill` to improve your level\n• Use `/freelance_work` to earn with this skill\n• Use `/my_skills` to see all your abilities",
            inline=False
        )
        
        embed.set_footer(text="🌟 Knowledge is light • Continuous learning brings prosperity")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error learning skill.")
        print(f"Learn skill error: {e}")


@bot.tree.command(name="freelance_work", description="Use your skills to earn money through freelancing")
async def freelance_work(interaction: discord.Interaction, skill_name: str, hours: float):
    """Freelance using your learned skills"""
    
    if hours <= 0 or hours > 6:
        await interaction.response.send_message("❌ Work hours must be between 0.1 and 6.0 per session")
        return
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Check if user has this skill
        cursor.execute('''
            SELECT skill_level, experience_points FROM user_skills 
            WHERE user_id = ? AND skill_name = ?
        ''', (str(interaction.user.id), skill_name.lower()))
        
        skill_data = cursor.fetchone()
        if not skill_data:
            await interaction.response.send_message(f"❌ You don't have the skill '{skill_name}'. Use `/learn_skill` first.")
            conn.close()
            return
        
        skill_level, experience = skill_data
        
        # Calculate earnings based on skill level
        base_rates = {
            'arabic_calligraphy': 3.0, 'quran_recitation': 4.0, 'islamic_teaching': 5.0,
            'halal_cooking': 3.0, 'textile_weaving': 4.0, 'gemstone_cutting': 6.0,
            'herbal_medicine': 5.0, 'islamic_law': 7.0, 'architecture': 8.0, 'translation': 4.0
        }
        
        base_rate = base_rates.get(skill_name.lower(), 3.0)
        skill_multiplier = 1 + (skill_level - 1) * 0.3  # 30% increase per level
        hourly_rate = base_rate * skill_multiplier
        
        gross_earnings = hourly_rate * hours
        taxes = calculate_taxes(gross_earnings, 'gold_dinars')
        net_earnings = gross_earnings - taxes
        
        # Add experience points
        exp_gained = int(hours * 10)  # 10 exp per hour
        new_experience = experience + exp_gained
        
        # Check for level up (every 100 experience points)
        new_level = skill_level
        if new_experience >= skill_level * 100:
            new_level = skill_level + 1
            new_experience = new_experience - (skill_level * 100)
        
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        new_gold = user_data['gold_dinars'] + net_earnings
        
        # Update database
        cursor.execute('UPDATE users SET gold_dinars = ? WHERE user_id = ?', 
                     (new_gold, user_data['user_id']))
        
        cursor.execute('''
            UPDATE user_skills SET skill_level = ?, experience_points = ? 
            WHERE user_id = ? AND skill_name = ?
        ''', (new_level, new_experience, user_data['user_id'], skill_name.lower()))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'freelance_work', net_earnings, 'gold_dinars', f'Freelance: {skill_name} ({hours}h)'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="💼 Freelance Work Completed!",
            description=f"Excellent work with your {skill_name.replace('_', ' ')} skills!",
            color=0x00AA00
        )
        
        embed.add_field(
            name="💰 Earnings",
            value=f"Hours Worked: {hours}\nHourly Rate: ₯{hourly_rate:.2f}\nGross: ₯{gross_earnings:.2f}\nTaxes: ₯{taxes:.2f}\n**Net: ₯{net_earnings:.2f} gold dinars**",
            inline=False
        )
        
        embed.add_field(
            name="📈 Skill Progress",
            value=f"Skill Level: {skill_level} → {new_level}{'🎉 Level Up!' if new_level > skill_level else ''}\nExperience: +{exp_gained} points\nSkill Multiplier: {skill_multiplier:.1f}x",
            inline=False
        )
        
        embed.set_footer(text="🎯 Practice makes perfect • Your skills grow with dedication")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error completing freelance work.")
        print(f"Freelance work error: {e}")


@bot.tree.command(name="my_skills", description="View all your learned skills and levels")
async def my_skills(interaction: discord.Interaction):
    """View your skill portfolio and freelancing capabilities"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT skill_name, skill_level, experience_points, learned_date 
            FROM user_skills 
            WHERE user_id = ?
            ORDER BY skill_level DESC, experience_points DESC
        ''', (str(interaction.user.id),))
        
        skills = cursor.fetchall()
        conn.close()
        
        if not skills:
            embed = discord.Embed(
                title="📚 Your Skills Portfolio",
                description="You haven't learned any skills yet. Use `/learn_skill` to start building your expertise!",
                color=0x888888
            )
            
            embed.add_field(
                name="💡 Available Skills",
                value="arabic_calligraphy, quran_recitation, islamic_teaching, halal_cooking, textile_weaving, gemstone_cutting, herbal_medicine, islamic_law, architecture, translation",
                inline=False
            )
        else:
            embed = discord.Embed(
                title="📚 Your Skills Portfolio",
                description=f"You have mastered {len(skills)} valuable skills!",
                color=0x0066CC
            )
            
            for skill_name, level, exp, learned_date in skills:
                # Calculate potential earnings
                base_rates = {
                    'arabic_calligraphy': 3.0, 'quran_recitation': 4.0, 'islamic_teaching': 5.0,
                    'halal_cooking': 3.0, 'textile_weaving': 4.0, 'gemstone_cutting': 6.0,
                    'herbal_medicine': 5.0, 'islamic_law': 7.0, 'architecture': 8.0, 'translation': 4.0
                }
                
                base_rate = base_rates.get(skill_name, 3.0)
                skill_multiplier = 1 + (level - 1) * 0.3
                hourly_rate = base_rate * skill_multiplier
                
                next_level_exp = level * 100
                
                embed.add_field(
                    name=f"🎯 {skill_name.replace('_', ' ').title()}",
                    value=f"Level: {level}\nEarning Rate: ₯{hourly_rate:.2f}/hour\nExp: {exp}/{next_level_exp}\nLearned: {learned_date[:10]}",
                    inline=True
                )
            
            embed.add_field(
                name="💼 Freelancing",
                value="Use `/freelance_work [skill] [hours]` to earn money with your skills!",
                inline=False
            )
        
        embed.set_footer(text="🌟 Knowledge is power • Skills are your greatest investment")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error loading skills.")
        print(f"My skills error: {e}")


# === ACHIEVEMENT SYSTEM ===
@bot.tree.command(name="achievements", description="View your accomplishments and progress")
async def achievements(interaction: discord.Interaction):
    """View your Islamic economy achievements and milestones"""
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get user's earned achievements
        cursor.execute('''
            SELECT achievement_name, achievement_description, earned_date, reward_amount 
            FROM achievements 
            WHERE user_id = ?
            ORDER BY earned_date DESC
        ''', (str(interaction.user.id),))
        
        earned_achievements = cursor.fetchall()
        
        # Get user stats for potential achievements
        user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
        
        cursor.execute('SELECT COUNT(*) FROM transactions WHERE user_id = ? AND transaction_type = "charity"', (str(interaction.user.id),))
        charity_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM businesses WHERE owner_id = ?', (str(interaction.user.id),))
        business_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM user_skills WHERE user_id = ?', (str(interaction.user.id),))
        skills_count = cursor.fetchone()[0]
        
        conn.close()
        
        embed = discord.Embed(
            title="🏆 Your Achievements",
            description="Celebrating your journey in the Islamic economy!",
            color=0xFFD700
        )
        
        if earned_achievements:
            achievement_list = []
            total_rewards = 0
            for name, desc, date, reward in earned_achievements[:8]:  # Show latest 8
                achievement_list.append(f"🏅 **{name}**\n   {desc}\n   Earned: {date[:10]} • Reward: ₯{reward:.1f}")
                total_rewards += reward
            
            embed.add_field(
                name="🎉 Earned Achievements",
                value="\n\n".join(achievement_list),
                inline=False
            )
            
            embed.add_field(
                name="💰 Total Achievement Rewards",
                value=f"₯{total_rewards:.2f} gold dinars earned from achievements!",
                inline=False
            )
        
        # Show potential achievements
        potential = []
        earned_names = [ach[0] for ach in earned_achievements]
        
        if user_data['total_wealth'] >= 500 and 'Wealthy Merchant' not in earned_names:
            potential.append("💰 **Wealthy Merchant** - Accumulate 500+ total wealth")
        
        if charity_count >= 10 and 'Generous Heart' not in earned_names:
            potential.append("🤲 **Generous Heart** - Give charity 10+ times")
        
        if business_count >= 3 and 'Business Empire' not in earned_names:
            potential.append("🏢 **Business Empire** - Own 3+ businesses")
        
        if skills_count >= 5 and 'Master Learner' not in earned_names:
            potential.append("📚 **Master Learner** - Learn 5+ skills")
        
        if potential:
            embed.add_field(
                name="🎯 Potential Achievements",
                value="\n".join(potential[:5]),
                inline=False
            )
        
        if not earned_achievements and not potential:
            embed.add_field(
                name="🌱 Getting Started",
                value="Start your journey by:\n• Working and earning money\n• Giving charity\n• Starting a business\n• Learning new skills",
                inline=False
            )
        
        embed.set_footer(text="🌟 Every achievement tells a story • Keep building your legacy")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error loading achievements.")
        print(f"Achievements error: {e}")


# === ISLAMIC CALENDAR EVENTS ===
@bot.tree.command(name="islamic_calendar", description="View Islamic calendar events and special bonuses")
async def islamic_calendar(interaction: discord.Interaction):
    """View current Islamic calendar events and seasonal bonuses"""
    
    import datetime
    
    # Get current Hijri month approximation (this is simplified)
    months = [
        "Muharram", "Safar", "Rabi' al-awwal", "Rabi' al-thani",
        "Jumada al-awwal", "Jumada al-thani", "Rajab", "Sha'ban",
        "Ramadan", "Shawwal", "Dhu al-Qi'dah", "Dhu al-Hijjah"
    ]
    
    current_month = datetime.datetime.now().month
    islamic_month = months[(current_month - 1) % 12]
    
    try:
        embed = discord.Embed(
            title="🌙 Islamic Calendar Events",
            description=f"Current Month: **{islamic_month}**",
            color=0x4CAF50
        )
        
        # Month-specific bonuses and events
        monthly_events = {
            "Muharram": {
                "bonus": "10% bonus on charity giving",
                "event": "Islamic New Year celebrations",
                "special": "Day of Ashura remembrance",
                "tasks": ["extra_charity", "community_reflection"]
            },
            "Rabi' al-awwal": {
                "bonus": "15% bonus on Islamic education",
                "event": "Prophet's Birthday celebrations",
                "special": "Increased rewards for teaching",
                "tasks": ["prophet_study", "teaching_bonus"]
            },
            "Rajab": {
                "bonus": "20% bonus on pilgrimage savings",
                "event": "Sacred month preparations",
                "special": "Increased spiritual focus",
                "tasks": ["hajj_preparation", "spiritual_growth"]
            },
            "Sha'ban": {
                "bonus": "15% bonus on community work",
                "event": "Ramadan preparation month",
                "special": "Community building emphasis",
                "tasks": ["community_service", "ramadan_prep"]
            },
            "Ramadan": {
                "bonus": "25% bonus on ALL activities",
                "event": "Holy month of fasting",
                "special": "Maximum spiritual rewards",
                "tasks": ["iftar_preparation", "night_prayers", "charity_emphasis"]
            },
            "Shawwal": {
                "bonus": "20% bonus on celebration activities",
                "event": "Eid al-Fitr celebrations",
                "special": "Community joy and sharing",
                "tasks": ["eid_celebration", "family_gathering"]
            },
            "Dhu al-Hijjah": {
                "bonus": "30% bonus on Hajj-related activities",
                "event": "Hajj pilgrimage season",
                "special": "Peak spiritual season",
                "tasks": ["hajj_completion", "sacrifice_preparation", "pilgrimage_support"]
            }
        }
        
        default_event = {
            "bonus": "5% bonus on daily tasks",
            "event": "Regular blessed month",
            "special": "Consistent spiritual growth",
            "tasks": ["daily_consistency", "regular_worship"]
        }
        
        current_event = monthly_events.get(islamic_month, default_event)
        
        embed.add_field(
            name="🎁 Current Month Bonus",
            value=current_event["bonus"],
            inline=False
        )
        
        embed.add_field(
            name="🎉 Special Events",
            value=f"• {current_event['event']}\n• {current_event['special']}",
            inline=False
        )
        
        embed.add_field(
            name="📅 Weekly Schedule",
            value="• **Friday**: 20% bonus on mosque activities\n• **Saturday**: Community service emphasis\n• **Sunday**: Family and education focus",
            inline=False
        )
        
        embed.add_field(
            name="🌟 Seasonal Activities",
            value="• Use `/seasonal_work` for special opportunities\n• Check `/daily_tasks` for monthly specials\n• Extra rewards during sacred months",
            inline=False
        )
        
        embed.add_field(
            name="💡 Tips",
            value="• Islamic calendar bonuses stack with other bonuses\n• Sacred months offer the highest rewards\n• Plan your activities around special events",
            inline=False
        )
        
        embed.set_footer(text="🕌 Blessed times bring blessed opportunities • Follow the sacred calendar")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error loading Islamic calendar.")
        print(f"Islamic calendar error: {e}")


@bot.tree.command(name="seasonal_work", description="Take on special seasonal work opportunities")
async def seasonal_work(interaction: discord.Interaction):
    """Participate in seasonal Islamic work with special bonuses"""
    
    import datetime
    
    current_month = datetime.datetime.now().month
    current_day = datetime.datetime.now().day
    
    # Determine seasonal opportunities
    seasonal_jobs = []
    
    # Ramadan season (month 9 in Islamic calendar, roughly corresponds to various Gregorian months)
    if current_month in [4, 5, 6]:  # Spring months for this example
        seasonal_jobs.extend([
            {'name': 'iftar_preparation', 'reward': 8.0, 'desc': 'Prepare iftar meals for community'},
            {'name': 'quran_teaching', 'reward': 10.0, 'desc': 'Teach Quran during sacred time'},
            {'name': 'charity_collection', 'reward': 6.0, 'desc': 'Organize Zakat and charity drives'}
        ])
    
    # Hajj season
    if current_month in [7, 8]:  # Summer months
        seasonal_jobs.extend([
            {'name': 'pilgrimage_guide', 'reward': 15.0, 'desc': 'Guide pilgrims in Islamic practices'},
            {'name': 'hajj_logistics', 'reward': 12.0, 'desc': 'Assist with pilgrimage preparations'},
            {'name': 'sacrifice_coordination', 'reward': 10.0, 'desc': 'Help coordinate Eid sacrifices'}
        ])
    
    # Winter preparation
    if current_month in [10, 11, 12]:
        seasonal_jobs.extend([
            {'name': 'winter_relief', 'reward': 9.0, 'desc': 'Help prepare winter supplies for needy'},
            {'name': 'soup_kitchen', 'reward': 7.0, 'desc': 'Serve hot meals to community'},
            {'name': 'warm_clothing', 'reward': 8.0, 'desc': 'Distribute warm clothing to poor'}
        ])
    
    # Default community work
    if not seasonal_jobs:
        seasonal_jobs = [
            {'name': 'community_garden', 'reward': 6.0, 'desc': 'Maintain community food garden'},
            {'name': 'youth_education', 'reward': 7.0, 'desc': 'Teach Islamic values to youth'},
            {'name': 'elderly_support', 'reward': 8.0, 'desc': 'Support elderly community members'}
        ]
    
    try:
        embed = discord.Embed(
            title="🌟 Seasonal Work Opportunities",
            description="Special community work available this season!",
            color=0xFF6B35
        )
        
        for job in seasonal_jobs:
            embed.add_field(
                name=f"🛠️ {job['name'].replace('_', ' ').title()}",
                value=f"Reward: ₯{job['reward']:.1f} gold dinars\nDescription: {job['desc']}\nUse: `/do_seasonal [job_name]`",
                inline=True
            )
        
        embed.add_field(
            name="🎁 Seasonal Bonuses",
            value="• 25% bonus during sacred months\n• Extra community appreciation\n• Special achievement progress\n• No taxes on seasonal community work",
            inline=False
        )
        
        embed.set_footer(text="🤝 Seasonal service builds stronger communities • Adapt to the sacred calendar")
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error loading seasonal work.")
        print(f"Seasonal work error: {e}")


# === CURRENCY EXCHANGE SYSTEM ===
@bot.tree.command(name="exchange", description="Exchange between gold dinars and silver dirhams")
async def currency_exchange(interaction: discord.Interaction, amount: float, from_currency: str, to_currency: str):
    """Exchange currencies with current rates"""
    
    if from_currency.lower() not in ['gold', 'silver'] or to_currency.lower() not in ['gold', 'silver']:
        await interaction.response.send_message("❌ Currencies must be 'gold' or 'silver'")
        return
    
    if from_currency.lower() == to_currency.lower():
        await interaction.response.send_message("❌ Cannot exchange same currency")
        return
    
    if amount <= 0:
        await interaction.response.send_message("❌ Exchange amount must be positive")
        return
    
    user_data = get_user_account(str(interaction.user.id), interaction.user.display_name or interaction.user.name)
    exchange_rate = get_exchange_rate()
    
    # Calculate exchange
    if from_currency.lower() == 'gold':
        # Gold to Silver
        if user_data['gold_dinars'] < amount:
            await interaction.response.send_message(f"❌ Insufficient gold dinars. You have ₯{user_data['gold_dinars']:.2f}")
            return
        
        received_amount = amount * exchange_rate
        new_gold = user_data['gold_dinars'] - amount
        new_silver = user_data['silver_dirhams'] + received_amount
        
    else:
        # Silver to Gold  
        if user_data['silver_dirhams'] < amount:
            await interaction.response.send_message(f"❌ Insufficient silver dirhams. You have ₯{user_data['silver_dirhams']:.2f}")
            return
        
        received_amount = amount / exchange_rate
        new_gold = user_data['gold_dinars'] + received_amount
        new_silver = user_data['silver_dirhams'] - amount
    
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.execute('PRAGMA foreign_keys = ON')
        cursor = conn.cursor()
        
        # Update balances
        cursor.execute('''
            UPDATE users SET gold_dinars = ?, silver_dirhams = ? WHERE user_id = ?
        ''', (new_gold, new_silver, user_data['user_id']))
        
        # Record transaction
        cursor.execute('''
            INSERT INTO transactions (user_id, transaction_type, amount, currency, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_data['user_id'], 'currency_exchange', amount, f'{from_currency.lower()}_to_{to_currency.lower()}', f'Exchanged {amount:.2f} {from_currency} for {received_amount:.2f} {to_currency}'))
        
        conn.commit()
        conn.close()
        
        embed = discord.Embed(
            title="💱 Currency Exchange Successful",
            description="Your currency exchange has been completed!",
            color=0x00AA00
        )
        
        embed.add_field(
            name="🔄 Exchange Details",
            value=f"Exchanged: ₯{amount:.2f} {from_currency.title()}\nReceived: ₯{received_amount:.2f} {to_currency.title()}\nRate: 1 Gold = {exchange_rate} Silver",
            inline=False
        )
        
        embed.add_field(
            name="💰 New Balances",
            value=f"Gold Dinars: ₯{new_gold:.2f}\nSilver Dirhams: ₯{new_silver:.2f}",
            inline=False
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
        
    except Exception as e:
        await interaction.response.send_message("❌ Error processing exchange. Please try again later.")
        print(f"Exchange error: {e}")

@bot.tree.command(name="exchange_rates", description="View current currency exchange rates")
async def exchange_rates(interaction: discord.Interaction):
    """Show current exchange rates and silver dirham uses"""
    rate = get_exchange_rate()
    
    embed = discord.Embed(
        title="💱 Currency Exchange Information",
        description="Current exchange rates and currency uses",
        color=0xFFAA00
    )
    
    embed.add_field(
        name="📈 Exchange Rates",
        value=f"1 Gold Dinar = {rate} Silver Dirhams\n1 Silver Dirham = {1/rate:.4f} Gold Dinars",
        inline=False
    )
    
    embed.add_field(
        name="🥇 Gold Dinars - Premium Currency",
        value="• High-value investments\n• Business startup costs\n• Premium halal products\n• Major zakat payments",
        inline=True
    )
    
    embed.add_field(
        name="🥈 Silver Dirhams - Daily Currency", 
        value="• Daily job wages\n• Small trades\n• Local market purchases\n• Micro-investments\n• Charity donations",
        inline=True
    )
    
    embed.add_field(
        name="💡 Exchange Tips",
        value="• Exchange to silver for daily expenses\n• Exchange to gold for major investments\n• Rates based on historical Islamic standards",
        inline=False
    )
    
    await interaction.response.send_message(embed=embed)


# Error handling
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send(
            "Command not found. Use slash commands like `/account` or `/help`."
        )
    else:
        print(f"Error: {error}")
        await ctx.send("An error occurred while processing the command.")


# Run the bot
if __name__ == "__main__":
    discord_token = os.getenv('DISCORD_BOT_TOKEN')

    if not discord_token:
        print("Error: DISCORD_BOT_TOKEN not found in environment variables!")
        print("Please set your Discord bot token in the .env file.")
        exit(1)

    print("Starting DigiCap - Islamic Economy Discord Bot...")
    
    # Start UptimeRobot keep-alive server
    keep_alive()
    print("UptimeRobot keep-alive server started on port 8080")
    
    # Start Flask API server in separate thread for Minecraft integration
    flask_thread = threading.Thread(target=run_flask_app, daemon=True)
    flask_thread.start()
    print("Flask API server started on port 5000 for Minecraft integration")
    
    bot.run(discord_token)
