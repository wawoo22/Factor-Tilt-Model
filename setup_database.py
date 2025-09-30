import sqlite3
import os
from datetime import datetime

def create_database():
    db_path = "factor_monitoring.db"
    
    # Remove existing database for fresh start
    if os.path.exists(db_path):
        backup_name = f"factor_monitoring_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        os.rename(db_path, backup_name)
        print(f"Existing database backed up as {backup_name}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create all required tables
    tables = [
        '''CREATE TABLE factor_prices (
            date TEXT,
            symbol TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (date, symbol)
        )''',
        
        '''CREATE TABLE factor_returns (
            date TEXT,
            symbol TEXT,
            daily_return REAL,
            cumulative_return REAL,
            PRIMARY KEY (date, symbol)
        )''',
        
        '''CREATE TABLE factor_metrics (
            date TEXT PRIMARY KEY,
            value_growth_spread REAL,
            small_large_spread REAL,
            quality_junk_spread REAL,
            momentum_reversal_spread REAL,
            volatility_regime INTEGER,
            market_beta REAL
        )''',
        
        '''CREATE TABLE alerts_log (
            timestamp TEXT,
            alert_type TEXT,
            factor TEXT,
            message TEXT,
            severity TEXT,
            resolved BOOLEAN DEFAULT FALSE
        )''',
        
        '''CREATE TABLE trade_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            trade_id TEXT UNIQUE NOT NULL,
            factor TEXT NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            quantity REAL NOT NULL,
            price REAL NOT NULL,
            total_value REAL NOT NULL,
            reason TEXT NOT NULL,
            portfolio_value_before REAL,
            portfolio_value_after REAL,
            user_id TEXT DEFAULT 'system',
            hash_signature TEXT,
            created_date TEXT DEFAULT CURRENT_TIMESTAMP
        )''',
        
        '''CREATE TABLE attribution_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            total_return REAL,
            benchmark_return REAL,
            excess_return REAL,
            factor_contributions TEXT,
            sector_contributions TEXT,
            selection_effect REAL,
            allocation_effect REAL,
            interaction_effect REAL,
            risk_metrics TEXT,
            created_date TEXT DEFAULT CURRENT_TIMESTAMP
        )'''
    ]
    
    for table_sql in tables:
        cursor.execute(table_sql)
        print(f"✅ Created table: {table_sql.split('(')[0].replace('CREATE TABLE ', '')}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Database created successfully: {db_path}")
    return True

if __name__ == "__main__":
    create_database()
