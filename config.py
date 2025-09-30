import os
from datetime import datetime, timedelta

class Config:
    """Configuration settings for the factor investment system"""
    
    # Schwab API Settings
    SCHWAB_CLIENT_ID = os.getenv('SCHWAB_CLIENT_ID')
    SCHWAB_REFRESH_TOKEN = os.getenv('SCHWAB_REFRESH_TOKEN')
    SCHWAB_BASE_URL = "https://api.schwabapi.com"
    
    # Yahoo Finance Settings
    YAHOO_DATA_PERIOD = "2y"
    YAHOO_RETRY_ATTEMPTS = 3
    YAHOO_DELAY_BETWEEN_CALLS = 0.1
    
    # Factor Calculation Settings
    MOMENTUM_LOOKBACK = 252
    MOMENTUM_SKIP = 22
    VOLATILITY_WINDOW = 60
    
    # Portfolio Settings
    MAX_POSITION_SIZE = 0.10
    MIN_POSITION_SIZE = 0.001
    TARGET_PORTFOLIO_SIZE = 30  # Reduced for faster testing
    REBALANCE_THRESHOLD = 0.05
    
    # Factor Weights
    FACTOR_WEIGHTS = {
        'momentum': 0.30,
        'value_pe': 0.25,
        'value_pb': 0.15,
        'quality_roe': 0.30
    }
    
    # Smaller stock universe for testing
    STOCK_UNIVERSE = [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM',
        'JNJ', 'V', 'WMT', 'PG', 'UNH', 'HD', 'MA', 'DIS', 'PYPL', 'BAC',
        'NFLX', 'ADBE', 'CRM', 'KO', 'PFE', 'INTC', 'CSCO', 'PEP'
    ]
    
    # Data paths
    DATA_DIR = "data"
    OUTPUT_DIR = "output"
    LOG_DIR = "logs"
   
    # Schwab API Settings
    SCHWAB_CLIENT_ID = os.getenv('SCHWAB_CLIENT_ID')
    SCHWAB_REFRESH_TOKEN = os.getenv('SCHWAB_REFRESH_TOKEN')
    SCHWAB_CLIENT_SECRET = os.getenv('SCHWAB_CLIENT_SECRET')  # Add this line
    SCHWAB_BASE_URL = "https://api.schwabapi.com"
 
    @classmethod
    def validate_config(cls):
        """Validate configuration settings"""
        os.makedirs(cls.DATA_DIR, exist_ok=True)
        os.makedirs(cls.OUTPUT_DIR, exist_ok=True)
        os.makedirs(cls.LOG_DIR, exist_ok=True)
        print("✅ Configuration validated and directories created")
        return True
EOF
