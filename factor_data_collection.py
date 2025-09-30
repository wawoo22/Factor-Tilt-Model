import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import requests
import json
import sqlite3
import logging
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('factor_monitoring.log'),
        logging.StreamHandler()
    ]
)

class FactorDataCollector:
    """
    Comprehensive factor data collection system for cost-effective monitoring
    """
    
    def __init__(self, db_path: str = "factor_data.db"):
        self.db_path = db_path
        self.factor_etfs = {
            'Value': 'VTV',
            'Growth': 'VUG',
            'Quality': 'QUAL',
            'Momentum': 'MTUM',
            'Low_Volatility': 'USMV',
            'Size': 'VB',
            'Market': 'SPY',
            'Small_Cap': 'IWM',
            'High_Dividend': 'VYM'
        }
        
        self.economic_indicators = {
            'VIX': '^VIX',
            'TNX': '^TNX',  # 10-Year Treasury
            'TYX': '^TYX',  # 30-Year Treasury
            'DXY': 'DX-Y.NYB'  # Dollar Index
        }
        
        self.alpha_vantage_key = "YOUR_ALPHA_VANTAGE_KEY"  # Free tier available
        self.initialize_database()
        
    def initialize_database(self):
        """Initialize SQLite database for data storage"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS factor_prices (
                    date TEXT,
                    symbol TEXT,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume INTEGER,
                    PRIMARY KEY (date, symbol)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS factor_returns (
                    date TEXT,
                    symbol TEXT,
                    daily_return REAL,
                    cumulative_return REAL,
                    PRIMARY KEY (date, symbol)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS factor_metrics (
                    date TEXT PRIMARY KEY,
                    value_growth_spread REAL,
                    small_large_spread REAL,
                    quality_junk_spread REAL,
                    momentum_reversal_spread REAL,
                    volatility_regime INTEGER,
                    market_beta REAL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS alerts_log (
                    timestamp TEXT,
                    alert_type TEXT,
                    factor TEXT,
                    message TEXT,
                    severity TEXT,
                    resolved BOOLEAN DEFAULT FALSE
                )
            ''')
            
            conn.commit()
            conn.close()
            logging.info("Database initialized successfully")
            
        except Exception as e:
            logging.error(f"Database initialization error: {e}")
    
    def collect_factor_data(self, period: str = "1y") -> pd.DataFrame:
        """Collect factor ETF price data"""
        try:
            all_data = {}
            
            # Collect ETF data
            for factor_name, ticker in self.factor_etfs.items():
                try:
                    stock = yf.Ticker(ticker)
                    hist = stock.history(period=period)
                    if not hist.empty:
                        all_data[f"{factor_name}_{ticker}"] = hist['Close']
                        logging.info(f"Collected data for {factor_name} ({ticker})")
                except Exception as e:
                    logging.warning(f"Failed to collect {ticker}: {e}")
            
            # Collect economic indicators
            for indicator, ticker in self.economic_indicators.items():
                try:
                    stock = yf.Ticker(ticker)
                    hist = stock.history(period=period)
                    if not hist.empty:
                        all_data[f"{indicator}"] = hist['Close']
                        logging.info(f"Collected data for {indicator}")
                except Exception as e:
                    logging.warning(f"Failed to collect {indicator}: {e}")
            
            df = pd.DataFrame(all_data)
            df.index = pd.to_datetime(df.index)
            df = df.dropna()
            
            # Store in database
            self.store_price_data(df)
            
            return df
            
        except Exception as e:
            logging.error(f"Data collection error: {e}")
            return pd.DataFrame()
    
    def store_price_data(self, df: pd.DataFrame):
        """Store price data in SQLite database"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            for date_idx in df.index:
                date_str = date_idx.strftime('%Y-%m-%d')
                
                for column in df.columns:
                    if '_' in column:
                        symbol = column.split('_')[-1]
                    else:
                        symbol = column
                    
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR REPLACE INTO factor_prices 
                        (date, symbol, close) 
                        VALUES (?, ?, ?)
                    ''', (date_str, symbol, float(df.loc[date_idx, column])))
            
            conn.commit()
            conn.close()
            logging.info("Price data stored in database")
            
        except Exception as e:
            logging.error(f"Database storage error: {e}")
    
    def calculate_factor_returns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate factor returns and spreads"""
        try:
            returns = df.pct_change().dropna()
            
            # Calculate key spreads
            spreads = pd.DataFrame(index=returns.index)
            
            # Value vs Growth spread
            if 'Value_VTV' in returns.columns and 'Growth_VUG' in returns.columns:
                spreads['Value_Growth_Spread'] = returns['Value_VTV'] - returns['Growth_VUG']
            
            # Small vs Large spread  
            if 'Small_Cap' in returns.columns and 'Market_SPY' in returns.columns:
                spreads['Small_Large_Spread'] = returns['Small_Cap'] - returns['Market_SPY']
            
            # Quality vs Market spread
            if 'Quality_QUAL' in returns.columns and 'Market_SPY' in returns.columns:
                spreads['Quality_Market_Spread'] = returns['Quality_QUAL'] - returns['Market_SPY']
            
            # Low Vol vs Market spread
            if 'Low_Volatility_USMV' in returns.columns and 'Market_SPY' in returns.columns:
                spreads['LowVol_Market_Spread'] = returns['Low_Volatility_USMV'] - returns['Market_SPY']
            
            # Store returns in database
            self.store_returns_data(returns, spreads)
            
            return returns, spreads
            
        except Exception as e:
            logging.error(f"Returns calculation error: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def store_returns_data(self, returns: pd.DataFrame, spreads: pd.DataFrame):
        """Store returns and spreads in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for date_idx in returns.index:
                date_str = date_idx.strftime('%Y-%m-%d')
                
                # Store individual returns
                for column in returns.columns:
                    cursor.execute('''
                        INSERT OR REPLACE INTO factor_returns 
                        (date, symbol, daily_return) 
                        VALUES (?, ?, ?)
                    ''', (date_str, column, float(returns.loc[date_idx, column])))
                
                # Store spreads as metrics
                if not spreads.empty:
                    cursor.execute('''
                        INSERT OR REPLACE INTO factor_metrics 
                        (date, value_growth_spread, small_large_spread, quality_junk_spread) 
                        VALUES (?, ?, ?, ?)
                    ''', (
                        date_str,
                        float(spreads.loc[date_idx, 'Value_Growth_Spread']) if 'Value_Growth_Spread' in spreads.columns else None,
                        float(spreads.loc[date_idx, 'Small_Large_Spread']) if 'Small_Large_Spread' in spreads.columns else None,
                        float(spreads.loc[date_idx, 'Quality_Market_Spread']) if 'Quality_Market_Spread' in spreads.columns else None
                    ))
            
            conn.commit()
            conn.close()
            logging.info("Returns data stored in database")
            
        except Exception as e:
            logging.error(f"Returns storage error: {e}")
    
    def detect_volatility_regime(self, vix_data: pd.Series) -> int:
        """Detect current volatility regime"""
        try:
            latest_vix = vix_data.iloc[-1]
            
            if latest_vix > 30:
                return 2  # High volatility
            elif latest_vix > 20:
                return 1  # Medium volatility
            else:
                return 0  # Low volatility
                
        except Exception as e:
            logging.error(f"Volatility regime detection error: {e}")
            return 1  # Default to medium
    
    def generate_alerts(self, returns: pd.DataFrame, spreads: pd.DataFrame) -> List[Dict]:
        """Generate alerts based on factor movements"""
        alerts = []
        
        try:
            latest_returns = returns.iloc[-1]
            latest_spreads = spreads.iloc[-1] if not spreads.empty else pd.Series()
            
            # Large daily moves alert
            for factor in latest_returns.index:
                if abs(latest_returns[factor]) > 0.025:  # 2.5% threshold
                    severity = "HIGH" if abs(latest_returns[factor]) > 0.04 else "MEDIUM"
                    alerts.append({
                        'type': 'LARGE_MOVE',
                        'factor': factor,
                        'message': f"{factor} moved {latest_returns[factor]:.2%} today",
                        'severity': severity,
                        'value': latest_returns[factor]
                    })
            
            # Spread alerts
            if 'Value_Growth_Spread' in latest_spreads.index:
                if abs(latest_spreads['Value_Growth_Spread']) > 0.02:
                    alerts.append({
                        'type': 'SPREAD_ALERT',
                        'factor': 'Value_Growth',
                        'message': f"Value-Growth spread: {latest_spreads['Value_Growth_Spread']:.2%}",
                        'severity': 'MEDIUM',
                        'value': latest_spreads['Value_Growth_Spread']
                    })
            
            # Store alerts in database
            self.store_alerts(alerts)
            
            return alerts
            
        except Exception as e:
            logging.error(f"Alert generation error: {e}")
            return []
    
    def store_alerts(self, alerts: List[Dict]):
        """Store alerts in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for alert in alerts:
                cursor.execute('''
                    INSERT INTO alerts_log 
                    (timestamp, alert_type, factor, message, severity) 
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    datetime.now().isoformat(),
                    alert['type'],
                    alert['factor'],
                    alert['message'],
                    alert['severity']
                ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logging.error(f"Alert storage error: {e}")
    
    def get_factor_summary(self) -> Dict:
        """Generate factor performance summary"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get latest data
            latest_returns = pd.read_sql_query('''
                SELECT symbol, daily_return 
                FROM factor_returns 
                WHERE date = (SELECT MAX(date) FROM factor_returns)
            ''', conn)
            
            # Get recent alerts
            recent_alerts = pd.read_sql_query('''
                SELECT * FROM alerts_log 
                WHERE date(timestamp) = date('now')
                ORDER BY timestamp DESC
            ''', conn)
            
            conn.close()
            
            summary = {
                'date': datetime.now().strftime('%Y-%m-%d'),
                'latest_returns': latest_returns.to_dict('records'),
                'alerts_count': len(recent_alerts),
                'high_priority_alerts': len(recent_alerts[recent_alerts['severity'] == 'HIGH']),
                'recent_alerts': recent_alerts.head(5).to_dict('records')
            }
            
            return summary
            
        except Exception as e:
            logging.error(f"Summary generation error: {e}")
            return {}
    
    def run_daily_collection(self):
        """Run complete daily data collection and analysis"""
        try:
            logging.info("Starting daily factor data collection")
            
            # Collect data
            df = self.collect_factor_data(period="3mo")  # 3 months for daily updates
            
            if df.empty:
                logging.error("No data collected, exiting")
                return
            
            # Calculate returns and spreads
            returns, spreads = self.calculate_factor_returns(df)
            
            # Generate alerts
            alerts = self.generate_alerts(returns, spreads)
            
            # Generate summary
            summary = self.get_factor_summary()
            
            logging.info(f"Daily collection completed. Generated {len(alerts)} alerts")
            
            return {
                'data': df,
                'returns': returns,
                'spreads': spreads,
                'alerts': alerts,
                'summary': summary
            }
            
        except Exception as e:
            logging.error(f"Daily collection error: {e}")
            return None


# Example usage and testing
if __name__ == "__main__":
    # Initialize collector
    collector = FactorDataCollector()
    
    # Run daily collection
    results = collector.run_daily_collection()
    
    if results:
        print("=== FACTOR MONITORING SUMMARY ===")
        print(f"Data collected for {len(results['data'].columns)} instruments")
        print(f"Generated {len(results['alerts'])} alerts")
        
        if results['alerts']:
            print("\nActive Alerts:")
            for alert in results['alerts']:
                print(f"- {alert['severity']}: {alert['message']}")
        
        print(f"\nLatest returns (top 5):")
        if not results['returns'].empty:
            latest = results['returns'].iloc[-1].sort_values(ascending=False)
            for factor, return_val in latest.head().items():
                print(f"  {factor}: {return_val:.2%}")
    else:
        print("Data collection failed")
