import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import logging

class SimpleFactorCollector:
    """Simplified Factor Data Collector for initial implementation"""
    
    def __init__(self, db_path="factor_monitoring.db"):
        self.db_path = db_path
        self.factor_etfs = {
            'Value': 'VTV',
            'Growth': 'VUG', 
            'Quality': 'QUAL',
            'Momentum': 'MTUM',
            'Low_Volatility': 'USMV',
            'Size': 'VB',
            'Market': 'SPY'
        }
        
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
    def collect_data(self):
        """Collect current market data"""
        print("📊 Collecting factor data...")
        
        try:
            all_data = {}
            
            for factor_name, symbol in self.factor_etfs.items():
                print(f"   Fetching {factor_name} ({symbol})...")
                
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="5d")  # Last 5 days
                
                if not hist.empty:
                    latest_price = hist['Close'].iloc[-1]
                    daily_return = hist['Close'].pct_change().iloc[-1]
                    
                    all_data[factor_name] = {
                        'symbol': symbol,
                        'price': latest_price,
                        'daily_return': daily_return,
                        'date': hist.index[-1].strftime('%Y-%m-%d')
                    }
                    
                    print(f"   ✅ {factor_name}: ${latest_price:.2f} ({daily_return:+.2%})")
                else:
                    print(f"   ❌ No data for {symbol}")
            
            # Store in database
            self.store_data(all_data)
            
            return all_data
            
        except Exception as e:
            self.logger.error(f"Data collection failed: {e}")
            return {}
    
    def store_data(self, data):
        """Store data in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for factor_name, info in data.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO factor_prices 
                    (date, symbol, close) 
                    VALUES (?, ?, ?)
                """, (info['date'], info['symbol'], info['price']))
                
                cursor.execute("""
                    INSERT OR REPLACE INTO factor_returns 
                    (date, symbol, daily_return) 
                    VALUES (?, ?, ?)
                """, (info['date'], info['symbol'], info['daily_return']))
            
            conn.commit()
            conn.close()
            print("✅ Data stored in database")
            
        except Exception as e:
            self.logger.error(f"Database storage failed: {e}")
    
    def generate_simple_alerts(self, data):
        """Generate basic alerts"""
        alerts = []
        
        for factor_name, info in data.items():
            daily_return = info['daily_return']
            
            if abs(daily_return) > 0.025:  # 2.5% move
                severity = "HIGH" if abs(daily_return) > 0.04 else "MEDIUM"
                alerts.append({
                    'factor': factor_name,
                    'message': f"{factor_name} moved {daily_return:+.2%} today",
                    'severity': severity,
                    'timestamp': datetime.now().isoformat()
                })
        
        # Store alerts
        if alerts:
            self.store_alerts(alerts)
        
        return alerts
    
    def store_alerts(self, alerts):
        """Store alerts in database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for alert in alerts:
                cursor.execute("""
                    INSERT INTO alerts_log 
                    (timestamp, alert_type, factor, message, severity) 
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    alert['timestamp'],
                    'LARGE_MOVE',
                    alert['factor'],
                    alert['message'],
                    alert['severity']
                ))
            
            conn.commit()
            conn.close()
            print(f"✅ Stored {len(alerts)} alerts")
            
        except Exception as e:
            self.logger.error(f"Alert storage failed: {e}")
    
    def run_collection(self):
        """Run complete data collection and analysis"""
        print("\n🚀 STARTING FACTOR DATA COLLECTION")
        print("=" * 50)
        
        # Collect data
        data = self.collect_data()
        
        if not data:
            print("❌ No data collected")
            return None
        
        # Generate alerts
        alerts = self.generate_simple_alerts(data)
        
        # Summary
        print(f"\n📋 COLLECTION SUMMARY:")
        print(f"   📊 Factors collected: {len(data)}")
        print(f"   🚨 Alerts generated: {len(alerts)}")
        
        if alerts:
            print(f"\n🔔 ACTIVE ALERTS:")
            for alert in alerts:
                print(f"   {alert['severity']}: {alert['message']}")
        
        return {
            'data': data,
            'alerts': alerts,
            'timestamp': datetime.now().isoformat()
        }

# Test function
def test_data_collection():
    """Test the data collection system"""
    collector = SimpleFactorCollector()
    results = collector.run_collection()
    
    if results:
        print("\n✅ DATA COLLECTION TEST PASSED!")
        return True
    else:
        print("\n❌ DATA COLLECTION TEST FAILED!")
        return False

if __name__ == "__main__":
    test_data_collection()
