#!/usr/bin/env python3
"""
Schwab-Enhanced Factor Monitoring System
Includes Schwab API integration for real portfolio positions and market data
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sqlite3
import os
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import warnings
import requests
import base64
import json
warnings.filterwarnings('ignore')

class SchwabAPI:
    """Schwab API integration for market data and portfolio positions"""
    
    def __init__(self):
        load_dotenv()
        self.client_id = os.getenv('SCHWAB_CLIENT_ID')
        self.client_secret = os.getenv('SCHWAB_CLIENT_SECRET')
        self.refresh_token = os.getenv('SCHWAB_REFRESH_TOKEN')
        self.access_token = None
        self.token_expiry = None
        self.base_url = "https://api.schwabapi.com"
        
    def get_access_token(self):
        """Get access token using refresh token"""
        if not self.client_id or not self.client_secret or not self.refresh_token:
            return False
            
        try:
            token_url = f"{self.base_url}/v1/oauth/token"
            
            token_data = {
                'grant_type': 'refresh_token',
                'refresh_token': self.refresh_token,
                'client_id': self.client_id
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Accept': 'application/json'
            }
            
            # Add client secret authentication
            credentials = f"{self.client_id}:{self.client_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers['Authorization'] = f"Basic {encoded_credentials}"
            
            response = requests.post(token_url, data=token_data, headers=headers, timeout=30)
            
            if response.status_code == 200:
                tokens = response.json()
                self.access_token = tokens['access_token']
                self.token_expiry = datetime.now().timestamp() + tokens.get('expires_in', 1800) - 300
                return True
            else:
                print(f"   ⚠️  Schwab token refresh failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ⚠️  Schwab API error: {e}")
            return False
    
    def _ensure_valid_token(self):
        """Ensure we have a valid access token"""
        if not self.access_token or not self.token_expiry or datetime.now().timestamp() > self.token_expiry:
            return self.get_access_token()
        return True
    
    def get_quotes(self, symbols):
        """Get quotes for symbols"""
        if not self._ensure_valid_token():
            return None
            
        if isinstance(symbols, list):
            symbols = ','.join(symbols)
        
        try:
            url = f"{self.base_url}/marketdata/v1/quotes"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {'symbols': symbols}
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception as e:
            print(f"   ⚠️  Schwab quotes error: {e}")
            return None
    
    def get_account_numbers(self):
        """Get account numbers"""
        if not self._ensure_valid_token():
            return None
            
        try:
            url = f"{self.base_url}/trader/v1/accounts/accountNumbers"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception as e:
            print(f"   ⚠️  Schwab account numbers error: {e}")
            return None
    
    def get_account_positions(self, account_hash):
        """Get positions for an account"""
        if not self._ensure_valid_token():
            return None
            
        try:
            url = f"{self.base_url}/trader/v1/accounts/{account_hash}"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Accept': 'application/json'
            }
            params = {'fields': 'positions'}
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'securitiesAccount' in data:
                    return data['securitiesAccount'].get('positions', [])
                return []
            else:
                return None
                
        except Exception as e:
            print(f"   ⚠️  Schwab positions error: {e}")
            return None

class SchwabEnhancedFactorSystem:
    """Factor monitoring system with Schwab API integration"""
    
    def __init__(self):
        self.db_path = "schwab_factor_data.db"
        self.factor_etfs = {
            'Value': 'VTV',
            'Growth': 'VUG', 
            'Quality': 'QUAL',
            'Momentum': 'MTUM',
            'Low_Volatility': 'USMV',
            'Size': 'VB',
            'Market': 'SPY'
        }
        
        # Load environment
        load_dotenv()
        self.email = os.getenv('FACTOR_EMAIL')
        self.email_password = os.getenv('FACTOR_EMAIL_PASSWORD')
        self.recipients = os.getenv('FACTOR_RECIPIENTS', '').split(',')
        self.recipients = [r.strip() for r in self.recipients if r.strip()]
        
        # Initialize Schwab API
        self.schwab_api = SchwabAPI()
        self.schwab_available = False
        
        self.setup_database()
        self.check_schwab_connection()
    
    def setup_database(self):
        """Setup database with portfolio positions table"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS factor_data (
                date TEXT,
                symbol TEXT,
                price REAL,
                daily_return REAL,
                data_source TEXT,
                PRIMARY KEY (date, symbol)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS portfolio_positions (
                date TEXT,
                symbol TEXT,
                quantity REAL,
                market_value REAL,
                cost_basis REAL,
                unrealized_pnl REAL,
                PRIMARY KEY (date, symbol)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                timestamp TEXT,
                message TEXT,
                severity TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        print("✅ Database setup complete")
    
    def check_schwab_connection(self):
        """Check if Schwab API is available"""
        print("🔐 Checking Schwab API connection...")
        
        if self.schwab_api.get_access_token():
            self.schwab_available = True
            print("   ✅ Schwab API connected")
        else:
            self.schwab_available = False
            print("   ⚠️  Schwab API not available - will use Yahoo Finance only")
    
    def get_portfolio_positions(self):
        """Get current portfolio positions from Schwab"""
        if not self.schwab_available:
            return None
        
        print("📊 Fetching portfolio positions from Schwab...")
        
        try:
            # Get account numbers
            accounts = self.schwab_api.get_account_numbers()
            
            if not accounts:
                print("   ⚠️  No account data available")
                return None
            
            # Get positions for first account
            if isinstance(accounts, list) and len(accounts) > 0:
                account_hash = accounts[0].get('hashValue')
                if account_hash:
                    positions = self.schwab_api.get_account_positions(account_hash)
                    
                    if positions:
                        print(f"   ✅ Found {len(positions)} positions")
                        return self.parse_positions(positions)
                    else:
                        print("   ⚠️  No positions returned")
                        return None
            
            return None
            
        except Exception as e:
            print(f"   ❌ Error fetching positions: {e}")
            return None
    
    def parse_positions(self, positions):
        """Parse Schwab positions data"""
        parsed_positions = []
        
        for position in positions:
            try:
                instrument = position.get('instrument', {})
                symbol = instrument.get('symbol', 'UNKNOWN')
                
                parsed_positions.append({
                    'symbol': symbol,
                    'quantity': position.get('longQuantity', 0),
                    'market_value': position.get('marketValue', 0),
                    'cost_basis': position.get('averagePrice', 0) * position.get('longQuantity', 0),
                    'current_price': position.get('marketValue', 0) / max(position.get('longQuantity', 1), 1),
                    'unrealized_pnl': position.get('marketValue', 0) - (position.get('averagePrice', 0) * position.get('longQuantity', 0))
                })
            except Exception as e:
                print(f"   ⚠️  Error parsing position: {e}")
                continue
        
        return parsed_positions
    
    def store_portfolio_positions(self, positions):
        """Store portfolio positions in database"""
        if not positions:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        for position in positions:
            cursor.execute("""
                INSERT OR REPLACE INTO portfolio_positions 
                (date, symbol, quantity, market_value, cost_basis, unrealized_pnl) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (today, position['symbol'], position['quantity'], 
                  position['market_value'], position['cost_basis'], 
                  position['unrealized_pnl']))
        
        conn.commit()
        conn.close()
        print("✅ Portfolio positions stored")
    
    def collect_factor_data(self):
        """Collect factor data from Schwab API and/or Yahoo Finance"""
        print("📊 Collecting factor data...")
        
        data = {}
        alerts = []
        
        # Try Schwab API first
        if self.schwab_available:
            print("   Using Schwab API for market data...")
            schwab_data = self.collect_from_schwab()
            if schwab_data:
                data.update(schwab_data)
        
        # Fill in any missing data with Yahoo Finance
        missing_symbols = [sym for sym in self.factor_etfs.values() if sym not in [d.get('symbol') for d in data.values()]]
        
        if missing_symbols or not self.schwab_available:
            print("   Using Yahoo Finance for market data...")
            yahoo_data = self.collect_from_yahoo()
            
            # Merge data, preferring Schwab when available
            for factor_name, info in yahoo_data.items():
                if factor_name not in data:
                    data[factor_name] = info
        
        # Generate alerts
        for factor_name, info in data.items():
            if abs(info['daily_return']) > 0.025:  # 2.5% threshold
                severity = "HIGH" if abs(info['daily_return']) > 0.04 else "MEDIUM"
                alerts.append({
                    'factor': factor_name,
                    'message': f"{factor_name} moved {info['daily_return']:+.2%} today",
                    'severity': severity
                })
        
        # Store data
        self.store_data(data, alerts)
        
        return data, alerts
    
    def collect_from_schwab(self):
        """Collect data from Schwab API"""
        data = {}
        
        symbols = list(self.factor_etfs.values())
        quotes = self.schwab_api.get_quotes(symbols)
        
        if not quotes:
            return data
        
        for factor_name, symbol in self.factor_etfs.items():
            try:
                if symbol in quotes:
                    quote = quotes[symbol]
                    
                    # Extract price data
                    current_price = quote.get('mark', quote.get('lastPrice', 0))
                    prev_close = quote.get('closePrice', current_price)
                    
                    if prev_close > 0:
                        daily_return = (current_price - prev_close) / prev_close
                    else:
                        daily_return = 0.0
                    
                    data[factor_name] = {
                        'symbol': symbol,
                        'price': float(current_price),
                        'daily_return': float(daily_return),
                        'date': datetime.now().strftime('%Y-%m-%d'),
                        'source': 'Schwab'
                    }
                    
                    print(f"   ✅ {factor_name}: ${current_price:.2f} ({daily_return:+.2%}) [Schwab]")
                    
            except Exception as e:
                print(f"   ⚠️  Error processing Schwab data for {symbol}: {e}")
                continue
        
        return data
    
    def collect_from_yahoo(self):
        """Collect data from Yahoo Finance"""
        data = {}
        
        for factor_name, symbol in self.factor_etfs.items():
            try:
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="5d")
                
                if not hist.empty:
                    latest_price = float(hist['Close'].iloc[-1])
                    daily_return = float(hist['Close'].pct_change().iloc[-1])
                    
                    data[factor_name] = {
                        'symbol': symbol,
                        'price': latest_price,
                        'daily_return': daily_return,
                        'date': hist.index[-1].strftime('%Y-%m-%d'),
                        'source': 'Yahoo'
                    }
                    
                    print(f"   ✅ {factor_name}: ${latest_price:.2f} ({daily_return:+.2%}) [Yahoo]")
                    
            except Exception as e:
                print(f"   ❌ Error fetching {symbol}: {e}")
        
        return data
    
    def store_data(self, data, alerts):
        """Store data in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store factor data
        for factor_name, info in data.items():
            cursor.execute("""
                INSERT OR REPLACE INTO factor_data 
                (date, symbol, price, daily_return, data_source) 
                VALUES (?, ?, ?, ?, ?)
            """, (info['date'], info['symbol'], info['price'], 
                  info['daily_return'], info.get('source', 'Unknown')))
        
        # Store alerts
        for alert in alerts:
            cursor.execute("""
                INSERT INTO alerts 
                (timestamp, message, severity) 
                VALUES (?, ?, ?)
            """, (datetime.now().isoformat(), alert['message'], alert['severity']))
        
        conn.commit()
        conn.close()
        print("✅ Data stored in database")
    
    def create_email_report(self, data, alerts, positions=None):
        """Create HTML email report with portfolio positions"""
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #2c3e50; color: white; padding: 15px; text-align: center; }}
                .factor-table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                .factor-table th, .factor-table td {{ 
                    border: 1px solid #ddd; 
                    padding: 12px; 
                    text-align: left; 
                }}
                .factor-table th {{ background-color: #3498db; color: white; }}
                .positive {{ color: #27ae60; font-weight: bold; }}
                .negative {{ color: #e74c3c; font-weight: bold; }}
                .alert-high {{ background: #ffebee; padding: 10px; margin: 5px 0; border-left: 4px solid #e74c3c; }}
                .alert-medium {{ background: #fff3e0; padding: 10px; margin: 5px 0; border-left: 4px solid #f39c12; }}
                .schwab-badge {{ background: #00a0df; color: white; padding: 2px 6px; border-radius: 3px; font-size: 10px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Factor Monitoring Report</h1>
                <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}</p>
                <p>{'<span class="schwab-badge">Schwab API Connected</span>' if self.schwab_available else 'Yahoo Finance Data'}</p>
            </div>
        """
        
        # Portfolio positions section (if available)
        if positions:
            total_value = sum([p['market_value'] for p in positions])
            total_pnl = sum([p['unrealized_pnl'] for p in positions])
            
            html_content += f"""
            <h2>💼 Portfolio Positions (Schwab)</h2>
            <p><strong>Total Value:</strong> ${total_value:,.2f} | 
               <strong>Total P&L:</strong> <span class="{'positive' if total_pnl >= 0 else 'negative'}">${total_pnl:+,.2f}</span></p>
            <table class="factor-table">
                <tr>
                    <th>Symbol</th>
                    <th>Quantity</th>
                    <th>Market Value</th>
                    <th>Cost Basis</th>
                    <th>Unrealized P&L</th>
                </tr>
            """
            
            for position in positions:
                pnl_class = 'positive' if position['unrealized_pnl'] >= 0 else 'negative'
                html_content += f"""
                    <tr>
                        <td><strong>{position['symbol']}</strong></td>
                        <td>{position['quantity']:.0f}</td>
                        <td>${position['market_value']:,.2f}</td>
                        <td>${position['cost_basis']:,.2f}</td>
                        <td class="{pnl_class}">${position['unrealized_pnl']:+,.2f}</td>
                    </tr>
                """
            
            html_content += "</table>"
        
        # Factor performance section
        html_content += """
            <h2>💹 Factor Performance</h2>
            <table class="factor-table">
                <tr>
                    <th>Factor</th>
                    <th>Symbol</th>
                    <th>Price</th>
                    <th>Daily Return</th>
                    <th>Source</th>
                </tr>
        """
        
        for factor_name, info in data.items():
            return_class = 'positive' if info['daily_return'] > 0 else 'negative'
            source_badge = f"<span class='schwab-badge'>{info.get('source', 'N/A')}</span>"
            html_content += f"""
                <tr>
                    <td><strong>{factor_name}</strong></td>
                    <td>{info['symbol']}</td>
                    <td>${info['price']:.2f}</td>
                    <td class="{return_class}">{info['daily_return']:+.2%}</td>
                    <td>{source_badge}</td>
                </tr>
            """
        
        html_content += "</table>"
        
        # Alerts section
        if alerts:
            html_content += "<h2>🚨 Active Alerts</h2>"
            for alert in alerts:
                alert_class = f"alert-{alert['severity'].lower()}"
                html_content += f"""
                    <div class="{alert_class}">
                        <strong>{alert['severity']}:</strong> {alert['message']}
                    </div>
                """
        else:
            html_content += "<h2>✅ No Alerts</h2><p>All factors within normal ranges.</p>"
        
        html_content += """
            <hr>
            <p><em>Generated by Schwab-Enhanced Factor Monitoring System</em></p>
        </body>
        </html>
        """
        
        return html_content
    
    def send_email_report(self, data, alerts, positions=None):
        """Send email report"""
        if not self.email or not self.email_password or not self.recipients:
            print("⚠️  Email not configured - skipping email report")
            return False
        
        try:
            print("📧 Sending email report...")
            
            msg = MIMEMultipart('alternative')
            msg['From'] = self.email
            msg['To'] = ', '.join(self.recipients)
            
            alert_count = len(alerts)
            subject = f"Factor Report - {datetime.now().strftime('%Y-%m-%d')}"
            if alert_count > 0:
                subject += f" ({alert_count} alerts)"
            if self.schwab_available:
                subject += " [Schwab]"
            
            msg['Subject'] = subject
            
            html_content = self.create_email_report(data, alerts, positions)
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email, self.email_password)
            
            for recipient in self.recipients:
                server.sendmail(self.email, recipient, msg.as_string())
                print(f"   ✅ Sent to {recipient}")
            
            server.quit()
            print("✅ Email report sent successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Email sending failed: {e}")
            return False
    
    def run_analysis(self):
        """Run complete factor analysis with Schwab integration"""
        print("\n🚀 SCHWAB-ENHANCED FACTOR MONITORING SYSTEM")
        print("=" * 60)
        print(f"Start Time: {datetime.now()}")
        print(f"Schwab API: {'✅ Connected' if self.schwab_available else '❌ Not Available'}")
        print("=" * 60)
        
        # Get portfolio positions from Schwab (if available)
        positions = None
        if self.schwab_available:
            positions = self.get_portfolio_positions()
            if positions:
                self.store_portfolio_positions(positions)
                total_value = sum([p['market_value'] for p in positions])
                print(f"\n💼 Portfolio: ${total_value:,.2f} across {len(positions)} positions")
        
        # Collect factor data
        data, alerts = self.collect_factor_data()
        
        if not data:
            print("❌ No data collected")
            return False
        
        # Display summary
        print(f"\n📋 SUMMARY:")
        print(f"   📊 Factors collected: {len(data)}")
        print(f"   🚨 Alerts generated: {len(alerts)}")
        if positions:
            print(f"   💼 Portfolio positions: {len(positions)}")
        
        if alerts:
            print(f"\n🔔 ACTIVE ALERTS:")
            for alert in alerts:
                print(f"   {alert['severity']}: {alert['message']}")
        
        # Send email
        email_sent = self.send_email_report(data, alerts, positions)
        
        print(f"\n✅ ANALYSIS COMPLETE!")
        print(f"   📊 Data collected and stored")
        print(f"   📧 Email report: {'Sent' if email_sent else 'Skipped'}")
        print(f"   💾 Database: {self.db_path}")
        
        return True

def main():
    """Main function"""
    try:
        system = SchwabEnhancedFactorSystem()
        success = system.run_analysis()
        
        if success:
            print("\n🎉 SUCCESS! Schwab-enhanced factor monitoring is working!")
            print("\nFeatures enabled:")
            print("✅ Factor ETF price tracking")
            if system.schwab_available:
                print("✅ Schwab API integration")
                print("✅ Real portfolio positions")
                print("✅ Live market data from Schwab")
            else:
                print("⚠️  Yahoo Finance fallback (Schwab API not configured)")
            print("✅ Email alerts")
            print("✅ Database storage")
        else:
            print("\n❌ System failed - check the errors above")
    
    except Exception as e:
        print(f"\n💥 System error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
