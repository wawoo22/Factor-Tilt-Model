#!/usr/bin/env python3
"""
Minimal Factor Monitoring System
Clean implementation that works without corrupted files
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
warnings.filterwarnings('ignore')

class MinimalFactorSystem:
    """Minimal factor monitoring system that definitely works"""
    
    def __init__(self):
        self.db_path = "minimal_factor_data.db"
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
        
        self.setup_database()
    
    def setup_database(self):
        """Setup minimal database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS factor_data (
                date TEXT,
                symbol TEXT,
                price REAL,
                daily_return REAL,
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
    
    def collect_factor_data(self):
        """Collect current factor data"""
        print("📊 Collecting factor data...")
        
        data = {}
        alerts = []
        
        for factor_name, symbol in self.factor_etfs.items():
            try:
                print(f"   Fetching {factor_name} ({symbol})...")
                
                ticker = yf.Ticker(symbol)
                hist = ticker.history(period="5d")
                
                if not hist.empty:
                    latest_price = float(hist['Close'].iloc[-1])
                    daily_return = float(hist['Close'].pct_change().iloc[-1])
                    
                    data[factor_name] = {
                        'symbol': symbol,
                        'price': latest_price,
                        'daily_return': daily_return,
                        'date': hist.index[-1].strftime('%Y-%m-%d')
                    }
                    
                    # Generate alerts for large moves
                    if abs(daily_return) > 0.025:  # 2.5% threshold
                        severity = "HIGH" if abs(daily_return) > 0.04 else "MEDIUM"
                        alerts.append({
                            'factor': factor_name,
                            'message': f"{factor_name} moved {daily_return:+.2%} today",
                            'severity': severity
                        })
                    
                    print(f"   ✅ {factor_name}: ${latest_price:.2f} ({daily_return:+.2%})")
                else:
                    print(f"   ❌ No data for {symbol}")
                    
            except Exception as e:
                print(f"   ❌ Error fetching {symbol}: {e}")
        
        # Store data
        self.store_data(data, alerts)
        
        return data, alerts
    
    def store_data(self, data, alerts):
        """Store data in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Store factor data
        for factor_name, info in data.items():
            cursor.execute("""
                INSERT OR REPLACE INTO factor_data 
                (date, symbol, price, daily_return) 
                VALUES (?, ?, ?, ?)
            """, (info['date'], info['symbol'], info['price'], info['daily_return']))
        
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
    
    def create_email_report(self, data, alerts):
        """Create HTML email report"""
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
            </style>
        </head>
        <body>
            <div class="header">
                <h1>📊 Factor Monitoring Report</h1>
                <p>{datetime.now().strftime('%Y-%m-%d %H:%M:%S EST')}</p>
            </div>
            
            <h2>💹 Factor Performance</h2>
            <table class="factor-table">
                <tr>
                    <th>Factor</th>
                    <th>Symbol</th>
                    <th>Price</th>
                    <th>Daily Return</th>
                </tr>
        """
        
        # Add factor data
        for factor_name, info in data.items():
            return_class = 'positive' if info['daily_return'] > 0 else 'negative'
            html_content += f"""
                <tr>
                    <td><strong>{factor_name}</strong></td>
                    <td>{info['symbol']}</td>
                    <td>${info['price']:.2f}</td>
                    <td class="{return_class}">{info['daily_return']:+.2%}</td>
                </tr>
            """
        
        html_content += "</table>"
        
        # Add alerts section
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
            <p><em>Generated by Minimal Factor Monitoring System</em></p>
        </body>
        </html>
        """
        
        return html_content
    
    def send_email_report(self, data, alerts):
        """Send email report"""
        if not self.email or not self.email_password or not self.recipients:
            print("⚠️  Email not configured - skipping email report")
            return False
        
        try:
            print("📧 Sending email report...")
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.email
            msg['To'] = ', '.join(self.recipients)
            
            # Subject with alert count
            alert_count = len(alerts)
            subject = f"Factor Report - {datetime.now().strftime('%Y-%m-%d')}"
            if alert_count > 0:
                subject += f" ({alert_count} alerts)"
            
            msg['Subject'] = subject
            
            # Create HTML content
            html_content = self.create_email_report(data, alerts)
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)
            
            # Send email
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
        """Run complete factor analysis"""
        print("\n🚀 MINIMAL FACTOR MONITORING SYSTEM")
        print("=" * 50)
        print(f"Start Time: {datetime.now()}")
        print("=" * 50)
        
        # Collect data
        data, alerts = self.collect_factor_data()
        
        if not data:
            print("❌ No data collected")
            return False
        
        # Display summary
        print(f"\n📋 SUMMARY:")
        print(f"   📊 Factors collected: {len(data)}")
        print(f"   🚨 Alerts generated: {len(alerts)}")
        
        if alerts:
            print(f"\n🔔 ACTIVE ALERTS:")
            for alert in alerts:
                print(f"   {alert['severity']}: {alert['message']}")
        
        # Send email if configured
        email_sent = self.send_email_report(data, alerts)
        
        print(f"\n✅ ANALYSIS COMPLETE!")
        print(f"   📊 Data collected and stored")
        print(f"   📧 Email report: {'Sent' if email_sent else 'Skipped (not configured)'}")
        print(f"   💾 Database: {self.db_path}")
        
        return True

def main():
    """Main function"""
    try:
        system = MinimalFactorSystem()
        success = system.run_analysis()
        
        if success:
            print("\n🎉 SUCCESS! Factor monitoring system is working!")
            print("\nNext steps:")
            print("1. Check your email for the report")
            print("2. Review the database file: minimal_factor_data.db")
            print("3. Run this script daily for monitoring")
            print("\n💡 To run daily automatically:")
            print("   factor run  # Use the convenient command")
        else:
            print("\n❌ System failed - check the errors above")
    
    except Exception as e:
        print(f"\n💥 System error: {e}")
        print("This might be due to:")
        print("- Missing required packages")
        print("- Network connectivity issues")
        print("- Invalid email configuration")

if __name__ == "__main__":
    main()
