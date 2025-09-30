#!/usr/bin/env python3
"""
Factor Monitoring System - Complete Diagnostics
Updated with Schwab API testing
"""

import os
import sys
from dotenv import load_dotenv
import requests
import base64

def test_schwab_api():
    """Test Schwab API connection and credentials"""
    print("🔍 Testing Schwab API...")
    
    load_dotenv()
    
    client_id = os.getenv('SCHWAB_CLIENT_ID')
    refresh_token = os.getenv('SCHWAB_REFRESH_TOKEN')
    client_secret = os.getenv('SCHWAB_CLIENT_SECRET')
    
    # Check if credentials exist
    if not client_id or not refresh_token or not client_secret:
        print("   ⚠️  Schwab API credentials not configured")
        print(f"      Client ID: {'✓' if client_id else '✗'}")
        print(f"      Refresh Token: {'✓' if refresh_token else '✗'}")
        print(f"      Client Secret: {'✓' if client_secret else '✗'}")
        return False, "Not configured"
    
    try:
        # Test token refresh
        print("   🔄 Testing token refresh...")
        
        token_url = "https://api.schwabapi.com/v1/oauth/token"
        
        token_data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': client_id
        }
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        
        # Add client secret authentication
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers['Authorization'] = f"Basic {encoded_credentials}"
        
        response = requests.post(token_url, data=token_data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            tokens = response.json()
            access_token = tokens['access_token']
            
            print("   ✅ Token refresh successful")
            
            # Test market data endpoint
            print("   📊 Testing market data access...")
            
            api_headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }
            
            quotes_response = requests.get(
                'https://api.schwabapi.com/marketdata/v1/quotes?symbols=AAPL', 
                headers=api_headers, 
                timeout=10
            )
            
            if quotes_response.status_code == 200:
                print("   ✅ Market data access working")
                return True, "Fully operational"
            else:
                print(f"   ⚠️  Market data access limited: {quotes_response.status_code}")
                return True, "Token works, limited data access"
        else:
            print(f"   ❌ Token refresh failed: {response.status_code}")
            print(f"      Response: {response.text[:200]}")
            return False, f"Token refresh failed: {response.status_code}"
            
    except requests.exceptions.Timeout:
        print("   ❌ Request timed out")
        return False, "Timeout"
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Connection error: {e}")
        return False, f"Connection error: {str(e)[:50]}"
    except Exception as e:
        print(f"   ❌ Unexpected error: {e}")
        return False, f"Error: {str(e)[:50]}"

def run_complete_diagnostics():
    """Run complete system diagnostics"""
    print("FACTOR MONITORING SYSTEM - COMPLETE DIAGNOSTICS")
    print("="*60)
    
    load_dotenv()
    
    checks = []
    
    # 1. Python Environment
    print("\n1️⃣  PYTHON ENVIRONMENT")
    print("-" * 40)
    python_version = sys.version_info
    python_ok = python_version.major >= 3 and python_version.minor >= 8
    version_str = f"{python_version.major}.{python_version.minor}.{python_version.micro}"
    checks.append(("Python Version", python_ok, version_str))
    print(f"   {'✅' if python_ok else '❌'} Python {version_str}")
    
    # 2. Required Packages
    print("\n2️⃣  REQUIRED PACKAGES")
    print("-" * 40)
    required_packages = {
        'pandas': 'Data manipulation',
        'numpy': 'Numerical computing',
        'yfinance': 'Market data',
        'requests': 'API calls',
        'schedule': 'Task scheduling',
        'sqlite3': 'Database (built-in)'
    }
    
    packages_ok = True
    missing = []
    
    for package, description in required_packages.items():
        try:
            __import__(package)
            print(f"   ✅ {package:15} - {description}")
        except ImportError:
            packages_ok = False
            missing.append(package)
            print(f"   ❌ {package:15} - {description}")
    
    checks.append(("Required Packages", packages_ok, f"Missing: {missing}" if missing else "All present"))
    
    # 3. Database
    print("\n3️⃣  DATABASE CONNECTION")
    print("-" * 40)
    try:
        import sqlite3
        conn = sqlite3.connect("factor_monitoring.db")
        conn.execute("SELECT 1")
        conn.close()
        db_ok = True
        print("   ✅ SQLite database accessible")
    except Exception as e:
        db_ok = False
        print(f"   ❌ Database error: {e}")
    
    checks.append(("Database Connection", db_ok, "SQLite"))
    
    # 4. Email Configuration
    print("\n4️⃣  EMAIL CONFIGURATION")
    print("-" * 40)
    email = os.getenv('FACTOR_EMAIL')
    password = os.getenv('FACTOR_EMAIL_PASSWORD')
    recipients = os.getenv('FACTOR_RECIPIENTS')
    
    email_ok = bool(email and password)
    
    if email:
        print(f"   ✅ Sender email: {email}")
    else:
        print(f"   ❌ Sender email: Not set")
    
    if password:
        print(f"   ✅ App password: Set (length {len(password)})")
    else:
        print(f"   ❌ App password: Not set")
    
    if recipients:
        recipient_list = [r.strip() for r in recipients.split(',') if r.strip()]
        print(f"   ✅ Recipients: {len(recipient_list)} configured")
    else:
        print(f"   ⚠️  Recipients: None configured")
    
    checks.append(("Email Config", email_ok, email if email else 'Not set'))
    
    # 5. Schwab API
    print("\n5️⃣  SCHWAB API")
    print("-" * 40)
    schwab_ok, schwab_status = test_schwab_api()
    checks.append(("Schwab API", schwab_ok, schwab_status))
    
    # 6. Market Data API
    print("\n6️⃣  MARKET DATA API (Yahoo Finance)")
    print("-" * 40)
    try:
        import yfinance as yf
        import warnings
        warnings.filterwarnings('ignore')
        
        print("   📥 Testing data download...")
        
        # Try the new API method first (v0.2.0+)
        try:
            ticker = yf.Ticker('SPY')
            test_data = ticker.history(period='5d')
            market_data_ok = not test_data.empty
            if market_data_ok:
                latest_price = float(test_data['Close'].iloc[-1])
                print(f"   ✅ Yahoo Finance working (SPY: ${latest_price:.2f})")
            else:
                print("   ❌ No data returned")
        except Exception as e1:
            # Fallback to old method
            print(f"   ⚠️  New API method failed, trying alternative...")
            test_data = yf.download('SPY', period='5d', progress=False, auto_adjust=True)
            market_data_ok = not test_data.empty
            if market_data_ok:
                latest_price = float(test_data['Close'].iloc[-1])
                print(f"   ✅ Yahoo Finance working (SPY: ${latest_price:.2f})")
            else:
                print("   ❌ No data returned")
    except Exception as e:
        market_data_ok = False
        print(f"   ❌ Yahoo Finance error: {str(e)[:80]}")
    
    checks.append(("Market Data API", market_data_ok, "Yahoo Finance"))
    
    # 7. File Permissions
    print("\n7️⃣  FILE PERMISSIONS")
    print("-" * 40)
    try:
        test_file = 'test_permissions.tmp'
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        file_permissions_ok = True
        print("   ✅ Read/Write permissions OK")
    except Exception as e:
        file_permissions_ok = False
        print(f"   ❌ Permission error: {e}")
    
    checks.append(("File Permissions", file_permissions_ok, "Read/Write"))
    
    # 8. Environment File
    print("\n8️⃣  ENVIRONMENT CONFIGURATION")
    print("-" * 40)
    env_file_exists = os.path.exists('.env')
    if env_file_exists:
        print("   ✅ .env file found")
        # Count configured variables
        configured_vars = sum([
            1 if os.getenv('FACTOR_EMAIL') else 0,
            1 if os.getenv('FACTOR_EMAIL_PASSWORD') else 0,
            1 if os.getenv('SCHWAB_CLIENT_ID') else 0,
            1 if os.getenv('SCHWAB_REFRESH_TOKEN') else 0,
            1 if os.getenv('SCHWAB_CLIENT_SECRET') else 0,
        ])
        print(f"   📋 {configured_vars}/5 key variables configured")
    else:
        print("   ⚠️  .env file not found")
    
    checks.append(("Environment File", env_file_exists, "Found" if env_file_exists else "Missing"))
    
    # Print Summary
    print("\n" + "="*60)
    print("📊 DIAGNOSTICS SUMMARY")
    print("="*60)
    
    passed = 0
    total = len(checks)
    
    for name, status, detail in checks:
        emoji = "✅" if status else "❌"
        status_text = "PASS" if status else "FAIL"
        print(f"{emoji} {name:25} [{status_text:4}] {detail}")
        if status:
            passed += 1
    
    print("\n" + "="*60)
    print(f"RESULT: {passed}/{total} checks passed ({passed/total*100:.1f}%)")
    print("="*60)
    
    # Recommendations
    print("\n💡 RECOMMENDATIONS:")
    print("-" * 40)
    
    if passed == total:
        print("🎉 EXCELLENT! All systems operational!")
        print("✅ Your Factor Monitoring System is ready for production")
        print("\n📋 Next steps:")
        print("   1. Run: python minimal_factor_system.py")
        print("   2. Review the email report")
        print("   3. Set up automated daily runs")
    else:
        print("⚠️  Some components need attention:")
        
        if not packages_ok:
            print(f"\n📦 Install missing packages:")
            print(f"   pip install {' '.join(missing)}")
        
        if not email_ok:
            print(f"\n📧 Configure email in .env file:")
            print(f"   FACTOR_EMAIL=your_email@gmail.com")
            print(f"   FACTOR_EMAIL_PASSWORD=your_app_password")
        
        if not schwab_ok:
            print(f"\n🔐 Configure Schwab API (optional):")
            print(f"   1. Visit: https://developer.schwab.com/")
            print(f"   2. Run: python get_refresh_token.py")
            print(f"   3. Add credentials to .env file")
        
        print(f"\n✅ Core functionality ({passed}/{total} checks) is operational")
        print(f"   You can still use the system with limited features")
    
    return passed == total

if __name__ == "__main__":
    try:
        success = run_complete_diagnostics()
        
        print("\n" + "="*60)
        if success:
            print("🚀 READY FOR DEPLOYMENT")
        else:
            print("🔧 CONFIGURATION NEEDED")
        print("="*60)
        
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print("\n\n⏹️  Diagnostics cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Diagnostics error: {e}")
        sys.exit(1)
