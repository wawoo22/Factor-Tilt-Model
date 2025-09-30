#!/usr/bin/env python3
"""
Test Schwab API with proper scopes and parameters
"""

import os
import requests
import base64
from datetime import datetime
from dotenv import load_dotenv

def get_access_token_with_scopes():
    """Get access token with full scopes"""
    print("🔄 Getting access token with full API scopes...")
    
    load_dotenv()
    
    client_id = os.getenv('SCHWAB_CLIENT_ID')
    refresh_token = os.getenv('SCHWAB_REFRESH_TOKEN')
    client_secret = os.getenv('SCHWAB_CLIENT_SECRET')
    
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
    
    try:
        response = requests.post(token_url, data=token_data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            tokens = response.json()
            print(f"✅ Token obtained successfully")
            print(f"   Scope: {tokens.get('scope', 'N/A')}")
            return tokens['access_token']
        else:
            print(f"❌ Token refresh failed: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def test_market_data_endpoints(access_token):
    """Test market data endpoints with proper parameters"""
    print("\n📊 TESTING MARKET DATA ENDPOINTS")
    print("=" * 40)
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    # Test cases with proper parameters
    test_cases = [
        {
            'name': 'Market Hours - Equity',
            'url': 'https://api.schwabapi.com/marketdata/v1/markets',
            'params': {'markets': 'equity'}
        },
        {
            'name': 'Market Hours - All',
            'url': 'https://api.schwabapi.com/marketdata/v1/markets',
            'params': {'markets': 'equity,option,bond,future,forex'}
        },
        {
            'name': 'Quote - AAPL',
            'url': 'https://api.schwabapi.com/marketdata/v1/quotes',
            'params': {'symbols': 'AAPL'}
        },
        {
            'name': 'Quote - Multiple Stocks',
            'url': 'https://api.schwabapi.com/marketdata/v1/quotes',
            'params': {'symbols': 'AAPL,MSFT,GOOGL'}
        },
        {
            'name': 'Movers - $DJI',
            'url': 'https://api.schwabapi.com/marketdata/v1/movers/$DJI',
            'params': {}
        }
    ]
    
    successful_tests = []
    
    for test in test_cases:
        try:
            print(f"\n🔍 Testing: {test['name']}")
            print(f"   URL: {test['url']}")
            print(f"   Params: {test['params']}")
            
            response = requests.get(
                test['url'], 
                headers=headers, 
                params=test['params'], 
                timeout=10
            )
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                print(f"   ✅ SUCCESS!")
                try:
                    data = response.json()
                    if isinstance(data, dict) and 'data' in data:
                        print(f"   📊 Returned data structure")
                    elif isinstance(data, list):
                        print(f"   📊 Returned {len(data)} items")
                    else:
                        print(f"   📊 Returned: {type(data)}")
                    successful_tests.append(test['name'])
                except:
                    print(f"   📊 Non-JSON response")
                    successful_tests.append(test['name'])
            else:
                print(f"   ❌ Failed: {response.status_code}")
                if response.text:
                    error_text = response.text[:300]
                    print(f"   Error: {error_text}")
                    
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    return successful_tests

def test_account_endpoints(access_token):
    """Test account endpoints - may need special permissions"""
    print("\n🏦 TESTING ACCOUNT ENDPOINTS")
    print("=" * 40)
    
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }
    
    test_cases = [
        {
            'name': 'Account Numbers',
            'url': 'https://api.schwabapi.com/trader/v1/accounts/accountNumbers'
        },
        {
            'name': 'User Preferences',
            'url': 'https://api.schwabapi.com/trader/v1/userPreference'
        },
        {
            'name': 'Accounts List',
            'url': 'https://api.schwabapi.com/trader/v1/accounts'
        }
    ]
    
    successful_tests = []
    auth_issues = []
    
    for test in test_cases:
        try:
            print(f"\n🔍 Testing: {test['name']}")
            print(f"   URL: {test['url']}")
            
            response = requests.get(test['url'], headers=headers, timeout=10)
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                print(f"   ✅ SUCCESS!")
                try:
                    data = response.json()
                    print(f"   📊 Data received")
                    successful_tests.append(test['name'])
                except:
                    print(f"   📊 Non-JSON response")
                    successful_tests.append(test['name'])
                    
            elif response.status_code == 401:
                print(f"   🔒 Unauthorized - App may need additional permissions")
                auth_issues.append(test['name'])
                
            elif response.status_code == 403:
                print(f"   🚫 Forbidden - Account may need API access enabled")
                auth_issues.append(test['name'])
                
            else:
                print(f"   ❌ Failed: {response.status_code}")
                if response.text:
                    error_text = response.text[:300]
                    print(f"   Error: {error_text}")
                    
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    return successful_tests, auth_issues

def check_app_permissions():
    """Provide guidance on app permissions"""
    print("\n🔧 APP PERMISSIONS GUIDANCE")
    print("=" * 40)
    
    print("Based on the test results, here's what you need to check:")
    print("\n1. 🏦 SCHWAB DEVELOPER PORTAL:")
    print("   - Go to https://developer.schwab.com/")
    print("   - Login and check 'My Apps'")
    print("   - Verify your app status is 'Approved' or 'Active'")
    print("   - Check the API scopes/permissions requested")
    
    print("\n2. 📋 REQUIRED API SCOPES:")
    print("   For account access, your app needs:")
    print("   ✅ Read Account and Positions")
    print("   ✅ Read Balances") 
    print("   ✅ Read Orders")
    print("   ✅ Market Data (usually included)")
    
    print("\n3. 🔄 RE-AUTHENTICATION:")
    print("   If you added new scopes, you may need to:")
    print("   - Re-run: python get_refresh_token.py")
    print("   - Answer 'N' to testing existing credentials")
    print("   - Go through the full auth flow again")
    
    print("\n4. 💰 ACCOUNT REQUIREMENTS:")
    print("   - Your Schwab account may need API access enabled")
    print("   - Contact Schwab support if account endpoints don't work")
    print("   - Some features may require a brokerage account vs. bank account")

def main():
    """Main test function"""
    print("🧪 SCHWAB API COMPREHENSIVE TEST")
    print("Testing API scopes and permissions")
    print("=" * 60)
    
    # Get access token
    access_token = get_access_token_with_scopes()
    
    if not access_token:
        print("❌ Could not obtain access token")
        return
    
    # Test market data (should work for most apps)
    market_successes = test_market_data_endpoints(access_token)
    
    # Test account endpoints (may need special permissions)
    account_successes, auth_issues = test_account_endpoints(access_token)
    
    # Summary
    print(f"\n📊 TEST RESULTS SUMMARY")
    print("=" * 30)
    
    if market_successes:
        print(f"✅ Market Data: {len(market_successes)} endpoints working")
        for success in market_successes:
            print(f"   ✅ {success}")
    else:
        print(f"❌ Market Data: No endpoints working")
    
    if account_successes:
        print(f"\n✅ Account Data: {len(account_successes)} endpoints working")
        for success in account_successes:
            print(f"   ✅ {success}")
    else:
        print(f"\n🔒 Account Data: No endpoints working")
    
    if auth_issues:
        print(f"\n🔒 Authorization Issues: {len(auth_issues)} endpoints need permissions")
        for issue in auth_issues:
            print(f"   🔒 {issue}")
    
    # Recommendations
    print(f"\n🎯 RECOMMENDATIONS")
    print("=" * 20)
    
    if market_successes:
        print("✅ Your API authentication works!")
        print("📊 You can use market data for factor analysis")
        
        if not account_successes:
            print("🔒 Account access needs additional permissions")
            print("📝 You can still run factor analysis using market data")
            print("💡 For portfolio management, you'll need account permissions")
    else:
        print("❌ API access issues detected")
        print("🔧 Check app approval status and permissions")
    
    # Show next steps
    if market_successes:
        print(f"\n📈 NEXT STEPS:")
        print("1. ✅ Your system can collect market data")
        print("2. 📊 Run factor analysis: python run_analysis.py")
        print("3. 🔧 For account access, check app permissions in Schwab Portal")
    
    # Provide detailed guidance
    check_app_permissions()

if __name__ == "__main__":
    main()
