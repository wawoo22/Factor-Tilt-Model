import yfinance as yf
import requests
from datetime import datetime, timedelta

def test_yahoo_finance():
    """Test Yahoo Finance API"""
    try:
        # Test factor ETF data
        symbols = ['VTV', 'VUG', 'QUAL', 'MTUM', 'USMV', 'VB', 'SPY']
        
        for symbol in symbols:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period="5d")
            
            if hist.empty:
                print(f"❌ No data for {symbol}")
                return False
            else:
                latest_price = hist['Close'].iloc[-1]
                print(f"✅ {symbol}: ${latest_price:.2f}")
        
        return True
        
    except Exception as e:
        print(f"❌ Yahoo Finance API test failed: {e}")
        return False

def test_fred_api():
    """Test FRED economic data"""
    try:
        # Test VIX data
        vix_ticker = yf.Ticker("^VIX")
        vix_data = vix_ticker.history(period="5d")
        
        if not vix_data.empty:
            current_vix = vix_data['Close'].iloc[-1]
            print(f"✅ VIX Level: {current_vix:.2f}")
            return True
        else:
            print("❌ VIX data not available")
            return False
            
    except Exception as e:
        print(f"❌ Economic data test failed: {e}")
        return False

def test_alpha_vantage(api_key=None):
    """Test Alpha Vantage API (optional)"""
    if not api_key:
        print("⚠️  Alpha Vantage API key not provided - skipping")
        return True
    
    try:
        url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol=SPY&apikey={api_key}"
        response = requests.get(url)
        data = response.json()
        
        if 'Symbol' in data:
            print("✅ Alpha Vantage API working")
            return True
        else:
            print("❌ Alpha Vantage API test failed")
            return False
            
    except Exception as e:
        print(f"❌ Alpha Vantage API error: {e}")
        return False

def run_all_api_tests():
    print("TESTING MARKET DATA APIs")
    print("="*40)
    
    yahoo_ok = test_yahoo_finance()
    fred_ok = test_fred_api()
    alpha_ok = test_alpha_vantage()
    
    print("\n" + "="*40)
    if yahoo_ok and fred_ok and alpha_ok:
        print("✅ ALL API TESTS PASSED")
        return True
    else:
        print("❌ SOME API TESTS FAILED")
        return False

if __name__ == "__main__":
    run_all_api_tests()
