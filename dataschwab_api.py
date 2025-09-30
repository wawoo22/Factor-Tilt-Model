import requests
import base64
import time
import logging
from datetime import datetime
from config import Config

class SchwabAPI:
    """Enhanced Schwab API integration for market data and portfolio execution"""
    
    def __init__(self):
        self.client_id = Config.SCHWAB_CLIENT_ID
        self.refresh_token = Config.SCHWAB_REFRESH_TOKEN
        self.client_secret = Config.SCHWAB_CLIENT_SECRET
        self.access_token = None
        self.token_expiry = None
        self.base_url = Config.SCHWAB_BASE_URL
        self.logger = logging.getLogger(__name__)
        
    def get_access_token(self):
        """Get access token using refresh token"""
        url = f"{self.base_url}/v1/oauth/token"
        
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
        if self.client_secret:
            credentials = f"{self.client_id}:{self.client_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers['Authorization'] = f"Basic {encoded_credentials}"
        
        try:
            response = requests.post(url, data=token_data, headers=headers, timeout=30)
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                # Token expires in 30 minutes, refresh at 25 minutes
                self.token_expiry = datetime.now().timestamp() + token_data.get('expires_in', 1800) - 300
                self.logger.info("Access token refreshed successfully")
                return True
            else:
                self.logger.error(f"Token refresh failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            self.logger.error(f"Token refresh error: {e}")
            return False
    
    def _ensure_valid_token(self):
        """Ensure we have a valid access token"""
        if not self.access_token or not self.token_expiry or datetime.now().timestamp() > self.token_expiry:
            return self.get_access_token()
        return True
    
    def _make_api_request(self, url, params=None):
        """Make authenticated API request"""
        if not self._ensure_valid_token():
            return None
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"API request failed: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            self.logger.error(f"API request error: {e}")
            return None
    
    def get_market_hours(self, markets='equity'):
        """Get market hours for specified markets"""
        url = f"{self.base_url}/marketdata/v1/markets"
        params = {'markets': markets}
        return self._make_api_request(url, params)
    
    def get_quotes(self, symbols):
        """Get quotes for one or more symbols"""
        if isinstance(symbols, list):
            symbols = ','.join(symbols)
        
        url = f"{self.base_url}/marketdata/v1/quotes"
        params = {'symbols': symbols}
        return self._make_api_request(url, params)
    
    def get_price_history(self, symbol, period_type='year', period=1, frequency_type='daily', frequency=1):
        """Get price history for a symbol"""
        url = f"{self.base_url}/marketdata/v1/pricehistory"
        params = {
            'symbol': symbol,
            'periodType': period_type,
            'period': period,
            'frequencyType': frequency_type,
            'frequency': frequency
        }
        return self._make_api_request(url, params)
    
    def get_movers(self, index='$DJI', direction='up', change='percent'):
        """Get market movers"""
        url = f"{self.base_url}/marketdata/v1/movers/{index}"
        params = {'direction': direction, 'change': change}
        return self._make_api_request(url, params)
    
    def get_current_prices(self, symbols):
        """Get current prices for symbols (compatible with yahoo_data_collector)"""
        quotes_data = self.get_quotes(symbols)
        
        if not quotes_data:
            return {}
        
        current_prices = {}
        
        # Handle single symbol vs multiple symbols response format
        if isinstance(quotes_data, dict):
            for symbol, quote in quotes_data.items():
                if isinstance(quote, dict) and 'mark' in quote:
                    current_prices[symbol] = {
                        'price': quote['mark'],
                        'timestamp': datetime.now()
                    }
        
        return current_prices
    
    # Account-related methods (will require additional permissions)
    def get_account_info(self):
        """Get account information - requires account permissions"""
        url = f"{self.base_url}/trader/v1/accounts/accountNumbers"
        result = self._make_api_request(url)
        
        if result is None:
            self.logger.warning("Account access requires additional API permissions")
            self.logger.info("Your app needs 'Read Account and Positions' scope in Schwab Developer Portal")
            return None
        
        return result
    
    def get_current_positions(self, account_id):
        """Get current positions - requires account permissions"""
        url = f"{self.base_url}/trader/v1/accounts/{account_id}"
        params = {'fields': 'positions'}
        result = self._make_api_request(url, params)
        
        if result is None:
            self.logger.warning("Position access requires additional API permissions")
            return None
        
        # Extract positions from account data
        if isinstance(result, dict) and 'securitiesAccount' in result:
            return result['securitiesAccount'].get('positions', [])
        
        return []
    
    def calculate_portfolio_value(self, positions):
        """Calculate total portfolio value from positions"""
        if not positions:
            return 0
        
        total_value = 0
        for position in positions:
            market_value = position.get('marketValue', 0)
            if isinstance(market_value, (int, float)):
                total_value += market_value
        
        return total_value
    
    def place_order(self, account_id, order_data):
        """Place order - requires trading permissions (not implemented for safety)"""
        self.logger.warning("Order placement not implemented - requires additional permissions and testing")
        self.logger.info("For paper trading, use Schwab's web interface or mobile app")
        return None
    
    def test_connection(self):
        """Test API connection and return capabilities"""
        self.logger.info("Testing Schwab API connection...")
        
        capabilities = {
            'market_data': False,
            'account_access': False,
            'trading': False
        }
        
        # Test market data
        try:
            quotes = self.get_quotes('AAPL')
            if quotes:
                capabilities['market_data'] = True
                self.logger.info("✅ Market data access working")
            else:
                self.logger.error("❌ Market data access failed")
        except Exception as e:
            self.logger.error(f"❌ Market data test failed: {e}")
        
        # Test account access
        try:
            accounts = self.get_account_info()
            if accounts:
                capabilities['account_access'] = True
                self.logger.info("✅ Account access working")
            else:
                self.logger.warning("🔒 Account access requires additional permissions")
        except Exception as e:
            self.logger.warning(f"🔒 Account access test failed: {e}")
        
        return capabilities

