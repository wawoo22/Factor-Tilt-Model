#!/usr/bin/env python3
"""
Charles Schwab API - Get Refresh Token Script
Reworked for Schwab API credentials

IMPORTANT: Make sure you have:
1. Registered your app at https://developer.schwab.com/
2. Your app is approved and active
3. You have your Client ID and callback URL configured
"""

import requests
import urllib.parse as up
from urllib.parse import unquote, quote
import json
import base64
import os
from datetime import datetime

class SchwabAuthenticator:
    """Handles Schwab API OAuth2 authentication"""
    
    def __init__(self, client_id, redirect_uri, client_secret=None):
        self.client_id = client_id
        self.redirect_uri = redirect_uri
        self.client_secret = client_secret  # Schwab uses client secret for some flows
        
        # Schwab API endpoints
        self.auth_url = "https://api.schwabapi.com/v1/oauth/authorize"
        self.token_url = "https://api.schwabapi.com/v1/oauth/token"
        
        print(f"🦆 Schwab API Authenticator Initialized")
        print(f"   Client ID: {self.client_id}")
        print(f"   Redirect URI: {self.redirect_uri}")
    
    def get_authorization_url(self, scope="readonly"):
        """Generate authorization URL for user to visit"""
        
        params = {
            'response_type': 'code',
            'client_id': self.client_id,
            'scope': scope,
            'redirect_uri': self.redirect_uri
        }
        
        # Build URL with proper encoding
        auth_url = f"{self.auth_url}?" + "&".join([
            f"{key}={quote(str(value))}" for key, value in params.items()
        ])
        
        return auth_url
    
    def extract_code_from_callback(self, callback_url):
        """Extract authorization code from callback URL"""
        try:
            # Parse the URL
            parsed_url = up.urlparse(callback_url)
            
            # Extract query parameters
            query_params = up.parse_qs(parsed_url.query)
            
            if 'code' in query_params:
                code = query_params['code'][0]
                print(f"✅ Authorization code extracted successfully")
                print(f"   Code length: {len(code)} characters")
                return unquote(code)
            else:
                print("❌ No authorization code found in URL")
                print(f"   Available parameters: {list(query_params.keys())}")
                
                # Check for error
                if 'error' in query_params:
                    error = query_params['error'][0]
                    error_desc = query_params.get('error_description', ['Unknown'])[0]
                    print(f"❌ Error in callback: {error}")
                    print(f"   Description: {error_desc}")
                
                return None
                
        except Exception as e:
            print(f"❌ Error parsing callback URL: {e}")
            return None
    
    def exchange_code_for_tokens(self, authorization_code):
        """Exchange authorization code for access and refresh tokens"""
        
        print("🔄 Exchanging authorization code for tokens...")
        
        # Prepare token request data
        token_data = {
            'grant_type': 'authorization_code',
            'code': authorization_code,
            'client_id': self.client_id,
            'redirect_uri': self.redirect_uri
        }
        
        # Schwab uses Basic authentication with client credentials
        auth_header = None
        if self.client_secret:
            # Encode client credentials for Basic auth
            credentials = f"{self.client_id}:{self.client_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            auth_header = f"Basic {encoded_credentials}"
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        
        if auth_header:
            headers['Authorization'] = auth_header
        
        try:
            print("📤 Sending token request to Schwab...")
            
            response = requests.post(
                self.token_url, 
                data=token_data, 
                headers=headers,
                timeout=30
            )
            
            print(f"📥 Response status: {response.status_code}")
            
            if response.status_code == 200:
                tokens = response.json()
                
                print("✅ SUCCESS! Tokens received from Schwab")
                
                # Display token information (safely)
                print(f"\n🔑 TOKEN INFORMATION:")
                print(f"   Access Token: {tokens.get('access_token', 'N/A')[:20]}...")
                
                if 'refresh_token' in tokens:
                    print(f"   Refresh Token: {tokens['refresh_token'][:20]}...")
                    print(f"   Refresh Token Length: {len(tokens['refresh_token'])} chars")
                else:
                    print("   Refresh Token: Not provided (may require different scope)")
                
                print(f"   Token Type: {tokens.get('token_type', 'N/A')}")
                print(f"   Expires In: {tokens.get('expires_in', 'N/A')} seconds")
                print(f"   Scope: {tokens.get('scope', 'N/A')}")
                
                return tokens
                
            else:
                print(f"❌ Token request failed with status {response.status_code}")
                print(f"   Response: {response.text}")
                
                try:
                    error_data = response.json()
                    print(f"   Error: {error_data.get('error', 'Unknown')}")
                    print(f"   Description: {error_data.get('error_description', 'No description')}")
                except:
                    pass
                
                return None
                
        except requests.exceptions.Timeout:
            print("❌ Request timed out - check your internet connection")
            return None
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            return None
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return None
    
    def save_tokens_to_env(self, tokens):
        """Save tokens to .env file"""
        try:
            env_lines = []
            
            # Read existing .env file
            if os.path.exists('.env'):
                with open('.env', 'r') as f:
                    env_lines = f.readlines()
            
            # Remove existing Schwab token lines
            env_lines = [line for line in env_lines if not any(
                schwab_var in line for schwab_var in [
                    'SCHWAB_CLIENT_ID', 'SCHWAB_REFRESH_TOKEN', 'SCHWAB_ACCESS_TOKEN'
                ]
            )]
            
            # Add new token information
            env_lines.append(f"\n# Schwab API Configuration - Added {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            env_lines.append(f"SCHWAB_CLIENT_ID={self.client_id}\n")
            
            if 'refresh_token' in tokens:
                env_lines.append(f"SCHWAB_REFRESH_TOKEN={tokens['refresh_token']}\n")
            
            if 'access_token' in tokens:
                env_lines.append(f"SCHWAB_ACCESS_TOKEN={tokens['access_token']}\n")
            
            # Write back to .env file
            with open('.env', 'w') as f:
                f.writelines(env_lines)
            
            print(f"✅ Tokens saved to .env file")
            print(f"   File location: {os.path.abspath('.env')}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to save tokens: {e}")
            return False

def get_schwab_refresh_token():
    """Main function to get Schwab refresh token"""
    
    print("🦆 SCHWAB API - REFRESH TOKEN SETUP")
    print("=" * 60)
    
    # Get credentials from user with proper defaults
    print("📝 Enter your credentials (or press Enter to use defaults):")
    print()
    
    client_id = input("🔑 Client ID [default: g7ylG3PXLk86ZaMELrDQ5sP3ndDJqwDLDSvTZFBqmQSD5kav]: ").strip()
    if not client_id:
        client_id = "g7ylG3PXLk86ZaMELrDQ5sP3ndDJqwDLDSvTZFBqmQSD5kav"
        print(f"   ✅ Using default Client ID")
    
    client_secret = input("🔑 Client Secret [default: Z870coS5JfXinRckHPDOwBpjf8PCgAthphUIJuYPGAMjfSGiKzzn1HNYD0NLM5W4]: ").strip()
    if not client_secret:
        client_secret = "Z870coS5JfXinRckHPDOwBpjf8PCgAthphUIJuYPGAMjfSGiKzzn1HNYD0NLM5W4"
        print(f"   ✅ Using default Client Secret")
    
    redirect_uri = input("🔗 Redirect URI [default: https://127.0.0.1]: ").strip()
    if not redirect_uri:
        redirect_uri = "https://127.0.0.1"
        print(f"   ✅ Using default Redirect URI")
    
    print()
    print(f"📋 Using credentials:")
    print(f"   Client ID: {client_id}")
    print(f"   Client Secret: {client_secret[:10]}...{client_secret[-5:]}")
    print(f"   Redirect URI: {redirect_uri}")
    
    # Initialize authenticator
    authenticator = SchwabAuthenticator(client_id, redirect_uri, client_secret)
    
    # Step 1: Generate authorization URL
    print(f"\n🔗 STEP 1: Authorization URL Generated")
    auth_url = authenticator.get_authorization_url(scope="readonly")
    
    print(f"\n🔗 AUTHORIZATION URL:")
    print(f"{auth_url}")
    
    print(f"\n📋 INSTRUCTIONS:")
    print(f"1. 🌐 COPY the URL above and paste it into your browser")
    print(f"2. 🔐 Log in to your Schwab account") 
    print(f"3. ✅ Click 'Authorize' or 'Allow' when prompted")
    print(f"4. ⚠️  Your browser will try to go to {redirect_uri} and show an error")
    print(f"5. 📋 COPY the ENTIRE URL from your browser address bar")
    print(f"6. 📥 PASTE it below")
    
    print(f"\n" + "=" * 60)
    
    # Step 2: Get callback URL from user
    print(f"🔄 STEP 2: Get Authorization Code")
    
    while True:
        callback_url = input("\n📥 Paste the full callback URL here: ").strip()
        
        if not callback_url:
            print("❌ No URL entered. Please paste the callback URL.")
            continue
        
        if redirect_uri not in callback_url:
            print(f"⚠️  Warning: URL doesn't contain expected redirect URI")
            print(f"   Expected: {redirect_uri}")
            print(f"   Got: {callback_url[:50]}...")
            
            continue_anyway = input("   Continue anyway? (y/N): ").strip().lower()
            if continue_anyway != 'y':
                continue
        
        # Extract authorization code
        auth_code = authenticator.extract_code_from_callback(callback_url)
        
        if auth_code:
            break
        else:
            print("❌ Could not extract authorization code. Please try again.")
            continue
    
    # Step 3: Exchange code for tokens
    print(f"\n🔄 STEP 3: Exchange Code for Tokens")
    tokens = authenticator.exchange_code_for_tokens(auth_code)
    
    if not tokens:
        print("❌ Failed to get tokens. Check your setup and try again.")
        return False
    
    # Step 4: Save tokens
    print(f"\n💾 STEP 4: Save Tokens")
    saved = authenticator.save_tokens_to_env(tokens)
    
    if saved:
        print(f"\n🎉 SUCCESS! Schwab API setup complete!")
        print(f"\n📋 WHAT'S NEXT:")
        print(f"   1. ✅ Your tokens are saved in .env file")
        print(f"   2. 🔧 Update your Factor Monitoring System to use Schwab API")
        print(f"   3. 📊 Test the connection with your new credentials")
        
        # Display next steps for integration
        print(f"\n🔧 INTEGRATION STEPS:")
        print(f"   Add to your Python code:")
        print(f"   ```python")
        print(f"   from dotenv import load_dotenv")
        print(f"   import os")
        print(f"   ")
        print(f"   load_dotenv()")
        print(f"   schwab_client_id = os.getenv('SCHWAB_CLIENT_ID')")
        print(f"   schwab_refresh_token = os.getenv('SCHWAB_REFRESH_TOKEN')")
        print(f"   ```")
        
        return True
    else:
        print("❌ Failed to save tokens. Please save them manually.")
        return False

def test_saved_credentials():
    """Test the saved credentials"""
    print("\n🧪 TESTING SAVED CREDENTIALS")
    print("=" * 40)
    
    try:
        from dotenv import load_dotenv
        load_dotenv()
        
        client_id = os.getenv('SCHWAB_CLIENT_ID')
        refresh_token = os.getenv('SCHWAB_REFRESH_TOKEN')
        access_token = os.getenv('SCHWAB_ACCESS_TOKEN')
        
        print(f"📋 Credentials Check:")
        print(f"   Client ID: {'✅ Found' if client_id else '❌ Missing'}")
        print(f"   Refresh Token: {'✅ Found' if refresh_token else '❌ Missing'}")
        print(f"   Access Token: {'✅ Found' if access_token else '❌ Missing'}")
        
        if client_id and refresh_token:
            print(f"\n✅ Credentials look good!")
            print(f"   Client ID: {client_id}")
            print(f"   Refresh Token: {refresh_token[:20]}...{refresh_token[-10:]}")
            return True
        else:
            print(f"\n❌ Missing required credentials")
            return False
            
    except ImportError:
        print("⚠️  python-dotenv not installed. Install with: pip install python-dotenv")
        return False
    except Exception as e:
        print(f"❌ Error testing credentials: {e}")
        return False

if __name__ == "__main__":
    print("🦆 SCHWAB API AUTHENTICATION SETUP")
    print("This script will help you get refresh tokens for the Schwab API")
    print("(Successor to TD Ameritrade API)")
    print("=" * 60)
    
    # Check if user wants to test existing credentials first
    if os.path.exists('.env'):
        test_first = input("🔍 .env file exists. Test existing credentials first? (y/N): ").strip().lower()
        if test_first == 'y':
            if test_saved_credentials():
                print("\n✅ Existing credentials work! No need to re-authenticate.")
                exit(0)
            else:
                print("\n🔄 Existing credentials don't work. Continuing with new authentication...")
    
    # Run the main authentication flow
    success = get_schwab_refresh_token()
    
    if success:
        print("\n🎉 SETUP COMPLETE!")
        
        # Test the newly saved credentials
        test_saved_credentials()
        
        print("\nYour Factor Monitoring System is now ready for Schwab API integration!")
    else:
        print("\n❌ SETUP FAILED!")
        print("Please check the error messages above and try again.")
        print("\n💡 Common issues:")
        print("   - Wrong client ID or redirect URI")
        print("   - App not approved in Schwab Developer Portal") 
        print("   - Network/firewall issues")
        print("   - Incorrect callback URL copied")
