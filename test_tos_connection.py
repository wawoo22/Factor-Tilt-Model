import os
from dotenv import load_dotenv
import requests

def test_tos_connection():
    load_dotenv()
    
    client_id = os.getenv('TOS_CLIENT_ID')
    refresh_token = os.getenv('TOS_REFRESH_TOKEN')
    
    if not client_id or not refresh_token:
        print("❌ TD Ameritrade credentials not found in .env file")
        return False
    
    # Test token refresh
    token_url = "https://api.tdameritrade.com/v1/oauth2/token"
    
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id
    }
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    try:
        response = requests.post(token_url, headers=headers, data=data)
        
        if response.status_code == 200:
            tokens = response.json()
            access_token = tokens['access_token']
            
            print("✅ TD Ameritrade API connection successful")
            
            # Test account data access
            accounts_url = "https://api.tdameritrade.com/v1/accounts"
            auth_headers = {'Authorization': f'Bearer {access_token}'}
            
            account_response = requests.get(accounts_url, headers=auth_headers)
            
            if account_response.status_code == 200:
                accounts = account_response.json()
                print(f"✅ Found {len(accounts)} account(s)")
                
                # Save account ID to .env
                if accounts:
                    account_id = accounts[0]['securitiesAccount']['accountId']
                    
                    with open('.env', 'a') as f:
                        f.write(f"TOS_ACCOUNT_ID={account_id}\n")
                    
                    print(f"✅ Account ID saved: {account_id}")
                
                return True
            else:
                print(f"❌ Account access failed: {account_response.status_code}")
                return False
        
        else:
            print(f"❌ Token refresh failed: {response.status_code}")
            print(response.text)
            return False
            
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

if __name__ == "__main__":
    test_tos_connection()
