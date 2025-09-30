#!/usr/bin/env python3
"""
Quick start script for factor analysis
Fixed import issues
"""

import os
import sys
import logging
from datetime import datetime

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def quick_analysis():
    """Run a quick factor analysis"""
    print("🚀 Starting Quick Factor Analysis...")
    print("=" * 50)
    
    try:
        # Import after fixing path
        from main import FactorInvestmentSystem
        
        # Initialize system
        print("🔧 Initializing Factor Investment System...")
        system = FactorInvestmentSystem()
        
        # Run analysis (use cache for speed)
        print("📊 Running analysis...")
        results = system.run_full_analysis(use_cache=True, analyze_current=False)
        
        if results:
            weights = results['weights']
            
            print("\n✅ Analysis Complete!")
            print(f"Generated portfolio with {len(weights)} stocks")
            
            # Show top 10 holdings
            sorted_weights = sorted(weights.items(), key=lambda x: x[1], reverse=True)
            print("\n🏆 Top 10 Holdings:")
            for i, (symbol, weight) in enumerate(sorted_weights[:10], 1):
                print(f"  {i:2d}. {symbol}: {weight:.2%}")
            
            return weights
        else:
            print("❌ Analysis failed")
            return None
            
    except ImportError as e:
        print(f"❌ Import Error: {e}")
        print("\n🔧 Let's try a simpler approach...")
        return run_simple_analysis()
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\n🔧 Let's try a simpler approach...")
        return run_simple_analysis()

def run_simple_analysis():
    """Run a simplified analysis without complex imports"""
    print("\n🔧 Running Simplified Factor Analysis...")
    print("=" * 50)
    
    try:
        # Basic imports
        import yfinance as yf
        import pandas as pd
        import numpy as np
        from dotenv import load_dotenv
        
        # Load environment
        load_dotenv()
        
        # Test Schwab API connection
        print("🔍 Testing Schwab API connection...")
        
        client_id = os.getenv('SCHWAB_CLIENT_ID')
        client_secret = os.getenv('SCHWAB_CLIENT_SECRET')
        refresh_token = os.getenv('SCHWAB_REFRESH_TOKEN')
        
        if client_id and client_secret and refresh_token:
            print("✅ Schwab credentials found")
            
            # Test API connection
            import requests
            import base64
            
            # Get access token
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
            credentials = f"{client_id}:{client_secret}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers['Authorization'] = f"Basic {encoded_credentials}"
            
            response = requests.post(token_url, data=token_data, headers=headers, timeout=30)
            
            if response.status_code == 200:
                tokens = response.json()
                access_token = tokens['access_token']
                print("✅ Schwab API connected successfully!")
                
                # Test market data
                api_headers = {'Authorization': f'Bearer {access_token}', 'Accept': 'application/json'}
                quotes_response = requests.get(
                    'https://api.schwabapi.com/marketdata/v1/quotes?symbols=AAPL', 
                    headers=api_headers, 
                    timeout=10
                )
                
                if quotes_response.status_code == 200:
                    print("✅ Market data access working!")
                else:
                    print(f"⚠️ Market data test failed: {quotes_response.status_code}")
            else:
                print(f"❌ Schwab API connection failed: {response.status_code}")
        else:
            print("⚠️ Schwab credentials missing - will use Yahoo Finance only")
        
        # Simple factor analysis using Yahoo Finance
        print("\n📊 Running basic factor analysis...")
        
        # Sample stock universe
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'JNJ', 'V']
        
        print(f"📈 Analyzing {len(symbols)} stocks...")
        
        stock_data = {}
        factor_scores = {}
        
        for symbol in symbols:
            try:
                print(f"  📥 Fetching {symbol}...")
                ticker = yf.Ticker(symbol)
                
                # Get price data
                hist = ticker.history(period="1y")
                info = ticker.info
                
                if len(hist) > 50:  # Sufficient data
                    # Calculate simple momentum
                    current_price = hist['Close'][-1]
                    past_price = hist['Close'][-252] if len(hist) >= 252 else hist['Close'][0]
                    momentum = (current_price / past_price) - 1
                    
                    # Calculate volatility
                    returns = hist['Close'].pct_change().dropna()
                    volatility = returns.std() * np.sqrt(252)
                    
                    # Get valuation metrics
                    pe_ratio = info.get('trailingPE', 0)
                    market_cap = info.get('marketCap', 0)
                    
                    # Simple factor score
                    score = 0
                    if momentum > 0:
                        score += 0.3  # Positive momentum
                    if volatility < 0.3:
                        score += 0.2  # Low volatility
                    if pe_ratio > 0 and pe_ratio < 25:
                        score += 0.2  # Reasonable valuation
                    if market_cap > 10e9:
                        score += 0.3  # Large cap stability
                    
                    factor_scores[symbol] = score
                    stock_data[symbol] = {
                        'momentum': momentum,
                        'volatility': volatility,
                        'pe_ratio': pe_ratio,
                        'market_cap': market_cap,
                        'score': score
                    }
                
            except Exception as e:
                print(f"  ❌ Failed to process {symbol}: {e}")
        
        # Create simple portfolio
        if factor_scores:
            print(f"\n✅ Analysis complete! Processed {len(factor_scores)} stocks")
            
            # Sort by factor score
            sorted_stocks = sorted(factor_scores.items(), key=lambda x: x[1], reverse=True)
            
            print("\n🏆 Top Stocks by Factor Score:")
            for i, (symbol, score) in enumerate(sorted_stocks[:10], 1):
                data = stock_data[symbol]
                print(f"  {i:2d}. {symbol}: {score:.2f} "
                      f"(Mom: {data['momentum']:+.1%}, Vol: {data['volatility']:.1%}, "
                      f"P/E: {data['pe_ratio']:.1f})")
            
            # Create equal-weight portfolio of top stocks
            top_stocks = [symbol for symbol, _ in sorted_stocks[:10]]
            portfolio_weights = {symbol: 1.0/len(top_stocks) for symbol in top_stocks}
            
            print(f"\n📊 Created equal-weight portfolio with {len(portfolio_weights)} holdings")
            
            return portfolio_weights
        else:
            print("❌ No valid stock data collected")
            return None
            
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print("💡 Install with: pip install yfinance pandas numpy python-dotenv requests")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def main():
    """Main entry point"""
    print("🎯 Factor Analysis Quick Start")
    print("Testing system and running analysis...")
    print("=" * 60)
    
    # Try full analysis first, fall back to simple if needed
    results = quick_analysis()
    
    if results:
        print(f"\n🎉 SUCCESS!")
        print(f"✅ Factor analysis completed")
        print(f"📊 Portfolio created with {len(results)} holdings")
        print(f"\n💡 Next steps:")
        print(f"   1. Review the analysis results above")
        print(f"   2. Run the full system: python main.py")
        print(f"   3. Check output files in the 'output' directory")
    else:
        print(f"\n❌ Analysis failed")
        print(f"💡 Troubleshooting:")
        print(f"   1. Check that all required packages are installed")
        print(f"   2. Verify your .env file has the correct credentials")
        print(f"   3. Check your internet connection")
        print(f"   4. Review the error messages above")

if __name__ == "__main__":
    main()
