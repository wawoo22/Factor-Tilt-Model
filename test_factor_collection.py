# test_factor_collection.py
from factor_data_collection import FactorDataCollector
import pandas as pd

def test_data_collection():
    print("Testing Factor Data Collection...")
    
    collector = FactorDataCollector("test_factor_data.db")
    
    # Test data collection
    results = collector.run_daily_collection()
    
    if results and results['data'] is not None:
        print("✅ Data collection successful!")
        print(f"Data shape: {results['data'].shape}")
        print(f"Columns: {list(results['data'].columns)}")
        print(f"Latest date: {results['data'].index[-1]}")
        print(f"Alerts generated: {len(results['alerts'])}")
        
        # Show sample data
        print("\nSample data:")
        print(results['data'].tail())
        
        return True
    else:
        print("❌ Data collection failed")
        return False

if __name__ == "__main__":
    test_data_collection(
