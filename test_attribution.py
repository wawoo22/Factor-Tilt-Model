from performance_attribution_system import PerformanceAttributionSystem
from datetime import datetime, timedelta

def test_attribution():
    print("Testing Performance Attribution System...")
    
    attribution_system = PerformanceAttributionSystem("test_attribution.db")
    
    # Test with sample portfolio
    portfolio_weights = {
        'Value': 0.30,
        'Growth': 0.20,
        'Quality': 0.20,
        'Low_Volatility': 0.15,
        'Momentum': 0.10,
        'Size': 0.05
    }
    
    # Test attribution for last 30 days
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    try:
        attribution_result = attribution_system.generate_comprehensive_attribution_report(
            start_date=start_date,
            end_date=end_date,
            portfolio_weights=portfolio_weights
        )
        
        if attribution_result:
            print("✅ Attribution analysis successful!")
            print(f"Total Return: {attribution_result.total_return:.2%}")
            print(f"Benchmark Return: {attribution_result.benchmark_return:.2%}")
            print(f"Excess Return: {attribution_result.excess_return:.2%}")
            
            print("\nFactor Contributions:")
            for factor, contribution in attribution_result.factor_contributions.items():
                print(f"  {factor}: {contribution:.2%}")
            
            return True
        else:
            print("❌ Attribution analysis failed")
            return False
            
    except Exception as e:
        print(f"❌ Attribution test failed: {e}")
        return False

if __name__ == "__main__":
    test_attribution()
