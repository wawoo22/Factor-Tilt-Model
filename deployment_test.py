from complete_system_integration import CompleteFactorMonitoringSystem, SystemConfig
import asyncio
import os
from dotenv import load_dotenv

async def run_deployment_test():
    print("FACTOR MONITORING SYSTEM - DEPLOYMENT TEST")
    print("="*50)
    
    load_dotenv()
    
    # Create test configuration
    config = SystemConfig(
        db_path="test_deployment.db",
        email_sender=os.getenv('FACTOR_EMAIL'),
        email_password=os.getenv('FACTOR_EMAIL_PASSWORD'),
        email_recipients=os.getenv('FACTOR_RECIPIENTS', '').split(','),
        tos_client_id=os.getenv('TOS_CLIENT_ID', ''),
        tos_refresh_token=os.getenv('TOS_REFRESH_TOKEN', ''),
        tos_account_id=os.getenv('TOS_ACCOUNT_ID', ''),
        portfolio_value=float(os.getenv('PORTFOLIO_VALUE', 100000)),
        target_allocations={
            'Value': float(os.getenv('TARGET_VALUE', 0.30)),
            'Growth': float(os.getenv('TARGET_GROWTH', 0.20)),
            'Quality': float(os.getenv('TARGET_QUALITY', 0.20)),
            'Low_Volatility': float(os.getenv('TARGET_LOW_VOLATILITY', 0.15)),
            'Momentum': float(os.getenv('TARGET_MOMENTUM', 0.10)),
            'Size': float(os.getenv('TARGET_SIZE', 0.05))
        }
    )
    
    # Initialize system
    print("Initializing Factor Monitoring System...")
    factor_system = CompleteFactorMonitoringSystem(config)
    
    # Test system startup
    print("Testing system startup...")
    startup_success = await factor_system.start_system()
    
    if startup_success:
        print("✅ System startup successful!")
        
        # Run one complete cycle
        print("Running test daily routine...")
        routine_success = await factor_system.run_daily_routine()
        
        if routine_success:
            print("✅ Daily routine completed successfully!")
            print("🎉 DEPLOYMENT TEST PASSED!")
            return True
        else:
            print("❌ Daily routine failed")
            return False
    else:
        print("❌ System startup failed")
        return False

if __name__ == "__main__":
    success = asyncio.run(run_deployment_test())
    
    if success:
        print("\n" + "="*50)
        print("READY FOR PRODUCTION DEPLOYMENT")
        print("="*50)
        print("\nNext steps:")
        print("1. Review test_deployment.db for sample data")
        print("2. Check email for test reports")
        print("3. Run: python complete_system_integration.py start")
    else:
        print("\n" + "="*50)
        print("DEPLOYMENT TEST FAILED")
        print("="*50)
        print("Please resolve issues before production deployment")
