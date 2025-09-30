#!/usr/bin/env python3
import os
import sys
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from complete_system_integration import CompleteFactorMonitoringSystem, SystemConfig

def load_production_config():
    """Load production configuration"""
    load_dotenv('.env.production')
    
    config = SystemConfig(
        db_path=os.getenv('DB_PATH', 'factor_monitoring_production.db'),
        email_sender=os.getenv('FACTOR_EMAIL'),
        email_password=os.getenv('FACTOR_EMAIL_PASSWORD'),
        email_recipients=os.getenv('FACTOR_RECIPIENTS', '').split(','),
        tos_client_id=os.getenv('TOS_CLIENT_ID', ''),
        tos_refresh_token=os.getenv('TOS_REFRESH_TOKEN', ''),
        tos_account_id=os.getenv('TOS_ACCOUNT_ID', ''),
        portfolio_value=float(os.getenv('PORTFOLIO_VALUE', 1000000)),
        target_allocations={
            'Value': float(os.getenv('TARGET_VALUE', 0.30)),
            'Growth': float(os.getenv('TARGET_GROWTH', 0.20)),
            'Quality': float(os.getenv('TARGET_QUALITY', 0.20)),
            'Low_Volatility': float(os.getenv('TARGET_LOW_VOLATILITY', 0.15)),
            'Momentum': float(os.getenv('TARGET_MOMENTUM', 0.10)),
            'Size': float(os.getenv('TARGET_SIZE', 0.05))
        },
        max_daily_trades=int(os.getenv('MAX_DAILY_TRADES', 25)),
        max_position_drift=float(os.getenv('MAX_POSITION_DRIFT', 0.05)),
        max_single_trade_pct=float(os.getenv('MAX_SINGLE_TRADE_PCT', 0.10))
    )
    
    return config

async def main():
    """Main production startup"""
    print("🚀 FACTOR MONITORING SYSTEM - PRODUCTION STARTUP")
    print("=" * 60)
    print(f"Start Time: {datetime.now()}")
    print("=" * 60)
    
    try:
        # Load configuration
        config = load_production_config()
        
        # Initialize system
        factor_system = CompleteFactorMonitoringSystem(config)
        
        # Start system
        startup_success = await factor_system.start_system()
        
        if startup_success:
            print("✅ Production system started successfully!")
            print("\n📊 System Status:")
            print(f"  Database: {config.db_path}")
            print(f"  Email: {config.email_sender}")
            print(f"  Portfolio Value: ${config.portfolio_value:,.2f}")
            print(f"  thinkorswim: {'Connected' if config.tos_client_id else 'Simulation Mode'}")
            
            print(f"\n🎯 Target Allocations:")
            for factor, allocation in config.target_allocations.items():
                print(f"  {factor}: {allocation:.1%}")
            
            print("\n🔄 System is now monitoring markets...")
            print("Press Ctrl+C to shutdown gracefully")
            
            # Enter monitoring loop
            factor_system.run_monitoring_loop()
            
        else:
            print("❌ Production startup failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Shutdown requested...")
    except Exception as e:
        print(f"\n💥 Production error: {e}")
        sys.exit(1)
    
    print("👋 Production system shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())
