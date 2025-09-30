#!/usr/bin/env python3
"""
Simple Factor Monitoring System
Main application that ties everything together
"""

import time
import schedule
from datetime import datetime
import logging
from factor_data_collector import SimpleFactorCollector
from simple_email_system import SimpleEmailSystem

class SimpleFactorMonitor:
    """Main Factor Monitoring Application"""
    
    def __init__(self):
        self.data_collector = SimpleFactorCollector()
        self.email_system = SimpleEmailSystem()
        self.running = False
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('factor_monitor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def run_daily_routine(self):
        """Run the daily monitoring routine"""
        try:
            print("\n" + "="*60)
            print(f"🚀 DAILY FACTOR ROUTINE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*60)
            
            # Step 1: Collect market data
            print("\n📊 Step 1: Collecting market data...")
            results = self.data_collector.run_collection()
            
            if not results:
                print("❌ Data collection failed - skipping email")
                return False
            
            # Step 2: Send email report
            print("\n📧 Step 2: Sending email report...")
            email_sent = self.email_system.send_report(
                results['data'], 
                results['alerts']
            )
            
            if email_sent:
                print("✅ Daily routine completed successfully!")
            else:
                print("⚠️  Daily routine completed with email issues")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Daily routine failed: {e}")
            print(f"❌ Daily routine failed: {e}")
            return False
    
    def schedule_operations(self):
        """Schedule automated operations"""
        # Schedule daily report at market close + 1 hour (5:00 PM EST)
        schedule.every().day.at("17:00").do(self.run_daily_routine)
        
        # Schedule morning update at 9:00 AM EST
        schedule.every().day.at("09:00").do(self.run_daily_routine)
        
        print("⏰ Scheduled operations:")
        print("   📊 Morning update: 9:00 AM EST")
        print("   📊 Evening report: 5:00 PM EST")
    
    def run_manual(self):
        """Run monitoring manually (for testing)"""
        print("🔧 MANUAL RUN MODE")
        return self.run_daily_routine()
    
    def start_monitoring(self):
        """Start continuous monitoring"""
        print("\n🚀 STARTING FACTOR MONITORING SYSTEM")
        print("="*60)
        
        # Test systems first
        print("🧪 Testing systems...")
        
        # Test data collection
        print("   Testing data collection...")
        test_data = self.data_collector.collect_data()
        if not test_data:
            print("❌ Data collection test failed")
            return False
        print("   ✅ Data collection working")
        
        # Test email system
        print("   Testing email system...")
        if not self.email_system.sender_email:
            print("❌ Email system not configured")
            return False
        print("   ✅ Email system configured")
        
        print("\n✅ All systems operational!")
        
        # Schedule operations
        self.schedule_operations()
        
        # Run initial routine
        print("\n🎬 Running initial routine...")
        self.run_daily_routine()
        
        # Start monitoring loop
        self.running = True
        print(f"\n⏰ Monitoring started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("📊 System will run scheduled updates automatically")
        print("⏹️  Press Ctrl+C to stop")
        
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            print("\n\n⏹️  Shutdown requested...")
            self.stop_monitoring()
    
    def stop_monitoring(self):
        """Stop monitoring system"""
        self.running = False
        print("✅ Factor Monitoring System stopped")
        print(f"📊 Final run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Main entry point"""
    monitor = SimpleFactorMonitor()
    
    print("SIMPLE FACTOR MONITORING SYSTEM")
    print("="*40)
    print("1. Manual run (test)")
    print("2. Start automated monitoring")
    print("3. Exit")
    
    choice = input("\nSelect option (1-3): ").strip()
    
    if choice == "1":
        print("\n🔧 Running manual test...")
        success = monitor.run_manual()
        if success:
            print("✅ Manual test completed successfully!")
        else:
            print("❌ Manual test failed!")
            
    elif choice == "2":
        print("\n⏰ Starting automated monitoring...")
        monitor.start_monitoring()
        
    elif choice == "3":
        print("👋 Goodbye!")
        
    else:
        print("❌ Invalid choice")

if __name__ == "__main__":
    main()
