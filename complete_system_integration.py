#!/usr/bin/env python3
"""
Complete Factor Monitoring System Integration
Fixed syntax error in class definition
"""

import asyncio
import schedule
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import os
from dataclasses import dataclass

# Import our custom modules
from factor_data_collection import FactorDataCollector
from email_alert_system import FactorAlertSystem, EmailConfig
from record_keeping_system import FactorRecordKeeping, FactorRecordIntegration
from performance_attribution_system import PerformanceAttributionSystem

@dataclass
class SystemConfig:
    """Complete system configuration"""
    # Database settings
    db_path: str = "factor_monitoring.db"
    
    # Email settings
    email_sender: str = ""
    email_password: str = ""
    email_recipients: List[str] = None
    
    # Schwab API settings
    schwab_client_id: str = ""
    schwab_refresh_token: str = ""
    schwab_account_id: str = ""
    
    # Portfolio settings
    portfolio_value: float = 1000000.0
    target_allocations: Dict[str, float] = None
    
    # Risk limits
    max_daily_trades: int = 20
    max_position_drift: float = 0.05  # 5%
    max_single_trade_pct: float = 0.10  # 10%
    
    def __post_init__(self):
        if self.email_recipients is None:
            self.email_recipients = []
        if self.target_allocations is None:
            self.target_allocations = {
                'Value': 0.30,
                'Growth': 0.20,
                'Quality': 0.20,
                'Low_Volatility': 0.15,
                'Momentum': 0.10,
                'Size': 0.05
            }

class CompleteFactorMonitoringSystem:
    """
    Master class that integrates all factor monitoring components
    FIXED: Removed syntax error in class name
    """
    
    def __init__(self, config: SystemConfig):
        self.config = config
        self.logger = self.setup_logging()
        
        # Initialize all components
        self.data_collector = FactorDataCollector(config.db_path)
        self.email_system = self.setup_email_system()
        self.record_keeper = FactorRecordKeeping(config.db_path)
        self.attribution_system = PerformanceAttributionSystem(config.db_path)
        
        # Integration layer
        self.record_integration = FactorRecordIntegration(
            self.data_collector, 
            self.record_keeper
        )
        
        # System status
        self.system_active = False
        self.last_rebalance_date = None
        self.daily_trade_count = 0
        
    def setup_logging(self):
        """Setup comprehensive logging system"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('factor_system.log'),
                logging.StreamHandler()
            ]
        )
        
        logger = logging.getLogger('FactorSystem')
        return logger
    
    def setup_email_system(self):
        """Initialize email alert system"""
        try:
            email_config = EmailConfig(
                sender_email=self.config.email_sender,
                sender_password=self.config.email_password,
                recipient_emails=self.config.email_recipients
            )
            
            return FactorAlertSystem(email_config, self.config.db_path)
            
        except Exception as e:
            self.logger.error(f"Failed to setup email system: {e}")
            return None
    
    async def run_daily_routine(self):
        """Execute complete daily monitoring routine"""
        try:
            self.logger.info("Starting daily routine")
            
            # 1. Collect latest market data
            data_results = self.data_collector.run_daily_collection()
            if not data_results:
                self.logger.error("Data collection failed")
                return False
            
            # 2. Get current portfolio state (simulated for now)
            current_positions = self.get_simulated_positions()
            
            # 3. Calculate performance attribution
            attribution_result = self.run_performance_attribution()
            
            # 4. Check for rebalancing needs
            rebalancing_plan = self.assess_rebalancing_needs(current_positions)
            
            # 5. Simulate trades (no actual execution)
            if rebalancing_plan['trades_needed']:
                trade_results = await self.simulate_rebalancing_trades(rebalancing_plan)
            else:
                trade_results = {'message': 'No rebalancing needed'}
            
            # 6. Record all activities
            self.record_daily_activities(data_results, current_positions, 
                                       attribution_result, rebalancing_plan, trade_results)
            
            # 7. Generate and send reports
            await self.send_daily_reports(data_results, attribution_result, trade_results)
            
            self.logger.info("Daily routine completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Daily routine failed: {e}")
            await self.send_error_alert(f"Daily routine failed: {str(e)}")
            return False
    
    def get_simulated_positions(self):
        """Get simulated positions when live trading is unavailable"""
        return {
            'total_value': self.config.portfolio_value,
            'allocations': self.config.target_allocations.copy(),
            'positions': {},
            'simulated': True,
            'last_updated': datetime.now().isoformat()
        }
    
    def run_performance_attribution(self):
        """Run performance attribution analysis"""
        try:
            # Get last 30 days for attribution
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            
            attribution_result = self.attribution_system.generate_comprehensive_attribution_report(
                start_date=start_date,
                end_date=end_date,
                portfolio_weights=self.config.target_allocations
            )
            
            return attribution_result
            
        except Exception as e:
            self.logger.error(f"Performance attribution failed: {e}")
            return None
    
    def assess_rebalancing_needs(self, current_positions):
        """Assess if portfolio rebalancing is needed"""
        try:
            current_allocations = current_positions.get('allocations', {})
            target_allocations = self.config.target_allocations
            
            trades_needed = False
            suggested_trades = []
            
            for factor, target_weight in target_allocations.items():
                current_weight = current_allocations.get(factor, 0.0)
                drift = abs(current_weight - target_weight)
                
                if drift > self.config.max_position_drift:
                    trades_needed = True
                    
                    # Calculate trade size
                    total_value = current_positions['total_value']
                    target_value = total_value * target_weight
                    current_value = total_value * current_weight
                    trade_value = target_value - current_value
                    
                    symbol = self.get_symbol_from_factor(factor)
                    if symbol:
                        suggested_trades.append({
                            'factor': factor,
                            'symbol': symbol,
                            'action': 'BUY' if trade_value > 0 else 'SELL',
                            'value': abs(trade_value),
                            'reason': f'Rebalance {factor}: {current_weight:.1%} → {target_weight:.1%}'
                        })
            
            rebalancing_plan = {
                'trades_needed': trades_needed,
                'suggested_trades': suggested_trades,
                'total_trades': len(suggested_trades),
                'estimated_cost': self.estimate_trading_costs(suggested_trades),
                'assessment_time': datetime.now().isoformat()
            }
            
            return rebalancing_plan
            
        except Exception as e:
            self.logger.error(f"Rebalancing assessment failed: {e}")
            return {'trades_needed': False, 'error': str(e)}
    
    def get_symbol_from_factor(self, factor):
        """Map factor name to ETF symbol"""
        factor_map = {
            'Value': 'VTV',
            'Growth': 'VUG',
            'Quality': 'QUAL',
            'Momentum': 'MTUM',
            'Low_Volatility': 'USMV',
            'Size': 'VB'
        }
        return factor_map.get(factor)
    
    def estimate_trading_costs(self, trades):
        """Estimate transaction costs for proposed trades"""
        total_cost = 0.0
        
        for trade in trades:
            # Assume 0.5 bps spread cost
            spread_cost = trade['value'] * 0.0005
            total_cost += spread_cost
        
        return total_cost
    
    async def simulate_rebalancing_trades(self, rebalancing_plan):
        """Simulate rebalancing trades (no actual execution)"""
        try:
            self.logger.info("SIMULATION MODE: No actual trades executed")
            
            simulated_orders = []
            
            for trade in rebalancing_plan['suggested_trades']:
                # Simulate the trade
                estimated_price = 100.0  # Placeholder
                quantity = int(trade['value'] / estimated_price)
                
                if quantity > 0:
                    simulated_orders.append({
                        'factor': trade['factor'],
                        'symbol': trade['symbol'],
                        'action': trade['action'],
                        'quantity': quantity,
                        'estimated_price': estimated_price,
                        'reason': trade['reason'],
                        'status': 'SIMULATED'
                    })
                    
                    self.daily_trade_count += 1
            
            return {
                'orders_simulated': len(simulated_orders),
                'total_value_traded': sum([t['value'] for t in rebalancing_plan['suggested_trades']]),
                'mode': 'SIMULATION',
                'trades': simulated_orders
            }
            
        except Exception as e:
            self.logger.error(f"Trade simulation failed: {e}")
            return {'error': str(e)}
    
    def record_daily_activities(self, data_results, positions, attribution, rebalancing, trades):
        """Record all daily activities for compliance"""
        try:
            # Create daily portfolio snapshot
            portfolio_data = {
                'total_value': positions['total_value'],
                'factor_allocations': positions['allocations'],
                'performance_metrics': attribution.risk_adjusted_metrics if attribution else {},
                'market_conditions': {
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'alerts_generated': len(data_results.get('alerts', [])),
                    'rebalancing_executed': rebalancing['trades_needed']
                }
            }
            
            self.record_keeper.create_daily_snapshot(portfolio_data)
            
            # Log compliance events
            if rebalancing['trades_needed']:
                self.record_keeper.log_compliance_event(
                    event_type="REBALANCING_SIMULATED",
                    description=f"Simulated {len(trades.get('orders_simulated', []))} rebalancing trades",
                    severity="MEDIUM"
                )
            
            self.logger.info("Daily activities recorded successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to record daily activities: {e}")
    
    async def send_daily_reports(self, data_results, attribution, trade_results):
        """Send comprehensive daily reports"""
        try:
            if not self.email_system:
                self.logger.warning("Email system not available")
                return
            
            # Send daily factor report
            await asyncio.to_thread(self.email_system.send_daily_report)
            
            self.logger.info("Daily reports sent successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to send daily reports: {e}")
    
    async def send_error_alert(self, error_message):
        """Send immediate error alert"""
        try:
            if self.email_system:
                await asyncio.to_thread(
                    self.email_system.send_email,
                    "🚨 Factor System Error Alert",
                    f"<html><body><h2>System Error</h2><p>{error_message}</p><p>Time: {datetime.now()}</p></body></html>"
                )
        except:
            pass  # Don't let email errors crash the system
    
    def schedule_operations(self):
        """Schedule all automated operations"""
        # Daily data collection at market close + 30 minutes
        schedule.every().day.at("16:30").do(lambda: asyncio.run(self.run_daily_routine()))
        
        self.logger.info("Operations scheduled successfully")
    
    async def start_system(self):
        """Start the complete factor monitoring system"""
        try:
            self.start_time = datetime.now()
            self.system_active = True
            
            self.logger.info("="*60)
            self.logger.info("FACTOR MONITORING SYSTEM STARTUP")
            self.logger.info("="*60)
            
            # Test all connections
            self.logger.info("Testing system components...")
            
            # Test database
            if self.test_database_connection():
                self.logger.info("✅ Database connection: OK")
            else:
                self.logger.error("❌ Database connection: FAILED")
                return False
            
            # Test email system
            if self.email_system:
                self.logger.info("✅ Email system: CONFIGURED")
            else:
                self.logger.warning("⚠️ Email system: NOT CONFIGURED")
            
            self.logger.info("✅ Running in SIMULATION MODE (no live trading)")
            
            # Schedule operations
            self.schedule_operations()
            
            # Send startup notification
            if self.email_system:
                await asyncio.to_thread(
                    self.email_system.send_email,
                    "🚀 Factor Monitoring System Started",
                    f"<html><body><h2>System Startup</h2><p>Factor Monitoring System started successfully at {datetime.now()}</p></body></html>"
                )
            
            # Run initial data collection
            self.logger.info("Running initial data collection...")
            await self.run_daily_routine()
            
            self.logger.info("="*60)
            self.logger.info("SYSTEM STARTUP COMPLETE - ENTERING MONITORING MODE")
            self.logger.info("="*60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"System startup failed: {e}")
            return False
    
    def test_database_connection(self):
        """Test database connectivity"""
        try:
            import sqlite3
            conn = sqlite3.connect(self.config.db_path)
            conn.execute("SELECT 1")
            conn.close()
            return True
        except:
            return False
    
    def run_monitoring_loop(self):
        """Run continuous monitoring loop"""
        self.logger.info("Starting monitoring loop...")
        
        try:
            while self.system_active:
                # Run scheduled tasks
                schedule.run_pending()
                
                # Sleep for 1 minute
                time.sleep(60)
                
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested by user")
        except Exception as e:
            self.logger.error(f"Monitoring loop error: {e}")
        finally:
            self.shutdown_system()
    
    def shutdown_system(self):
        """Graceful system shutdown"""
        try:
            self.logger.info("Shutting down Factor Monitoring System...")
            
            self.system_active = False
            
            # Send shutdown notification
            if self.email_system:
                self.email_system.send_email(
                    "🛑 Factor Monitoring System Shutdown",
                    f"<html><body><h2>System Shutdown</h2><p>System shutdown at {datetime.now()}</p></body></html>"
                )
            
            self.logger.info("System shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Shutdown error: {e}")

# Export the fixed classes
__all__ = ['CompleteFactorMonitoringSystem', 'SystemConfig']
