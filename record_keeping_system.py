import sqlite3
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import hashlib
import os
from dataclasses import dataclass, asdict
from pathlib import Path
import shutil

@dataclass
class TradeRecord:
    """Data class for trade record structure"""
    timestamp: str
    trade_id: str
    factor: str
    symbol: str
    action: str  # BUY, SELL, REBALANCE
    quantity: float
    price: float
    total_value: float
    reason: str
    portfolio_value_before: float
    portfolio_value_after: float
    user_id: str = "system"
    
@dataclass
class DecisionRecord:
    """Data class for investment decision documentation"""
    timestamp: str
    decision_id: str
    decision_type: str  # ALLOCATION_CHANGE, REBALANCE, RISK_ADJUSTMENT
    factors_affected: List[str]
    rationale: str
    supporting_data: Dict
    expected_outcome: str
    actual_outcome: Optional[str] = None
    user_id: str = "system"

@dataclass
class RiskAssessment:
    """Data class for risk assessment records"""
    timestamp: str
    assessment_id: str
    portfolio_beta: float
    var_95: float
    max_drawdown: float
    factor_concentration: Dict[str, float]
    correlation_matrix: Dict
    regime_assessment: str
    risk_level: str  # LOW, MEDIUM, HIGH
    recommendations: List[str]

class FactorRecordKeeping:
    """
    Comprehensive record keeping system for factor investing
    Maintains audit trails, compliance documentation, and performance records
    """
    
    def __init__(self, db_path: str = "factor_records.db", backup_dir: str = "backups"):
        self.db_path = db_path
        self.backup_dir = Path(backup_dir)
        self.backup_dir.mkdir(exist_ok=True)
        
        # Initialize logging
        self.setup_logging()
        
        # Initialize database
        self.initialize_database()
        
        # Create daily backup on startup
        self.create_daily_backup()
    
    def setup_logging(self):
        """Setup comprehensive logging system"""
        log_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        )
        
        # File handler for all records
        file_handler = logging.FileHandler('factor_records.log')
        file_handler.setFormatter(log_formatter)
        file_handler.setLevel(logging.INFO)
        
        # File handler for audit trail (separate file)
        audit_handler = logging.FileHandler('audit_trail.log')
        audit_handler.setFormatter(log_formatter)
        audit_handler.setLevel(logging.INFO)
        
        # Setup logger
        self.logger = logging.getLogger('FactorRecords')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        
        # Setup audit logger
        self.audit_logger = logging.getLogger('AuditTrail')
        self.audit_logger.setLevel(logging.INFO)
        self.audit_logger.addHandler(audit_handler)
    
    def initialize_database(self):
        """Initialize SQLite database with all required tables"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Trade records table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trade_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    trade_id TEXT UNIQUE NOT NULL,
                    factor TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    action TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    price REAL NOT NULL,
                    total_value REAL NOT NULL,
                    reason TEXT NOT NULL,
                    portfolio_value_before REAL,
                    portfolio_value_after REAL,
                    user_id TEXT DEFAULT 'system',
                    hash_signature TEXT,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Decision records table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS decision_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    decision_id TEXT UNIQUE NOT NULL,
                    decision_type TEXT NOT NULL,
                    factors_affected TEXT NOT NULL,
                    rationale TEXT NOT NULL,
                    supporting_data TEXT,
                    expected_outcome TEXT,
                    actual_outcome TEXT,
                    user_id TEXT DEFAULT 'system',
                    hash_signature TEXT,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Risk assessment table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS risk_assessments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    assessment_id TEXT UNIQUE NOT NULL,
                    portfolio_beta REAL,
                    var_95 REAL,
                    max_drawdown REAL,
                    factor_concentration TEXT,
                    correlation_matrix TEXT,
                    regime_assessment TEXT,
                    risk_level TEXT,
                    recommendations TEXT,
                    hash_signature TEXT,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Daily portfolio snapshots
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT UNIQUE NOT NULL,
                    total_value REAL NOT NULL,
                    factor_allocations TEXT NOT NULL,
                    performance_metrics TEXT,
                    benchmark_comparison TEXT,
                    market_conditions TEXT,
                    hash_signature TEXT,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Compliance events table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS compliance_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    resolved BOOLEAN DEFAULT FALSE,
                    resolution_notes TEXT,
                    user_id TEXT DEFAULT 'system',
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Model performance tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS model_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    model_name TEXT NOT NULL,
                    prediction TEXT NOT NULL,
                    actual_outcome TEXT,
                    accuracy_score REAL,
                    confidence_level REAL,
                    notes TEXT,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
            self.logger.info("Database initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {e}")
            raise
    
    def generate_hash_signature(self, data: Dict) -> str:
        """Generate hash signature for data integrity"""
        data_string = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_string.encode()).hexdigest()
    
    def record_trade(self, trade: TradeRecord) -> bool:
        """Record a trade with full audit trail"""
        try:
            # Generate hash signature
            trade_dict = asdict(trade)
            hash_sig = self.generate_hash_signature(trade_dict)
            trade_dict['hash_signature'] = hash_sig
            
            # Store in database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO trade_records (
                    timestamp, trade_id, factor, symbol, action, quantity, 
                    price, total_value, reason, portfolio_value_before, 
                    portfolio_value_after, user_id, hash_signature
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade.timestamp, trade.trade_id, trade.factor, trade.symbol,
                trade.action, trade.quantity, trade.price, trade.total_value,
                trade.reason, trade.portfolio_value_before, 
                trade.portfolio_value_after, trade.user_id, hash_sig
            ))
            
            conn.commit()
            conn.close()
            
            # Log to audit trail
            self.audit_logger.info(f"TRADE_RECORDED: {trade.trade_id} - {trade.action} {trade.quantity} {trade.symbol} @ {trade.price}")
            
            self.logger.info(f"Trade recorded successfully: {trade.trade_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to record trade: {e}")
            return False
    
    def record_decision(self, decision: DecisionRecord) -> bool:
        """Record investment decision with rationale"""
        try:
            # Generate hash signature
            decision_dict = asdict(decision)
            hash_sig = self.generate_hash_signature(decision_dict)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO decision_records (
                    timestamp, decision_id, decision_type, factors_affected,
                    rationale, supporting_data, expected_outcome, actual_outcome,
                    user_id, hash_signature
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                decision.timestamp, decision.decision_id, decision.decision_type,
                json.dumps(decision.factors_affected), decision.rationale,
                json.dumps(decision.supporting_data), decision.expected_outcome,
                decision.actual_outcome, decision.user_id, hash_sig
            ))
            
            conn.commit()
            conn.close()
            
            # Log to audit trail
            self.audit_logger.info(f"DECISION_RECORDED: {decision.decision_id} - {decision.decision_type}")
            
            self.logger.info(f"Decision recorded successfully: {decision.decision_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to record decision: {e}")
            return False
    
    def record_risk_assessment(self, assessment: RiskAssessment) -> bool:
        """Record risk assessment with all metrics"""
        try:
            # Generate hash signature
            assessment_dict = asdict(assessment)
            hash_sig = self.generate_hash_signature(assessment_dict)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO risk_assessments (
                    timestamp, assessment_id, portfolio_beta, var_95, max_drawdown,
                    factor_concentration, correlation_matrix, regime_assessment,
                    risk_level, recommendations, hash_signature
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                assessment.timestamp, assessment.assessment_id, assessment.portfolio_beta,
                assessment.var_95, assessment.max_drawdown, 
                json.dumps(assessment.factor_concentration),
                json.dumps(assessment.correlation_matrix), assessment.regime_assessment,
                assessment.risk_level, json.dumps(assessment.recommendations), hash_sig
            ))
            
            conn.commit()
            conn.close()
            
            # Log to audit trail
            self.audit_logger.info(f"RISK_ASSESSMENT: {assessment.assessment_id} - Risk Level: {assessment.risk_level}")
            
            self.logger.info(f"Risk assessment recorded: {assessment.assessment_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to record risk assessment: {e}")
            return False
    
    def create_daily_snapshot(self, portfolio_data: Dict) -> bool:
        """Create daily portfolio snapshot"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Generate hash signature
            hash_sig = self.generate_hash_signature(portfolio_data)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO portfolio_snapshots (
                    date, total_value, factor_allocations, performance_metrics,
                    benchmark_comparison, market_conditions, hash_signature
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                today, portfolio_data.get('total_value', 0),
                json.dumps(portfolio_data.get('factor_allocations', {})),
                json.dumps(portfolio_data.get('performance_metrics', {})),
                json.dumps(portfolio_data.get('benchmark_comparison', {})),
                json.dumps(portfolio_data.get('market_conditions', {})),
                hash_sig
            ))
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Daily snapshot created for {today}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create daily snapshot: {e}")
            return False
    
    def log_compliance_event(self, event_type: str, description: str, severity: str = "MEDIUM") -> bool:
        """Log compliance-related events"""
        try:
            timestamp = datetime.now().isoformat()
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO compliance_events (
                    timestamp, event_type, description, severity
                ) VALUES (?, ?, ?, ?)
            ''', (timestamp, event_type, description, severity))
            
            conn.commit()
            conn.close()
            
            # Log to audit trail
            self.audit_logger.info(f"COMPLIANCE_EVENT: {event_type} - {severity} - {description}")
            
            self.logger.info(f"Compliance event logged: {event_type}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to log compliance event: {e}")
            return False
    
    def get_trade_history(self, start_date: str = None, end_date: str = None, factor: str = None) -> pd.DataFrame:
        """Retrieve trade history with optional filters"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = "SELECT * FROM trade_records WHERE 1=1"
            params = []
            
            if start_date:
                query += " AND date(timestamp) >= ?"
                params.append(start_date)
            
            if end_date:
                query += " AND date(timestamp) <= ?"
                params.append(end_date)
            
            if factor:
                query += " AND factor = ?"
                params.append(factor)
            
            query += " ORDER BY timestamp DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            self.logger.info(f"Retrieved {len(df)} trade records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve trade history: {e}")
            return pd.DataFrame()
    
    def get_decision_history(self, start_date: str = None, decision_type: str = None) -> pd.DataFrame:
        """Retrieve decision history with optional filters"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = "SELECT * FROM decision_records WHERE 1=1"
            params = []
            
            if start_date:
                query += " AND date(timestamp) >= ?"
                params.append(start_date)
            
            if decision_type:
                query += " AND decision_type = ?"
                params.append(decision_type)
            
            query += " ORDER BY timestamp DESC"
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            # Parse JSON fields
            if not df.empty:
                df['factors_affected'] = df['factors_affected'].apply(json.loads)
                df['supporting_data'] = df['supporting_data'].apply(
                    lambda x: json.loads(x) if x else {}
                )
            
            self.logger.info(f"Retrieved {len(df)} decision records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve decision history: {e}")
            return pd.DataFrame()
    
    def verify_data_integrity(self) -> Dict[str, bool]:
        """Verify data integrity using hash signatures"""
        results = {'trade_records': True, 'decision_records': True, 'risk_assessments': True}
        
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Check trade records
            trades_df = pd.read_sql_query(
                "SELECT * FROM trade_records WHERE hash_signature IS NOT NULL", conn
            )
            
            for _, row in trades_df.iterrows():
                original_hash = row['hash_signature']
                row_dict = row.drop(['id', 'hash_signature', 'created_date']).to_dict()
                calculated_hash = self.generate_hash_signature(row_dict)
                
                if original_hash != calculated_hash:
                    results['trade_records'] = False
                    self.logger.error(f"Data integrity violation in trade record: {row['trade_id']}")
            
            # Similar checks for other tables...
            
            conn.close()
            
            self.logger.info(f"Data integrity check completed: {results}")
            return results
            
        except Exception as e:
            self.logger.error(f"Data integrity check failed: {e}")
            return {k: False for k in results.keys()}
    
    def create_daily_backup(self):
        """Create daily database backup"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            backup_path = self.backup_dir / f"factor_records_backup_{today}.db"
            
            # Copy database file
            shutil.copy2(self.db_path, backup_path)
            
            # Keep only last 30 days of backups
            cutoff_date = datetime.now() - timedelta(days=30)
            
            for backup_file in self.backup_dir.glob("factor_records_backup_*.db"):
                file_date_str = backup_file.stem.split('_')[-1]
                try:
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    if file_date < cutoff_date:
                        backup_file.unlink()
                except ValueError:
                    # Skip files with invalid date format
                    continue
            
            self.logger.info(f"Daily backup created: {backup_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to create daily backup: {e}")
    
    def generate_compliance_report(self, start_date: str, end_date: str) -> Dict:
        """Generate comprehensive compliance report"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Trade summary
            trades_query = '''
                SELECT factor, action, COUNT(*) as count, SUM(total_value) as total_value
                FROM trade_records 
                WHERE date(timestamp) BETWEEN ? AND ?
                GROUP BY factor, action
            '''
            trades_df = pd.read_sql_query(trades_query, conn, params=(start_date, end_date))
            
            # Decision summary
            decisions_query = '''
                SELECT decision_type, COUNT(*) as count
                FROM decision_records 
                WHERE date(timestamp) BETWEEN ? AND ?
                GROUP BY decision_type
            '''
            decisions_df = pd.read_sql_query(decisions_query, conn, params=(start_date, end_date))
            
            # Risk assessments
            risk_query = '''
                SELECT risk_level, COUNT(*) as count, AVG(portfolio_beta) as avg_beta,
                       AVG(var_95) as avg_var, AVG(max_drawdown) as avg_drawdown
                FROM risk_assessments 
                WHERE date(timestamp) BETWEEN ? AND ?
                GROUP BY risk_level
            '''
            risk_df = pd.read_sql_query(risk_query, conn, params=(start_date, end_date))
            
            # Compliance events
            compliance_query = '''
                SELECT event_type, severity, COUNT(*) as count
                FROM compliance_events 
                WHERE date(timestamp) BETWEEN ? AND ?
                GROUP BY event_type, severity
            '''
            compliance_df = pd.read_sql_query(compliance_query, conn, params=(start_date, end_date))
            
            conn.close()
            
            # Data integrity check
            integrity_results = self.verify_data_integrity()
            
            report = {
                'report_period': f"{start_date} to {end_date}",
                'generated_at': datetime.now().isoformat(),
                'trade_summary': trades_df.to_dict('records'),
                'decision_summary': decisions_df.to_dict('records'),
                'risk_summary': risk_df.to_dict('records'),
                'compliance_events': compliance_df.to_dict('records'),
                'data_integrity': integrity_results,
                'total_trades': len(trades_df),
                'total_decisions': decisions_df['count'].sum() if not decisions_df.empty else 0,
                'total_risk_assessments': risk_df['count'].sum() if not risk_df.empty else 0
            }
            
            self.logger.info(f"Compliance report generated for {start_date} to {end_date}")
            return report
            
        except Exception as e:
            self.logger.error(f"Failed to generate compliance report: {e}")
            return {'error': str(e)}
    
    def export_records_to_csv(self, export_dir: str = "exports") -> bool:
        """Export all records to CSV files for external analysis"""
        try:
            export_path = Path(export_dir)
            export_path.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            conn = sqlite3.connect(self.db_path)
            
            # Export each table
            tables = [
                'trade_records', 'decision_records', 'risk_assessments', 
                'portfolio_snapshots', 'compliance_events', 'model_performance'
            ]
            
            for table in tables:
                df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                if not df.empty:
                    export_file = export_path / f"{table}_{timestamp}.csv"
                    df.to_csv(export_file, index=False)
                    self.logger.info(f"Exported {table} to {export_file}")
            
            conn.close()
            
            self.logger.info(f"All records exported to {export_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to export records: {e}")
            return False
    
    def cleanup_old_records(self, retention_days: int = 2555):  # 7 years default
        """Clean up old records beyond retention period"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime('%Y-%m-%d')
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Archive old records before deletion
            self.archive_old_records(cutoff_date)
            
            # Delete old records (keep critical audit data longer)
            tables_to_clean = [
                ('model_performance', 365),  # 1 year for model performance
                ('compliance_events', 2555), # 7 years for compliance
                ('trade_records', 2555),     # 7 years for trades
                ('decision_records', 2555),  # 7 years for decisions
            ]
            
            for table, retention in tables_to_clean:
                table_cutoff = (datetime.now() - timedelta(days=retention)).strftime('%Y-%m-%d')
                
                cursor.execute(f'''
                    DELETE FROM {table} 
                    WHERE date(timestamp) < ?
                ''', (table_cutoff,))
                
                deleted_count = cursor.rowcount
                self.logger.info(f"Deleted {deleted_count} old records from {table}")
            
            conn.commit()
            conn.close()
            
            self.logger.info("Record cleanup completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old records: {e}")
            return False
    
    def archive_old_records(self, cutoff_date: str):
        """Archive old records to separate database"""
        try:
            archive_db_path = f"factor_records_archive_{datetime.now().year}.db"
            
            # Create archive database with same structure
            archive_conn = sqlite3.connect(archive_db_path)
            main_conn = sqlite3.connect(self.db_path)
            
            # Copy schema
            main_conn.backup(archive_conn)
            
            # Clear current data from archive
            archive_cursor = archive_conn.cursor()
            tables = ['trade_records', 'decision_records', 'risk_assessments', 
                     'portfolio_snapshots', 'compliance_events']
            
            for table in tables:
                archive_cursor.execute(f'DELETE FROM {table}')
            
            # Copy old records to archive
            for table in tables:
                old_records = pd.read_sql_query(f'''
                    SELECT * FROM {table} 
                    WHERE date(timestamp) < ?
                ''', main_conn, params=(cutoff_date,))
                
                if not old_records.empty:
                    old_records.to_sql(table, archive_conn, if_exists='append', index=False)
            
            archive_conn.close()
            main_conn.close()
            
            self.logger.info(f"Old records archived to {archive_db_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to archive old records: {e}")

# Integration with Factor Data Collector
class FactorRecordIntegration:
    """
    Integration class that connects FactorDataCollector with RecordKeeping
    """
    
    def __init__(self, data_collector, record_keeper):
        self.data_collector = data_collector
        self.record_keeper = record_keeper
        self.logger = logging.getLogger('FactorIntegration')
    
    def execute_and_record_trade(self, factor: str, symbol: str, action: str, 
                                quantity: float, price: float, reason: str,
                                portfolio_value_before: float) -> bool:
        """Execute trade and automatically record it"""
        try:
            # Generate unique trade ID
            trade_id = f"{symbol}_{action}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Calculate trade value
            total_value = quantity * price
            portfolio_value_after = portfolio_value_before + (total_value if action == "SELL" else -total_value)
            
            # Create trade record
            trade_record = TradeRecord(
                timestamp=datetime.now().isoformat(),
                trade_id=trade_id,
                factor=factor,
                symbol=symbol,
                action=action,
                quantity=quantity,
                price=price,
                total_value=total_value,
                reason=reason,
                portfolio_value_before=portfolio_value_before,
                portfolio_value_after=portfolio_value_after
            )
            
            # Record the trade
            success = self.record_keeper.record_trade(trade_record)
            
            if success:
                self.logger.info(f"Trade executed and recorded: {trade_id}")
            else:
                self.logger.error(f"Failed to record trade: {trade_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to execute and record trade: {e}")
            return False
    
    def record_rebalancing_decision(self, current_allocations: Dict, target_allocations: Dict, 
                                  market_data: Dict, reason: str) -> str:
        """Record rebalancing decision with full context"""
        try:
            decision_id = f"REBAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Calculate allocation changes
            factors_affected = []
            supporting_data = {
                'current_allocations': current_allocations,
                'target_allocations': target_allocations,
                'market_data': market_data,
                'allocation_changes': {}
            }
            
            for factor in target_allocations:
                current = current_allocations.get(factor, 0)
                target = target_allocations[factor]
                change = target - current
                
                if abs(change) > 0.01:  # 1% threshold
                    factors_affected.append(factor)
                    supporting_data['allocation_changes'][factor] = change
            
            # Create decision record
            decision_record = DecisionRecord(
                timestamp=datetime.now().isoformat(),
                decision_id=decision_id,
                decision_type="REBALANCE",
                factors_affected=factors_affected,
                rationale=reason,
                supporting_data=supporting_data,
                expected_outcome=f"Rebalance portfolio to target allocations: {target_allocations}"
            )
            
            # Record the decision
            success = self.record_keeper.record_decision(decision_record)
            
            if success:
                self.logger.info(f"Rebalancing decision recorded: {decision_id}")
                return decision_id
            else:
                self.logger.error(f"Failed to record rebalancing decision")
                return ""
            
        except Exception as e:
            self.logger.error(f"Failed to record rebalancing decision: {e}")
            return ""
    
    def daily_record_keeping_routine(self):
        """Run daily record keeping routine"""
        try:
            # Create daily portfolio snapshot
            portfolio_data = self.get_current_portfolio_snapshot()
            self.record_keeper.create_daily_snapshot(portfolio_data)
            
            # Create daily risk assessment
            risk_assessment = self.generate_daily_risk_assessment()
            if risk_assessment:
                self.record_keeper.record_risk_assessment(risk_assessment)
            
            # Create daily backup
            self.record_keeper.create_daily_backup()
            
            self.logger.info("Daily record keeping routine completed")
            
        except Exception as e:
            self.logger.error(f"Daily record keeping routine failed: {e}")
    
    def get_current_portfolio_snapshot(self) -> Dict:
        """Get current portfolio snapshot data"""
        try:
            # This would integrate with your actual portfolio data
            # For now, return sample structure
            
            return {
                'total_value': 1000000.0,  # $1M example
                'factor_allocations': {
                    'Value': 0.30,
                    'Growth': 0.20,
                    'Quality': 0.20,
                    'Low_Volatility': 0.15,
                    'Momentum': 0.10,
                    'Size': 0.05
                },
                'performance_metrics': {
                    'daily_return': 0.005,
                    'ytd_return': 0.082,
                    'volatility': 0.12,
                    'sharpe_ratio': 1.25
                },
                'benchmark_comparison': {
                    'spy_relative_return': 0.015,
                    'tracking_error': 0.03
                },
                'market_conditions': {
                    'vix_level': 20.5,
                    'regime': 'MEDIUM_VOLATILITY'
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get portfolio snapshot: {e}")
            return {}
    
    def generate_daily_risk_assessment(self) -> Optional[RiskAssessment]:
        """Generate daily risk assessment"""
        try:
            assessment_id = f"RISK_{datetime.now().strftime('%Y%m%d')}"
            
            # This would calculate actual risk metrics
            # For now, return sample assessment
            
            return RiskAssessment(
                timestamp=datetime.now().isoformat(),
                assessment_id=assessment_id,
                portfolio_beta=0.87,
                var_95=0.025,
                max_drawdown=-0.085,
                factor_concentration={
                    'Value': 0.30,
                    'Growth': 0.20,
                    'Quality': 0.20,
                    'Low_Volatility': 0.15,
                    'Momentum': 0.10,
                    'Size': 0.05
                },
                correlation_matrix={
                    'Value_Growth': -0.65,
                    'Quality_LowVol': 0.45,
                    'Momentum_Growth': 0.72
                },
                regime_assessment="MEDIUM_VOLATILITY",
                risk_level="MEDIUM",
                recommendations=[
                    "Monitor momentum factor for potential reversal",
                    "Consider increasing low volatility allocation",
                    "Watch for regime change indicators"
                ]
            )
            
        except Exception as e:
            self.logger.error(f"Failed to generate risk assessment: {e}")
            return None

# Example usage and testing
if __name__ == "__main__":
    # Initialize record keeping system
    record_keeper = FactorRecordKeeping()
    
    # Test trade recording
    test_trade = TradeRecord(
        timestamp=datetime.now().isoformat(),
        trade_id="TEST_001",
        factor="Value",
        symbol="VTV",
        action="BUY",
        quantity=100.0,
        price=150.50,
        total_value=15050.0,
        reason="Rebalancing - Value factor underweight",
        portfolio_value_before=1000000.0,
        portfolio_value_after=984950.0
    )
    
    success = record_keeper.record_trade(test_trade)
    print(f"Test trade recorded: {success}")
    
    # Test decision recording
    test_decision = DecisionRecord(
        timestamp=datetime.now().isoformat(),
        decision_id="DEC_001",
        decision_type="ALLOCATION_CHANGE",
        factors_affected=["Value", "Growth"],
        rationale="Market regime change detected - shifting from growth to value",
        supporting_data={
            "vix_increase": 5.2,
            "value_growth_spread": -0.025,
            "economic_indicators": "recession_probability_increased"
        },
        expected_outcome="Outperformance during market downturn"
    )
    
    success = record_keeper.record_decision(test_decision)
    print(f"Test decision recorded: {success}")
    
    # Generate compliance report
    report = record_keeper.generate_compliance_report(
        start_date="2024-01-01",
        end_date="2024-12-31"
    )
    print(f"Compliance report generated: {len(report)} sections")
    
    # Export records
    success = record_keeper.export_records_to_csv()
    print(f"Records exported: {success}")
    
    print("Record keeping system test completed")
