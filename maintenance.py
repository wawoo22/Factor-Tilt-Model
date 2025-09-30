#!/usr/bin/env python3
"""
Factor Monitoring System - Maintenance Scripts
"""
import sqlite3
import os
import shutil
from datetime import datetime, timedelta
import pandas as pd
import argparse

def backup_database(db_path="factor_monitoring_production.db"):
    """Create database backup"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = "backups"
        
        os.makedirs(backup_dir, exist_ok=True)
        
        backup_path = os.path.join(backup_dir, f"factor_monitoring_backup_{timestamp}.db")
        shutil.copy2(db_path, backup_path)
        
        print(f"✅ Database backed up to: {backup_path}")
        
        # Cleanup old backups (keep last 30)
        backup_files = sorted([f for f in os.listdir(backup_dir) if f.startswith("factor_monitoring_backup_")])
        
        if len(backup_files) > 30:
            for old_backup in backup_files[:-30]:
                os.remove(os.path.join(backup_dir, old_backup))
                print(f"🗑️ Removed old backup: {old_backup}")
        
        return True
        
    except Exception as e:
        print(f"❌ Backup failed: {e}")
        return False

def cleanup_old_data(db_path="factor_monitoring_production.db", days=365):
    """Clean up old data beyond retention period"""
    try:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Tables to clean with their date columns
        cleanup_tables = [
            ('alerts_log', 'timestamp'),
            ('factor_returns', 'date'),
            ('factor_prices', 'date'),
            ('attribution_results', 'created_date')
        ]
        
        total_deleted = 0
        
        for table, date_column in cleanup_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE date({date_column}) < ?", (cutoff_date,))
            count_before = cursor.fetchone()[0]
            
            cursor.execute(f"DELETE FROM {table} WHERE date({date_column}) < ?", (cutoff_date,))
            deleted = cursor.rowcount
            total_deleted += deleted
            
            print(f"🧹 {table}: Deleted {deleted} old records")
        
        conn.commit()
        conn.close()
        
        print(f"✅ Cleanup complete: {total_deleted} total records deleted")
        return True
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False

def optimize_database(db_path="factor_monitoring_production.db"):
    """Optimize database performance"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Vacuum database
        cursor.execute("VACUUM")
        
        # Recompute statistics
        cursor.execute("ANALYZE")
        
        conn.close()
        
        print("✅ Database optimized")
        return True
        
    except Exception as e:
        print(f"❌ Database optimization failed: {e}")
        return False

def generate_system_report(db_path="factor_monitoring_production.db"):
    """Generate system health report"""
    try:
        conn = sqlite3.connect(db_path)
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'database_size': os.path.getsize(db_path) / 1024 / 1024,  # MB
        }
        
        # Count records in each table
        tables = ['factor_prices', 'factor_returns', 'alerts_log', 'trade_records', 'attribution_results']
        
        for table in tables:
            try:
                df = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", conn)
                report[f'{table}_count'] = df.iloc[0]['count']
            except:
                report[f'{table}_count'] = 0
        
        # Recent activity
        recent_alerts = pd.read_sql_query("""
            SELECT COUNT(*) as count FROM alerts_log 
            WHERE date(timestamp) >= date('now', '-7 days')
        """, conn)
        report['alerts_last_7_days'] = recent_alerts.iloc[0]['count']
        
        # System errors
        error_alerts = pd.read_sql_query("""
            SELECT COUNT(*) as count FROM alerts_log 
            WHERE severity = 'HIGH' AND date(timestamp) >= date('now', '-7 days')
        """, conn)
        report['high_priority_alerts_7_days'] = error_alerts.iloc[0]['count']
        
        conn.close()
        
        # Print report
        print("📊 SYSTEM HEALTH REPORT")
        print("=" * 40)
        print(f"Timestamp: {report['timestamp']}")
        print(f"Database Size: {report['database_size']:.2f} MB")
        print(f"Recent Alerts (7 days): {report['alerts_last_7_days']}")
        print(f"High Priority Alerts (7 days): {report['high_priority_alerts_7_days']}")
        print("\nRecord Counts:")
        
        for table in tables:
            count_key = f'{table}_count'
            if count_key in report:
                print(f"  {table}: {report[count_key]:,}")
        
        return report
        
    except Exception as e:
        print(f"❌ Report generation failed: {e}")
        return None

def check_system_health():
    """Perform comprehensive system health check"""
    print("🏥 SYSTEM HEALTH CHECK")
    print("=" * 40)
    
    health_status = {'overall': True, 'issues': []}
    
    # Check database
    db_path = "factor_monitoring_production.db"
    if os.path.exists(db_path):
        print("✅ Database: Present")
        
        # Check database size
        db_size_mb = os.path.getsize(db_path) / 1024 / 1024
        if db_size_mb > 1000:  # 1GB
            print(f"⚠️  Database size: {db_size_mb:.2f} MB (Consider cleanup)")
            health_status['issues'].append(f"Large database: {db_size_mb:.2f} MB")
        else:
            print(f"✅ Database size: {db_size_mb:.2f} MB")
    else:
        print("❌ Database: Missing")
        health_status['overall'] = False
        health_status['issues'].append("Database missing")
    
    # Check log files
    log_files = ['factor_system.log', 'audit_trail.log', 'factor_records.log']
    for log_file in log_files:
        if os.path.exists(log_file):
            log_size = os.path.getsize(log_file) / 1024 / 1024  # MB
            if log_size > 100:  # 100MB
                print(f"⚠️  {log_file}: {log_size:.2f} MB (Consider rotation)")
                health_status['issues'].append(f"Large log file: {log_file}")
            else:
                print(f"✅ {log_file}: {log_size:.2f} MB")
        else:
            print(f"⚠️  {log_file}: Not found")
    
    # Check disk space
    try:
        import shutil
        total, used, free = shutil.disk_usage('.')
        free_gb = free / (1024**3)
        
        if free_gb < 1:  # Less than 1GB free
            print(f"❌ Disk space: {free_gb:.2f} GB free (Critical)")
            health_status['overall'] = False
            health_status['issues'].append(f"Low disk space: {free_gb:.2f} GB")
        elif free_gb < 5:  # Less than 5GB free
            print(f"⚠️  Disk space: {free_gb:.2f} GB free (Warning)")
            health_status['issues'].append(f"Low disk space: {free_gb:.2f} GB")
        else:
            print(f"✅ Disk space: {free_gb:.2f} GB free")
            
    except Exception as e:
        print(f"❌ Disk space check failed: {e}")
        health_status['issues'].append("Disk space check failed")
    
    # Overall status
    print("\n" + "=" * 40)
    if health_status['overall'] and not health_status['issues']:
        print("🎉 SYSTEM HEALTH: EXCELLENT")
    elif health_status['overall']:
        print("⚠️  SYSTEM HEALTH: GOOD (Minor issues)")
        for issue in health_status['issues']:
            print(f"   - {issue}")
    else:
        print("❌ SYSTEM HEALTH: CRITICAL")
        for issue in health_status['issues']:
            print(f"   - {issue}")
    
    return health_status

def main():
    parser = argparse.ArgumentParser(description='Factor Monitoring System Maintenance')
    parser.add_argument('command', choices=['backup', 'cleanup', 'optimize', 'report', 'health', 'all'])
    parser.add_argument('--db', default='factor_monitoring_production.db', help='Database path')
    parser.add_argument('--days', type=int, default=365, help='Days to retain for cleanup')
    
    args = parser.parse_args()
    
    if args.command == 'backup':
        backup_database(args.db)
    elif args.command == 'cleanup':
        cleanup_old_data(args.db, args.days)
    elif args.command == 'optimize':
        optimize_database(args.db)
    elif args.command == 'report':
        generate_system_report(args.db)
    elif args.command == 'health':
        check_system_health()
    elif args.command == 'all':
        print("🔧 RUNNING FULL MAINTENANCE")
        print("=" * 50)
        backup_database(args.db)
        cleanup_old_data(args.db, args.days)
        optimize_database(args.db)
        generate_system_report(args.db)
        check_system_health()

if __name__ == "__main__":
    main()
