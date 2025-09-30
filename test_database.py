import sqlite3
from datetime import datetime

def test_database():
    try:
        conn = sqlite3.connect("factor_monitoring.db")
        cursor = conn.cursor()
        
        # Test insert
        cursor.execute("""
            INSERT INTO alerts_log (timestamp, alert_type, factor, message, severity)
            VALUES (?, ?, ?, ?, ?)
        """, (datetime.now().isoformat(), "TEST", "SYSTEM", "Database test", "LOW"))
        
        # Test select
        cursor.execute("SELECT COUNT(*) FROM alerts_log")
        count = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        print(f"✅ Database test successful! Records: {count}")
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

if __name__ == "__main__":
    test_database()
