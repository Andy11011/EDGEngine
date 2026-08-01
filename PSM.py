import psycopg2
import psycopg2.extras
import json
from datetime import datetime

class PSM:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PSM, cls).__new__(cls)
            cls._instance._init_db()
        return cls._instance

    def _init_db(self):
        """Initialize connection to Postgres and ensure schema exists."""
        print("Connecting to Postgres...")
        self.conn = psycopg2.connect(
            dbname="postgres",
            user="user",
            password="pass",
            host="productiondb",
            port=5432
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Create scanner_results_l1 table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS scanner_results_l1 (
                id SERIAL PRIMARY KEY,
                result JSONB NOT NULL,
                timestamp TIMESTAMP NOT NULL
            )
        ''')

        # Create filter_stats table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS filter_stats (
                filter_id VARCHAR(50) PRIMARY KEY,
                passed INTEGER NOT NULL DEFAULT 0,
                rejected INTEGER NOT NULL DEFAULT 0,
                last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Drop old filter_configs table if it exists (migration)
        self.cursor.execute("DROP TABLE IF EXISTS filter_configs")
        print("✅ Dropped obsolete filter_configs table (if existed)")

        self.conn.commit()

    # ----------------------------------------------------------------------
    # Scanner results methods (unchanged)
    # ----------------------------------------------------------------------
    def insert_record(self, result_data, timestamp):
        """Insert a record into scanner_results_l1, keeping max 100 rows."""
        result_json = json.dumps(result_data)
        timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        self.cursor.execute("SELECT COUNT(*) FROM scanner_results_l1")
        count = self.cursor.fetchone()["count"]
        if count >= 100:
            self.cursor.execute("""
                DELETE FROM scanner_results_l1
                WHERE id IN (
                    SELECT id FROM scanner_results_l1
                    ORDER BY timestamp ASC
                    LIMIT %s
                )
            """, (count - 99,))
        self.cursor.execute(
            "INSERT INTO scanner_results_l1 (result, timestamp) VALUES (%s, %s)",
            (result_json, timestamp_obj)
        )
        self.conn.commit()

    def get_records(self, max_entries=None):
        """Fetch records from scanner_results_l1."""
        if max_entries is not None:
            self.cursor.execute(
                "SELECT id, result, timestamp FROM scanner_results_l1 ORDER BY id DESC LIMIT %s",
                (max_entries,)
            )
        else:
            self.cursor.execute(
                "SELECT id, result, timestamp FROM scanner_results_l1 ORDER BY id DESC"
            )
        rows = self.cursor.fetchall()
        records = []
        for row in rows:
            records.append({
                'id': row["id"],
                'result': row["result"],
                'timestamp': row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            })
        return records

    def clear_records(self):
        self.cursor.execute("DELETE FROM scanner_results_l1")
        self.conn.commit()
        print("[INFO] All records cleared from scanner_results_l1")

    # ----------------------------------------------------------------------
    # Filter statistics methods (only stats remain)
    # ----------------------------------------------------------------------
    def upsert_filter_stats(self, filter_id: str, passed: int, rejected: int):
        self.cursor.execute('''
            INSERT INTO filter_stats (filter_id, passed, rejected, last_updated)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (filter_id)
            DO UPDATE SET
                passed = EXCLUDED.passed,
                rejected = EXCLUDED.rejected,
                last_updated = EXCLUDED.last_updated
        ''', (filter_id, passed, rejected, datetime.now()))
        self.conn.commit()

    def get_filter_stats(self, filter_id: str):
        self.cursor.execute(
            "SELECT filter_id, passed, rejected, last_updated FROM filter_stats WHERE filter_id = %s",
            (filter_id,)
        )
        row = self.cursor.fetchone()
        if not row:
            return None
        return {
            'filter_id': row['filter_id'],
            'passed': row['passed'],
            'rejected': row['rejected'],
            'last_updated': row['last_updated'].strftime("%Y-%m-%d %H:%M:%S")
        }

    def get_all_filter_stats(self):
        self.cursor.execute(
            "SELECT filter_id, passed, rejected, last_updated FROM filter_stats"
        )
        rows = self.cursor.fetchall()
        stats = []
        for row in rows:
            stats.append({
                'filter_id': row['filter_id'],
                'passed': row['passed'],
                'rejected': row['rejected'],
                'last_updated': row['last_updated'].strftime("%Y-%m-%d %H:%M:%S")
            })
        return stats

    def clear_filter_stats(self):
        self.cursor.execute("DELETE FROM filter_stats")
        self.conn.commit()
        print("[INFO] All filter stats cleared")

    def close(self):
        self.conn.close()