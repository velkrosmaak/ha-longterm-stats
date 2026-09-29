import sqlite3
import os
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_dir()
        self.init_db()

    def _ensure_dir(self):
        parent = Path(self.db_path).parent
        if parent:
            parent.mkdir(parents=True, exist_ok=True)

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode & PRAGMA optimizations for VM memory/disk performance
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA cache_size=-64000;")  # 64 MB memory cache
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA mmap_size=268435456;") # 256 MB memory-mapped I/O
        return conn

    def init_db(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Entities table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS entities (
                entity_id TEXT PRIMARY KEY,
                friendly_name TEXT,
                unit_of_measurement TEXT,
                device_class TEXT,
                state_class TEXT,
                enabled INTEGER DEFAULT 1,
                first_seen TEXT,
                last_seen TEXT
            );
            """)

            # 2. Raw sensor readings table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensor_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entity_id TEXT NOT NULL,
                timestamp TEXT NOT NULL, -- ISO8601 YYYY-MM-DDTHH:MM:SS
                value REAL,
                raw_value TEXT,
                FOREIGN KEY (entity_id) REFERENCES entities(entity_id)
            );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sensor_data_entity_ts ON sensor_data(entity_id, timestamp);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_sensor_data_ts ON sensor_data(timestamp);")

            # 3. Hourly aggregated stats
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_stats (
                entity_id TEXT NOT NULL,
                bucket_time TEXT NOT NULL, -- YYYY-MM-DD HH:00:00
                min_val REAL,
                max_val REAL,
                avg_val REAL,
                sum_val REAL,
                count_val INTEGER,
                PRIMARY KEY (entity_id, bucket_time)
            );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_hourly_entity_time ON hourly_stats(entity_id, bucket_time);")

            # 4. Daily aggregated stats
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                entity_id TEXT NOT NULL,
                bucket_date TEXT NOT NULL, -- YYYY-MM-DD
                min_val REAL,
                max_val REAL,
                avg_val REAL,
                sum_val REAL,
                count_val INTEGER,
                PRIMARY KEY (entity_id, bucket_date)
            );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_entity_date ON daily_stats(entity_id, bucket_date);")

            # 5. Rollup Watermark Tracker
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS rollup_watermarks (
                watermark_key TEXT PRIMARY KEY,
                last_processed_time TEXT NOT NULL
            );
            """)

            conn.commit()

    def upsert_entity(
        self,
        entity_id: str,
        friendly_name: Optional[str] = None,
        unit_of_measurement: Optional[str] = None,
        device_class: Optional[str] = None,
        state_class: Optional[str] = None,
    ):
        now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
            INSERT INTO entities (entity_id, friendly_name, unit_of_measurement, device_class, state_class, enabled, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(entity_id) DO UPDATE SET
                friendly_name = COALESCE(excluded.friendly_name, entities.friendly_name),
                unit_of_measurement = COALESCE(excluded.unit_of_measurement, entities.unit_of_measurement),
                device_class = COALESCE(excluded.device_class, entities.device_class),
                state_class = COALESCE(excluded.state_class, entities.state_class),
                last_seen = excluded.last_seen;
            """, (entity_id, friendly_name, unit_of_measurement, device_class, state_class, now_str, now_str))
            conn.commit()

    def upsert_entities_batch(self, entities: List[Tuple]):
        """Bulk upsert entity metadata in a single transaction.
        Each tuple: (entity_id, friendly_name, unit_of_measurement, device_class, state_class, now_str)
        """
        if not entities:
            return
        with self.get_connection() as conn:
            conn.executemany("""
            INSERT INTO entities (entity_id, friendly_name, unit_of_measurement, device_class, state_class, enabled, first_seen, last_seen)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(entity_id) DO UPDATE SET
                friendly_name = COALESCE(excluded.friendly_name, entities.friendly_name),
                unit_of_measurement = COALESCE(excluded.unit_of_measurement, entities.unit_of_measurement),
                device_class = COALESCE(excluded.device_class, entities.device_class),
                state_class = COALESCE(excluded.state_class, entities.state_class),
                last_seen = excluded.last_seen;
            """, entities)
            conn.commit()

    def set_entity_enabled(self, entity_id: str, enabled: bool):
        with self.get_connection() as conn:
            conn.execute("UPDATE entities SET enabled = ? WHERE entity_id = ?", (1 if enabled else 0, entity_id))
            conn.commit()

    def insert_reading(self, entity_id: str, timestamp: str, value: Optional[float], raw_value: str):
        with self.get_connection() as conn:
            conn.execute("""
            INSERT INTO sensor_data (entity_id, timestamp, value, raw_value)
            VALUES (?, ?, ?, ?);
            """, (entity_id, timestamp, value, raw_value))
            conn.commit()

    def insert_readings_batch(self, readings: List[Tuple[str, str, Optional[float], str]]):
        if not readings:
            return
        with self.get_connection() as conn:
            conn.executemany("""
            INSERT INTO sensor_data (entity_id, timestamp, value, raw_value)
            VALUES (?, ?, ?, ?);
            """, readings)
            conn.commit()

    def calculate_rollups(self, force_full: bool = False):
        """Computes incremental hourly and daily rollup statistics from raw data."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Fetch last hourly watermark
            last_hourly = None
            if not force_full:
                row_h = cursor.execute("SELECT last_processed_time FROM rollup_watermarks WHERE watermark_key = 'hourly'").fetchone()
                if row_h:
                    last_hourly = row_h["last_processed_time"]

            # 1. Hourly Rollup (Incremental)
            query_hourly = """
            INSERT INTO hourly_stats (entity_id, bucket_time, min_val, max_val, avg_val, sum_val, count_val)
            SELECT 
                entity_id,
                SUBSTR(timestamp, 1, 13) || ':00:00' AS bucket_time,
                MIN(value) as min_val,
                MAX(value) as max_val,
                AVG(value) as avg_val,
                SUM(value) as sum_val,
                COUNT(value) as count_val
            FROM sensor_data
            WHERE value IS NOT NULL
            """
            params_h = []
            if last_hourly:
                query_hourly += " AND timestamp >= ?"
                params_h.append(last_hourly)

            query_hourly += """
            GROUP BY entity_id, bucket_time
            ON CONFLICT(entity_id, bucket_time) DO UPDATE SET
                min_val = excluded.min_val,
                max_val = excluded.max_val,
                avg_val = excluded.avg_val,
                sum_val = excluded.sum_val,
                count_val = excluded.count_val;
            """
            cursor.execute(query_hourly, params_h)

            # Update hourly watermark
            max_h = cursor.execute("SELECT MAX(bucket_time) as max_b FROM hourly_stats").fetchone()
            if max_h and max_h["max_b"]:
                cursor.execute("""
                INSERT INTO rollup_watermarks (watermark_key, last_processed_time)
                VALUES ('hourly', ?)
                ON CONFLICT(watermark_key) DO UPDATE SET last_processed_time = excluded.last_processed_time;
                """, (max_h["max_b"],))

            # Fetch last daily watermark
            last_daily = None
            if not force_full:
                row_d = cursor.execute("SELECT last_processed_time FROM rollup_watermarks WHERE watermark_key = 'daily'").fetchone()
                if row_d:
                    last_daily = row_d["last_processed_time"]

            # 2. Daily Rollup (Incremental)
            query_daily = """
            INSERT INTO daily_stats (entity_id, bucket_date, min_val, max_val, avg_val, sum_val, count_val)
            SELECT 
                entity_id,
                SUBSTR(bucket_time, 1, 10) AS bucket_date,
                MIN(min_val) as min_val,
                MAX(max_val) as max_val,
                SUM(avg_val * count_val) / SUM(count_val) as avg_val,
                SUM(sum_val) as sum_val,
                SUM(count_val) as count_val
            FROM hourly_stats
            """
            params_d = []
            if last_daily:
                query_daily += " WHERE bucket_time >= ?"
                params_d.append(last_daily)

            query_daily += """
            GROUP BY entity_id, bucket_date
            ON CONFLICT(entity_id, bucket_date) DO UPDATE SET
                min_val = excluded.min_val,
                max_val = excluded.max_val,
                avg_val = excluded.avg_val,
                sum_val = excluded.sum_val,
                count_val = excluded.count_val;
            """
            cursor.execute(query_daily, params_d)

            # Update daily watermark
            max_d = cursor.execute("SELECT MAX(bucket_date) as max_b FROM daily_stats").fetchone()
            if max_d and max_d["max_b"]:
                cursor.execute("""
                INSERT INTO rollup_watermarks (watermark_key, last_processed_time)
                VALUES ('daily', ?)
                ON CONFLICT(watermark_key) DO UPDATE SET last_processed_time = excluded.last_processed_time;
                """, (max_d["max_b"],))

            conn.commit()

    def prune_raw_data(self, days_to_keep: int):
        if days_to_keep <= 0:
            return
        cutoff_date = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_to_keep)).isoformat()
        with self.get_connection() as conn:
            conn.execute("DELETE FROM sensor_data WHERE timestamp < ?", (cutoff_date,))
            conn.commit()

    def get_entities(
        self,
        enabled_only: bool = False,
        device_class: Optional[str] = None,
        sort_by: Optional[str] = "name",
        is_static_filter: Optional[bool] = None,
        dormant_days: int = 14,
        include_dormant: bool = True,
        is_dormant_filter: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        now = datetime.datetime.now(datetime.timezone.utc)
        dormant_cutoff = (now - datetime.timedelta(days=dormant_days)).isoformat()

        with self.get_connection() as conn:
            query = """
            SELECT 
                e.*,
                COALESCE(
                    (
                        SELECT (MIN(val) = MAX(val) AND COUNT(val) > 1)
                        FROM (
                            SELECT value as val FROM sensor_data WHERE entity_id = e.entity_id AND value IS NOT NULL
                            UNION ALL
                            SELECT min_val as val FROM daily_stats WHERE entity_id = e.entity_id
                            UNION ALL
                            SELECT max_val as val FROM daily_stats WHERE entity_id = e.entity_id
                        )
                    ),
                    0
                ) as is_static
            FROM entities e
            """
            params = []
            conditions = []
            if enabled_only:
                conditions.append("e.enabled = 1")
            if device_class:
                conditions.append("e.device_class = ?")
                params.append(device_class)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            if sort_by == "last_updated_desc":
                query += " ORDER BY e.last_seen DESC, e.friendly_name ASC"
            elif sort_by == "last_updated_asc":
                query += " ORDER BY e.last_seen ASC, e.friendly_name ASC"
            else:
                query += " ORDER BY e.friendly_name ASC, e.entity_id ASC"
            
            rows = conn.execute(query, params).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["is_static"] = bool(d["is_static"])
                
                # Check dormancy (last_seen < 14 days ago)
                last_seen = d.get("last_seen")
                d["is_dormant"] = bool(last_seen and last_seen < dormant_cutoff)

                if is_static_filter is not None and d["is_static"] != is_static_filter:
                    continue
                if is_dormant_filter is not None and d["is_dormant"] != is_dormant_filter:
                    continue
                if not include_dormant and d["is_dormant"]:
                    continue

                result.append(d)
            return result




    def get_time_series(
        self,
        entity_id: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        interval: str = "day",  # raw, hour, day, week, month, year
        metric: str = "avg"      # avg, min, max, sum
    ) -> List[Dict[str, Any]]:
        """Returns time series data for an entity aggregated over interval."""
        metric = metric.lower()
        if metric not in ("avg", "min", "max", "sum"):
            metric = "avg"

        with self.get_connection() as conn:
            if interval == "raw":
                query = """
                SELECT timestamp as time, value, raw_value
                FROM sensor_data
                WHERE entity_id = ? AND value IS NOT NULL
                """
                params = [entity_id]
                if start_time:
                    query += " AND timestamp >= ?"
                    params.append(start_time)
                if end_time:
                    query += " AND timestamp <= ?"
                    params.append(end_time)
                query += " ORDER BY timestamp ASC"
                rows = conn.execute(query, params).fetchall()
                return [{"time": r["time"], "value": r["value"]} for r in rows]

            elif interval == "hour":
                query = f"""
                SELECT bucket_time as time, {metric}_val as value, min_val, max_val, avg_val
                FROM hourly_stats
                WHERE entity_id = ?
                """
                params = [entity_id]
                if start_time:
                    query += " AND bucket_time >= ?"
                    params.append(start_time)
                if end_time:
                    query += " AND bucket_time <= ?"
                    params.append(end_time)
                query += " ORDER BY bucket_time ASC"
                rows = conn.execute(query, params).fetchall()
                return [dict(r) for r in rows]

            elif interval == "day":
                query = f"""
                SELECT bucket_date as time, {metric}_val as value, min_val, max_val, avg_val
                FROM daily_stats
                WHERE entity_id = ?
                """
                params = [entity_id]
                if start_time:
                    query += " AND bucket_date >= ?"
                    params.append(start_time)
                if end_time:
                    query += " AND bucket_date <= ?"
                    params.append(end_time)
                query += " ORDER BY bucket_date ASC"
                rows = conn.execute(query, params).fetchall()
                return [dict(r) for r in rows]

            elif interval == "week":
                # Aggregate daily_stats by ISO week (YYYY-Www or start of week)
                query = f"""
                SELECT 
                    STRFTIME('%Y-W%W', bucket_date) as time,
                    MIN(bucket_date) as week_start,
                    MIN(min_val) as min_val,
                    MAX(max_val) as max_val,
                    AVG(avg_val) as avg_val,
                    SUM(sum_val) as sum_val
                FROM daily_stats
                WHERE entity_id = ?
                """
                params = [entity_id]
                if start_time:
                    query += " AND bucket_date >= ?"
                    params.append(start_time)
                if end_time:
                    query += " AND bucket_date <= ?"
                    params.append(end_time)
                query += " GROUP BY time ORDER BY time ASC"
                rows = conn.execute(query, params).fetchall()
                result = []
                for r in rows:
                    val = r[f"{metric}_val"] if f"{metric}_val" in r.keys() else r["avg_val"]
                    result.append({"time": r["time"], "week_start": r["week_start"], "value": val, "min_val": r["min_val"], "max_val": r["max_val"], "avg_val": r["avg_val"]})
                return result

            elif interval == "month":
                query = f"""
                SELECT 
                    SUBSTR(bucket_date, 1, 7) as time,
                    MIN(min_val) as min_val,
                    MAX(max_val) as max_val,
                    AVG(avg_val) as avg_val,
                    SUM(sum_val) as sum_val
                FROM daily_stats
                WHERE entity_id = ?
                """
                params = [entity_id]
                if start_time:
                    query += " AND bucket_date >= ?"
                    params.append(start_time)
                if end_time:
                    query += " AND bucket_date <= ?"
                    params.append(end_time)
                query += " GROUP BY time ORDER BY time ASC"
                rows = conn.execute(query, params).fetchall()
                result = []
                for r in rows:
                    val = r[f"{metric}_val"] if f"{metric}_val" in r.keys() else r["avg_val"]
                    result.append({"time": r["time"], "value": val, "min_val": r["min_val"], "max_val": r["max_val"], "avg_val": r["avg_val"]})
                return result

            elif interval == "year":
                query = f"""
                SELECT 
                    SUBSTR(bucket_date, 1, 4) as time,
                    MIN(min_val) as min_val,
                    MAX(max_val) as max_val,
                    AVG(avg_val) as avg_val,
                    SUM(sum_val) as sum_val
                FROM daily_stats
                WHERE entity_id = ?
                """
                params = [entity_id]
                if start_time:
                    query += " AND bucket_date >= ?"
                    params.append(start_time)
                if end_time:
                    query += " AND bucket_date <= ?"
                    params.append(end_time)
                query += " GROUP BY time ORDER BY time ASC"
                rows = conn.execute(query, params).fetchall()
                result = []
                for r in rows:
                    val = r[f"{metric}_val"] if f"{metric}_val" in r.keys() else r["avg_val"]
                    result.append({"time": r["time"], "value": val, "min_val": r["min_val"], "max_val": r["max_val"], "avg_val": r["avg_val"]})
                return result

            return []

    def get_sensor_summary(self, entity_id: str) -> Dict[str, Any]:
        """Calculates current, 24h, 7d, 30d, 1y, and all-time statistics for a sensor."""
        with self.get_connection() as conn:
            # Entity meta
            entity = conn.execute("SELECT * FROM entities WHERE entity_id = ?", (entity_id,)).fetchone()
            if not entity:
                return {}

            # Latest reading
            latest = conn.execute(
                "SELECT timestamp, value, raw_value FROM sensor_data WHERE entity_id = ? ORDER BY timestamp DESC LIMIT 1",
                (entity_id,)
            ).fetchone()

            # Helper for time ranges
            now = datetime.datetime.now(datetime.timezone.utc)
            
            def stats_for_window(days: int):
                start = (now - datetime.timedelta(days=days)).isoformat()
                row = conn.execute("""
                SELECT MIN(min_val) as min_val, MAX(max_val) as max_val, AVG(avg_val) as avg_val
                FROM daily_stats
                WHERE entity_id = ? AND bucket_date >= ?
                """, (entity_id, start[:10])).fetchone()
                if row and row["min_val"] is not None:
                    return {"min": row["min_val"], "max": row["max_val"], "avg": round(row["avg_val"], 2) if row["avg_val"] else None}
                
                # Fallback to raw/hourly if daily rollup doesn't have it yet
                row2 = conn.execute("""
                SELECT MIN(value) as min_val, MAX(value) as max_val, AVG(value) as avg_val
                FROM sensor_data
                WHERE entity_id = ? AND timestamp >= ? AND value IS NOT NULL
                """, (entity_id, start)).fetchone()
                if row2 and row2["min_val"] is not None:
                    return {"min": row2["min_val"], "max": row2["max_val"], "avg": round(row2["avg_val"], 2) if row2["avg_val"] else None}
                return {"min": None, "max": None, "avg": None}

            all_time = conn.execute("""
            SELECT MIN(min_val) as min_val, MAX(max_val) as max_val, AVG(avg_val) as avg_val
            FROM daily_stats WHERE entity_id = ?
            """, (entity_id,)).fetchone()

            all_time_stats = {"min": None, "max": None, "avg": None}
            if all_time and all_time["min_val"] is not None:
                all_time_stats = {"min": all_time["min_val"], "max": all_time["max_val"], "avg": round(all_time["avg_val"], 2) if all_time["avg_val"] else None}
            else:
                raw_all = conn.execute("""
                SELECT MIN(value) as min_val, MAX(value) as max_val, AVG(value) as avg_val
                FROM sensor_data WHERE entity_id = ? AND value IS NOT NULL
                """, (entity_id,)).fetchone()
                if raw_all and raw_all["min_val"] is not None:
                    all_time_stats = {"min": raw_all["min_val"], "max": raw_all["max_val"], "avg": round(raw_all["avg_val"], 2) if raw_all["avg_val"] else None}

            return {
                "entity": dict(entity),
                "latest": dict(latest) if latest else None,
                "stats_24h": stats_for_window(1),
                "stats_7d": stats_for_window(7),
                "stats_30d": stats_for_window(30),
                "stats_365d": stats_for_window(365),
                "stats_all_time": all_time_stats
            }

    def get_heatmap_data(self, entity_id: str, year: Optional[int] = None) -> List[List[Any]]:
        """Returns [[YYYY-MM-DD, avg_value], ...] for ECharts calendar heatmap."""
        if not year:
            year = datetime.datetime.now().year
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
        with self.get_connection() as conn:
            rows = conn.execute("""
            SELECT bucket_date, avg_val, min_val, max_val
            FROM daily_stats
            WHERE entity_id = ? AND bucket_date >= ? AND bucket_date <= ?
            ORDER BY bucket_date ASC
            """, (entity_id, start_date, end_date)).fetchall()
            
            return [[r["bucket_date"], round(r["avg_val"], 2), round(r["min_val"], 2), round(r["max_val"], 2)] for r in rows]

    def get_period_comparison(self, entity_id: str, period_type: str = "month", metric: str = "avg") -> Dict[str, Any]:
        """Compares values by period (e.g. Month vs Month across different Years)."""
        metric = metric.lower()
        if metric not in ("avg", "min", "max", "sum"):
            metric = "avg"
            
        with self.get_connection() as conn:
            if period_type == "month":
                # Groups by Year and Month
                rows = conn.execute(f"""
                SELECT 
                    SUBSTR(bucket_date, 1, 4) as year,
                    SUBSTR(bucket_date, 6, 2) as month,
                    {metric}_val as value,
                    min_val, max_val, avg_val
                FROM daily_stats
                WHERE entity_id = ?
                """, (entity_id,)).fetchall()

                # Process into {year: {month_num: avg_val}}
                years_data: Dict[str, Dict[str, float]] = {}
                for r in rows:
                    yr = r["year"]
                    mo = r["month"]
                    val = r["avg_val"]
                    if yr not in years_data:
                        years_data[yr] = {}
                    if mo not in years_data[yr]:
                        years_data[yr][mo] = []
                    years_data[yr][mo].append(val)
                
                final_series = {}
                for yr, months in years_data.items():
                    final_series[yr] = {mo: round(sum(vals)/len(vals), 2) for mo, vals in months.items()}

                return {
                    "entity_id": entity_id,
                    "period_type": "month",
                    "metric": metric,
                    "series": final_series
                }
            return {}

    def get_db_status(self) -> Dict[str, Any]:
        db_file = Path(self.db_path)
        size_mb = round(db_file.stat().st_size / (1024 * 1024), 2) if db_file.exists() else 0.0

        with self.get_connection() as conn:
            total_raw = conn.execute("SELECT COUNT(*) FROM sensor_data").fetchone()[0]
            total_hourly = conn.execute("SELECT COUNT(*) FROM hourly_stats").fetchone()[0]
            total_daily = conn.execute("SELECT COUNT(*) FROM daily_stats").fetchone()[0]
            total_entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            active_entities = conn.execute("SELECT COUNT(*) FROM entities WHERE enabled = 1").fetchone()[0]

            return {
                "db_size_mb": size_mb,
                "total_raw_records": total_raw,
                "total_hourly_rollups": total_hourly,
                "total_daily_rollups": total_daily,
                "total_entities": total_entities,
                "active_entities": active_entities,
            }
