import asyncio
import json
import logging
import re
from pathlib import Path

import aiofiles
import aiosqlite

from ..models import DataPoint
from .base import AsyncSink, TryAgainError

logger = logging.getLogger(__name__)


def _sanitize_table_name(name: str) -> str:
    """Convert measurement name to valid SQLite table name."""
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    if name and name[0].isdigit():
        name = f"t_{name}"
    return name.lower()


class SQLiteSink(AsyncSink):
    """
    SQLite sink - one table per measurement + Grafana-friendly view.
    
    Config example:
    [[sinks]]
    name = "local_sqlite"
    type = "sqlite"
    config = {
        "db_path": "data/local.db",
        "batch_size": 2000,
        "journal_mode": "WAL"
    }
    """

    def __init__(self, config: dict, base_data_dir: Path):
        super().__init__(config, base_data_dir)

        sink_cfg = self.config

        db_path = sink_cfg.get("db_path", "local.db")
        self.db_path = Path(db_path)

        self.batch_size: int = sink_cfg.get("batch_size", 2000)
        self.journal_mode: str = sink_cfg.get("journal_mode", "WAL").upper()

        self._conn: aiosqlite.Connection | None = None
        self._known_tables: set[str] = set()

        logger.info(f"SQLiteSink '{self.name}' initialized → {self.db_path} (journal_mode={self.journal_mode})")

    async def _ensure_db(self) -> aiosqlite.Connection:
        """Ensure database exists with good performance settings."""
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            
            self._conn = await aiosqlite.connect(str(self.db_path))
            
            await self._conn.execute(f"PRAGMA journal_mode = {self.journal_mode}")
            await self._conn.execute("PRAGMA synchronous = NORMAL")
            await self._conn.execute("PRAGMA cache_size = -64000")  # ~64MB
            await self._conn.commit()

        return self._conn

    async def _ensure_table(self, measurement: str) -> str:
        """Ensure table + Grafana view exist for this measurement."""
        table_name = _sanitize_table_name(measurement)
        
        if table_name in self._known_tables:
            return table_name

        conn = await self._ensure_db()

        # Main table (compact storage)
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS "{table_name}" (
                time        REAL NOT NULL,
                tags        TEXT NOT NULL,
                fields      TEXT NOT NULL,
                tcreate TEXT DEFAULT (datetime('now'))
            )
        """)

        # Grafana-friendly view
        await conn.execute(f"""
            CREATE VIEW IF NOT EXISTS "v_{table_name}" AS
            SELECT 
                datetime(time, 'unixepoch') AS timestamp,
                time AS unix_time,
                tags,
                fields,
                json_extract(tags, '$.id') as device_id,
                tcreate
            FROM "{table_name}"
        """)

        # Performance indexes
        await conn.execute(f'CREATE INDEX IF NOT EXISTS "idx_{table_name}_time" ON "{table_name}"(time)')
        
        await conn.commit()
        self._known_tables.add(table_name)
        
        logger.info(f"Ensured table + view for measurement '{measurement}' → {table_name}")
        return table_name

    async def process_file(self, file_path: Path) -> None:
        """Process JSONL file and insert into per-measurement tables."""
        conn = await self._ensure_db()
        batch: dict[str, list[tuple]] = {}   # table_name → rows
        processed = 0

        try:
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                async for raw_line in f:
                    line = raw_line.strip()
                    if not line:
                        continue

                    try:
                        dp = DataPoint.from_json(line)
                    except Exception as e:
                        await self._write_line_to_jail(line, e, file_path)
                        continue

                    table_name = await self._ensure_table(dp.measurement)

                    row = (
                        dp.time,
                        json.dumps(dp.tags),
                        json.dumps(dp.fields)
                    )

                    if table_name not in batch:
                        batch[table_name] = []
                    batch[table_name].append(row)
                    processed += 1

                    if any(len(rows) >= self.batch_size for rows in batch.values()):
                        await self._flush_batches(conn, batch)

            if batch:
                await self._flush_batches(conn, batch)

            logger.info(f"SQLiteSink '{self.name}' inserted {processed} points from {file_path.name}")

        except Exception as e:
            logger.error(f"SQLiteSink '{self.name}' error: {e}")
            raise TryAgainError(f"SQLite error: {e}") from e

    async def _flush_batches(self, conn: aiosqlite.Connection, batch: dict[str, list[tuple]]) -> None:
        """Insert all pending batches."""
        for table_name, rows in list(batch.items()):
            if not rows:
                continue
            async with conn.executemany(f"""
                INSERT INTO "{table_name}" (time, tags, fields)
                VALUES (?, ?, ?)
            """, rows):
                pass
            batch[table_name] = []   # clear processed

        await conn.commit()

    async def stop(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None
        await super().stop()