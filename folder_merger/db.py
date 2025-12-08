"""SQLite-backed checkpoint database for durable state persistence."""

import json
import sqlite3
from pathlib import Path
from typing import Optional

from .models import FileInfo, ConflictRecord


class CheckpointDB:
    """SQLite-backed checkpoint manager for durable state persistence."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path), isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS scanned_files (
                folder INTEGER NOT NULL,
                relative_path TEXT NOT NULL,
                absolute_path TEXT NOT NULL,
                hash TEXT NOT NULL,
                size INTEGER NOT NULL,
                modified_time REAL NOT NULL,
                PRIMARY KEY (folder, relative_path)
            );

            CREATE TABLE IF NOT EXISTS processed_files (
                relative_path TEXT PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS conflicts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                relative_path TEXT NOT NULL,
                file1_info TEXT NOT NULL,
                file2_info TEXT NOT NULL,
                resolution TEXT NOT NULL,
                chosen_source TEXT NOT NULL,
                resolved_at TEXT
            );
        """)

    def close(self) -> None:
        """Close the database connection."""
        self.conn.close()

    def get_metadata(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a metadata value."""
        cursor = self.conn.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        )
        row = cursor.fetchone()
        return row[0] if row else default

    def set_metadata(self, key: str, value: str) -> None:
        """Set a metadata value."""
        self.conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
            (key, value)
        )

    def is_folder_scanned(self, folder: int) -> bool:
        """Check if a folder has been scanned."""
        return self.get_metadata(f"folder{folder}_scanned") == "true"

    def mark_folder_scanned(self, folder: int) -> None:
        """Mark a folder as scanned."""
        self.set_metadata(f"folder{folder}_scanned", "true")

    def save_scanned_file(self, folder: int, file_info: FileInfo) -> None:
        """Save a scanned file's information."""
        self.conn.execute(
            """INSERT OR REPLACE INTO scanned_files
               (folder, relative_path, absolute_path, hash, size, modified_time)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (folder, file_info.relative_path, file_info.absolute_path,
             file_info.hash, file_info.size, file_info.modified_time)
        )

    def save_scanned_files_batch(self, folder: int, files: dict[str, FileInfo]) -> None:
        """Save multiple scanned files in a single transaction."""
        self.conn.execute("BEGIN")
        try:
            for file_info in files.values():
                self.conn.execute(
                    """INSERT OR REPLACE INTO scanned_files
                       (folder, relative_path, absolute_path, hash, size, modified_time)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (folder, file_info.relative_path, file_info.absolute_path,
                     file_info.hash, file_info.size, file_info.modified_time)
                )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    def get_scanned_files(self, folder: int) -> dict[str, FileInfo]:
        """Get all scanned files for a folder."""
        cursor = self.conn.execute(
            """SELECT relative_path, absolute_path, hash, size, modified_time
               FROM scanned_files WHERE folder = ?""",
            (folder,)
        )
        files = {}
        for row in cursor:
            files[row[0]] = FileInfo(
                relative_path=row[0],
                absolute_path=row[1],
                hash=row[2],
                size=row[3],
                modified_time=row[4]
            )
        return files

    def mark_file_processed(self, relative_path: str) -> None:
        """Mark a file as processed."""
        self.conn.execute(
            "INSERT OR IGNORE INTO processed_files (relative_path) VALUES (?)",
            (relative_path,)
        )

    def is_file_processed(self, relative_path: str) -> bool:
        """Check if a file has been processed."""
        cursor = self.conn.execute(
            "SELECT 1 FROM processed_files WHERE relative_path = ?",
            (relative_path,)
        )
        return cursor.fetchone() is not None

    def get_processed_count(self) -> int:
        """Get the count of processed files."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM processed_files")
        return cursor.fetchone()[0]

    def set_phase(self, phase: str) -> None:
        """Set the current phase."""
        self.set_metadata("phase", phase)

    def get_phase(self) -> str:
        """Get the current phase."""
        return self.get_metadata("phase", "scanning")

    def log_conflict(self, record: ConflictRecord) -> None:
        """Log a conflict resolution."""
        self.conn.execute(
            """INSERT INTO conflicts
               (relative_path, file1_info, file2_info, resolution, chosen_source, resolved_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (record.relative_path, json.dumps(record.file1_info),
             json.dumps(record.file2_info), record.resolution,
             record.chosen_source, record.resolved_at)
        )

    def get_previous_resolution(self, relative_path: str) -> Optional[ConflictRecord]:
        """Get a previous resolution for a file path."""
        cursor = self.conn.execute(
            """SELECT relative_path, file1_info, file2_info, resolution, chosen_source, resolved_at
               FROM conflicts WHERE relative_path = ? AND resolution != 'pending'
               ORDER BY id DESC LIMIT 1""",
            (relative_path,)
        )
        row = cursor.fetchone()
        if row:
            return ConflictRecord(
                relative_path=row[0],
                file1_info=json.loads(row[1]),
                file2_info=json.loads(row[2]),
                resolution=row[3],
                chosen_source=row[4],
                resolved_at=row[5]
            )
        return None

    def get_conflict_count(self) -> int:
        """Get the total number of conflicts logged."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM conflicts")
        return cursor.fetchone()[0]

    def clear(self) -> None:
        """Delete the database file."""
        self.conn.close()
        if self.db_path.exists():
            self.db_path.unlink()