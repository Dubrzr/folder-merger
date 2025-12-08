"""Tests for folder_merger.db module."""

import pytest

from folder_merger.db import CheckpointDB
from folder_merger.models import FileInfo, ConflictRecord


class TestCheckpointDBInit:
    """Tests for CheckpointDB initialization."""

    def test_create_new_db(self, db_path):
        db = CheckpointDB(db_path)
        assert db_path.exists()
        db.close()

    def test_schema_created(self, checkpoint_db):
        # Verify tables exist by querying them
        cursor = checkpoint_db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}
        assert "metadata" in tables
        assert "scanned_files" in tables
        assert "processed_files" in tables
        assert "conflicts" in tables


class TestMetadata:
    """Tests for metadata operations."""

    def test_set_and_get_metadata(self, checkpoint_db):
        checkpoint_db.set_metadata("test_key", "test_value")
        result = checkpoint_db.get_metadata("test_key")
        assert result == "test_value"

    def test_get_missing_metadata_returns_none(self, checkpoint_db):
        result = checkpoint_db.get_metadata("nonexistent")
        assert result is None

    def test_get_missing_metadata_with_default(self, checkpoint_db):
        result = checkpoint_db.get_metadata("nonexistent", "default_value")
        assert result == "default_value"

    def test_update_metadata(self, checkpoint_db):
        checkpoint_db.set_metadata("key", "value1")
        checkpoint_db.set_metadata("key", "value2")
        result = checkpoint_db.get_metadata("key")
        assert result == "value2"


class TestFolderScanning:
    """Tests for folder scanning operations."""

    def test_is_folder_scanned_initially_false(self, checkpoint_db):
        assert checkpoint_db.is_folder_scanned(1) is False
        assert checkpoint_db.is_folder_scanned(2) is False

    def test_mark_folder_scanned(self, checkpoint_db):
        checkpoint_db.mark_folder_scanned(1)
        assert checkpoint_db.is_folder_scanned(1) is True
        assert checkpoint_db.is_folder_scanned(2) is False

    def test_save_and_get_scanned_file(self, checkpoint_db, sample_file_info):
        checkpoint_db.save_scanned_file(1, sample_file_info)
        files = checkpoint_db.get_scanned_files(1)
        assert sample_file_info.relative_path in files
        retrieved = files[sample_file_info.relative_path]
        assert retrieved.hash == sample_file_info.hash
        assert retrieved.size == sample_file_info.size

    def test_save_scanned_files_batch(self, checkpoint_db):
        files = {
            "file1.txt": FileInfo("file1.txt", "/path/file1.txt", "hash1", 100, 1000.0),
            "file2.txt": FileInfo("file2.txt", "/path/file2.txt", "hash2", 200, 2000.0),
            "file3.txt": FileInfo("file3.txt", "/path/file3.txt", "hash3", 300, 3000.0),
        }
        checkpoint_db.save_scanned_files_batch(1, files)
        retrieved = checkpoint_db.get_scanned_files(1)
        assert len(retrieved) == 3
        assert retrieved["file1.txt"].hash == "hash1"
        assert retrieved["file2.txt"].size == 200
        assert retrieved["file3.txt"].modified_time == 3000.0

    def test_get_scanned_files_empty_folder(self, checkpoint_db):
        files = checkpoint_db.get_scanned_files(1)
        assert files == {}

    def test_scanned_files_separate_by_folder(self, checkpoint_db):
        file1 = FileInfo("same.txt", "/folder1/same.txt", "hash1", 100, 1000.0)
        file2 = FileInfo("same.txt", "/folder2/same.txt", "hash2", 200, 2000.0)

        checkpoint_db.save_scanned_file(1, file1)
        checkpoint_db.save_scanned_file(2, file2)

        files1 = checkpoint_db.get_scanned_files(1)
        files2 = checkpoint_db.get_scanned_files(2)

        assert files1["same.txt"].hash == "hash1"
        assert files2["same.txt"].hash == "hash2"


class TestProcessedFiles:
    """Tests for processed files tracking."""

    def test_is_file_processed_initially_false(self, checkpoint_db):
        assert checkpoint_db.is_file_processed("any_file.txt") is False

    def test_mark_file_processed(self, checkpoint_db):
        checkpoint_db.mark_file_processed("test.txt")
        assert checkpoint_db.is_file_processed("test.txt") is True
        assert checkpoint_db.is_file_processed("other.txt") is False

    def test_mark_file_processed_idempotent(self, checkpoint_db):
        checkpoint_db.mark_file_processed("test.txt")
        checkpoint_db.mark_file_processed("test.txt")  # Should not raise
        assert checkpoint_db.is_file_processed("test.txt") is True

    def test_get_processed_count(self, checkpoint_db):
        assert checkpoint_db.get_processed_count() == 0
        checkpoint_db.mark_file_processed("file1.txt")
        assert checkpoint_db.get_processed_count() == 1
        checkpoint_db.mark_file_processed("file2.txt")
        assert checkpoint_db.get_processed_count() == 2
        checkpoint_db.mark_file_processed("file1.txt")  # Duplicate
        assert checkpoint_db.get_processed_count() == 2


class TestPhase:
    """Tests for phase tracking."""

    def test_get_phase_default(self, checkpoint_db):
        assert checkpoint_db.get_phase() == "scanning"

    def test_set_and_get_phase(self, checkpoint_db):
        checkpoint_db.set_phase("merging")
        assert checkpoint_db.get_phase() == "merging"

    def test_set_phase_multiple_times(self, checkpoint_db):
        checkpoint_db.set_phase("scanning")
        checkpoint_db.set_phase("copying")
        checkpoint_db.set_phase("resolving_conflicts")
        assert checkpoint_db.get_phase() == "resolving_conflicts"


class TestConflicts:
    """Tests for conflict logging."""

    def test_log_conflict(self, checkpoint_db, sample_conflict_record):
        checkpoint_db.log_conflict(sample_conflict_record)
        assert checkpoint_db.get_conflict_count() == 1

    def test_get_previous_resolution(self, checkpoint_db):
        record = ConflictRecord(
            relative_path="test.txt",
            file1_info={"hash": "abc"},
            file2_info={"hash": "def"},
            resolution="prefer_recent",
            chosen_source="folder1",
            resolved_at="2024-01-01T00:00:00"
        )
        checkpoint_db.log_conflict(record)

        result = checkpoint_db.get_previous_resolution("test.txt")
        assert result is not None
        assert result.relative_path == "test.txt"
        assert result.resolution == "prefer_recent"
        assert result.chosen_source == "folder1"

    def test_get_previous_resolution_none_when_not_found(self, checkpoint_db):
        result = checkpoint_db.get_previous_resolution("nonexistent.txt")
        assert result is None

    def test_get_previous_resolution_ignores_pending(self, checkpoint_db):
        record = ConflictRecord(
            relative_path="test.txt",
            file1_info={},
            file2_info={},
            resolution="pending",
            chosen_source="",
            resolved_at=None
        )
        checkpoint_db.log_conflict(record)
        result = checkpoint_db.get_previous_resolution("test.txt")
        assert result is None

    def test_get_previous_resolution_returns_latest(self, checkpoint_db):
        record1 = ConflictRecord(
            relative_path="test.txt",
            file1_info={},
            file2_info={},
            resolution="prefer_recent",
            chosen_source="folder1",
            resolved_at="2024-01-01T00:00:00"
        )
        record2 = ConflictRecord(
            relative_path="test.txt",
            file1_info={},
            file2_info={},
            resolution="prefer_oldest",
            chosen_source="folder2",
            resolved_at="2024-01-02T00:00:00"
        )
        checkpoint_db.log_conflict(record1)
        checkpoint_db.log_conflict(record2)

        result = checkpoint_db.get_previous_resolution("test.txt")
        assert result.resolution == "prefer_oldest"
        assert result.chosen_source == "folder2"

    def test_get_conflict_count(self, checkpoint_db):
        assert checkpoint_db.get_conflict_count() == 0

        for i in range(5):
            record = ConflictRecord(
                relative_path=f"file{i}.txt",
                file1_info={},
                file2_info={},
                resolution="prefer_recent",
                chosen_source="folder1",
                resolved_at=None
            )
            checkpoint_db.log_conflict(record)

        assert checkpoint_db.get_conflict_count() == 5


class TestClear:
    """Tests for database clearing."""

    def test_clear_deletes_file(self, db_path):
        db = CheckpointDB(db_path)
        db.set_metadata("key", "value")
        assert db_path.exists()

        db.clear()
        assert not db_path.exists()

    def test_clear_on_nonexistent_file(self, db_path):
        db = CheckpointDB(db_path)
        db.close()
        db_path.unlink()

        # Create new instance and clear - should not raise
        db2 = CheckpointDB(db_path)
        db2.clear()  # File may or may not exist at this point


class TestPersistence:
    """Tests for data persistence across sessions."""

    def test_data_persists_after_close(self, db_path):
        # First session
        db1 = CheckpointDB(db_path)
        db1.set_metadata("key", "value")
        db1.mark_folder_scanned(1)
        db1.mark_file_processed("test.txt")
        db1.close()

        # Second session
        db2 = CheckpointDB(db_path)
        assert db2.get_metadata("key") == "value"
        assert db2.is_folder_scanned(1) is True
        assert db2.is_file_processed("test.txt") is True
        db2.close()


class TestBatchSaveRollback:
    """Tests for batch save rollback on error."""

    def test_save_scanned_files_batch_rollback_on_error(self, db_path):
        """Test that batch save rolls back on error."""
        from folder_merger.db import CheckpointDB

        # Create a custom CheckpointDB subclass that raises during batch save
        class FailingCheckpointDB(CheckpointDB):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.insert_count = 0

            def save_scanned_files_batch(self, folder: int, files: dict):
                """Override to simulate failure during batch insert."""
                self.conn.execute("BEGIN")
                try:
                    for file_info in files.values():
                        self.insert_count += 1
                        if self.insert_count > 1:
                            raise Exception("Simulated error")
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

        db = FailingCheckpointDB(db_path)

        files = {
            "file1.txt": FileInfo("file1.txt", "/path/file1.txt", "h1", 100, 1000.0),
            "file2.txt": FileInfo("file2.txt", "/path/file2.txt", "h2", 200, 2000.0),
        }

        with pytest.raises(Exception, match="Simulated error"):
            db.save_scanned_files_batch(1, files)

        # Should have no files due to rollback
        assert len(db.get_scanned_files(1)) == 0
        db.close()