"""Shared test fixtures."""

import os
import tempfile
from pathlib import Path

import pytest

from folder_merger.db import CheckpointDB
from folder_merger.models import FileInfo, ConflictRecord


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_folders(temp_dir):
    """Create sample folder structures for testing."""
    folder1 = temp_dir / "folder1"
    folder2 = temp_dir / "folder2"
    output = temp_dir / "output"

    folder1.mkdir()
    folder2.mkdir()

    # Create files only in folder1
    (folder1 / "only_in_1.txt").write_text("only in folder 1")
    (folder1 / "subdir").mkdir()
    (folder1 / "subdir" / "nested.txt").write_text("nested in folder 1")

    # Create files only in folder2
    (folder2 / "only_in_2.txt").write_text("only in folder 2")

    # Create identical files in both
    (folder1 / "identical.txt").write_text("same content")
    (folder2 / "identical.txt").write_text("same content")

    # Create conflicting files (same name, different content)
    (folder1 / "conflict.txt").write_text("content from folder 1")
    (folder2 / "conflict.txt").write_text("content from folder 2")

    return folder1, folder2, output


@pytest.fixture
def db_path(temp_dir):
    """Create a temporary database path."""
    return temp_dir / "test_checkpoint.db"


@pytest.fixture
def checkpoint_db(db_path):
    """Create a CheckpointDB instance."""
    db = CheckpointDB(db_path)
    yield db
    try:
        db.close()
    except Exception:
        pass


@pytest.fixture
def sample_file_info():
    """Create a sample FileInfo for testing."""
    return FileInfo(
        relative_path="test/file.txt",
        absolute_path="/absolute/test/file.txt",
        hash="abc123def456",
        size=1024,
        modified_time=1700000000.0
    )


@pytest.fixture
def sample_conflict_record():
    """Create a sample ConflictRecord for testing."""
    return ConflictRecord(
        relative_path="conflict.txt",
        file1_info={"hash": "abc123", "size": 100},
        file2_info={"hash": "def456", "size": 200},
        resolution="prefer_recent",
        chosen_source="folder1",
        resolved_at="2024-01-01T12:00:00"
    )