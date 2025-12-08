"""Tests for folder_merger.models module."""

import pytest

from folder_merger.models import ConflictResolution, FileInfo, ConflictRecord


class TestConflictResolution:
    """Tests for ConflictResolution enum."""

    def test_prefer_recent_value(self):
        assert ConflictResolution.PREFER_RECENT.value == "prefer_recent"

    def test_prefer_oldest_value(self):
        assert ConflictResolution.PREFER_OLDEST.value == "prefer_oldest"

    def test_pending_value(self):
        assert ConflictResolution.PENDING.value == "pending"


class TestFileInfo:
    """Tests for FileInfo dataclass."""

    def test_create_file_info(self):
        info = FileInfo(
            relative_path="test.txt",
            absolute_path="/path/to/test.txt",
            hash="abc123",
            size=100,
            modified_time=1700000000.0
        )
        assert info.relative_path == "test.txt"
        assert info.absolute_path == "/path/to/test.txt"
        assert info.hash == "abc123"
        assert info.size == 100
        assert info.modified_time == 1700000000.0

    def test_to_dict(self, sample_file_info):
        result = sample_file_info.to_dict()
        assert isinstance(result, dict)
        assert result["relative_path"] == "test/file.txt"
        assert result["absolute_path"] == "/absolute/test/file.txt"
        assert result["hash"] == "abc123def456"
        assert result["size"] == 1024
        assert result["modified_time"] == 1700000000.0

    def test_from_dict(self):
        data = {
            "relative_path": "from_dict.txt",
            "absolute_path": "/path/from_dict.txt",
            "hash": "xyz789",
            "size": 500,
            "modified_time": 1600000000.0
        }
        info = FileInfo.from_dict(data)
        assert info.relative_path == "from_dict.txt"
        assert info.absolute_path == "/path/from_dict.txt"
        assert info.hash == "xyz789"
        assert info.size == 500
        assert info.modified_time == 1600000000.0

    def test_roundtrip_dict(self, sample_file_info):
        """Test that to_dict -> from_dict preserves data."""
        data = sample_file_info.to_dict()
        restored = FileInfo.from_dict(data)
        assert restored == sample_file_info


class TestConflictRecord:
    """Tests for ConflictRecord dataclass."""

    def test_create_conflict_record(self):
        record = ConflictRecord(
            relative_path="conflict.txt",
            file1_info={"hash": "abc"},
            file2_info={"hash": "def"},
            resolution="prefer_recent",
            chosen_source="folder1"
        )
        assert record.relative_path == "conflict.txt"
        assert record.file1_info == {"hash": "abc"}
        assert record.file2_info == {"hash": "def"}
        assert record.resolution == "prefer_recent"
        assert record.chosen_source == "folder1"
        assert record.resolved_at is None

    def test_create_with_resolved_at(self):
        record = ConflictRecord(
            relative_path="conflict.txt",
            file1_info={},
            file2_info={},
            resolution="prefer_oldest",
            chosen_source="folder2",
            resolved_at="2024-01-01T00:00:00"
        )
        assert record.resolved_at == "2024-01-01T00:00:00"

    def test_to_dict(self, sample_conflict_record):
        result = sample_conflict_record.to_dict()
        assert isinstance(result, dict)
        assert result["relative_path"] == "conflict.txt"
        assert result["resolution"] == "prefer_recent"
        assert result["chosen_source"] == "folder1"
        assert result["resolved_at"] == "2024-01-01T12:00:00"

    def test_from_dict(self):
        data = {
            "relative_path": "test.txt",
            "file1_info": {"a": 1},
            "file2_info": {"b": 2},
            "resolution": "prefer_oldest",
            "chosen_source": "folder2",
            "resolved_at": None
        }
        record = ConflictRecord.from_dict(data)
        assert record.relative_path == "test.txt"
        assert record.file1_info == {"a": 1}
        assert record.file2_info == {"b": 2}
        assert record.resolution == "prefer_oldest"
        assert record.chosen_source == "folder2"
        assert record.resolved_at is None

    def test_roundtrip_dict(self, sample_conflict_record):
        """Test that to_dict -> from_dict preserves data."""
        data = sample_conflict_record.to_dict()
        restored = ConflictRecord.from_dict(data)
        assert restored == sample_conflict_record