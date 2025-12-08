"""Tests for folder_merger.merger module."""

import os
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from folder_merger.db import CheckpointDB
from folder_merger.merger import (
    open_file_in_viewer,
    format_timestamp,
    format_size,
    copy_file,
    safe_copy_file,
    CopyError,
    resolve_conflict,
    merge_folders,
)
from folder_merger.models import FileInfo, ConflictRecord


class TestOpenFileInViewer:
    """Tests for open_file_in_viewer function."""

    @patch("folder_merger.merger.platform.system")
    @patch("folder_merger.merger.os.startfile")
    def test_open_file_windows(self, mock_startfile, mock_system):
        mock_system.return_value = "Windows"
        open_file_in_viewer("/path/to/file.txt")
        mock_startfile.assert_called_once_with("/path/to/file.txt")

    @patch("folder_merger.merger.platform.system")
    @patch("folder_merger.merger.subprocess.run")
    def test_open_file_macos(self, mock_run, mock_system):
        mock_system.return_value = "Darwin"
        open_file_in_viewer("/path/to/file.txt")
        mock_run.assert_called_once_with(["open", "/path/to/file.txt"], check=True)

    @patch("folder_merger.merger.platform.system")
    @patch("folder_merger.merger.subprocess.run")
    def test_open_file_linux(self, mock_run, mock_system):
        mock_system.return_value = "Linux"
        open_file_in_viewer("/path/to/file.txt")
        mock_run.assert_called_once_with(["xdg-open", "/path/to/file.txt"], check=True)

    @patch("folder_merger.merger.platform.system")
    @patch("folder_merger.merger.subprocess.run")
    def test_open_file_error_handled(self, mock_run, mock_system, capsys):
        mock_system.return_value = "Linux"
        mock_run.side_effect = Exception("Test error")

        open_file_in_viewer("/path/to/file.txt")

        captured = capsys.readouterr()
        assert "Could not open file" in captured.out
        assert "Please manually open" in captured.out


class TestFormatTimestamp:
    """Tests for format_timestamp function."""

    def test_format_timestamp(self):
        # Use a known timestamp
        timestamp = 1700000000.0
        result = format_timestamp(timestamp)
        assert isinstance(result, str)
        assert "2023" in result  # Year should be 2023

    def test_format_timestamp_format(self):
        timestamp = datetime(2024, 6, 15, 10, 30, 45).timestamp()
        result = format_timestamp(timestamp)
        assert "2024-06-15" in result
        assert "10:30:45" in result


class TestFormatSize:
    """Tests for format_size function."""

    def test_format_bytes(self):
        assert "100.0 B" == format_size(100)

    def test_format_kilobytes(self):
        assert "1.0 KB" == format_size(1024)
        assert "1.5 KB" == format_size(1536)

    def test_format_megabytes(self):
        assert "1.0 MB" == format_size(1024 * 1024)

    def test_format_gigabytes(self):
        assert "1.0 GB" == format_size(1024 * 1024 * 1024)

    def test_format_terabytes(self):
        assert "1.0 TB" == format_size(1024 * 1024 * 1024 * 1024)


class TestCopyFile:
    """Tests for copy_file function."""

    def test_copy_file_basic(self, temp_dir):
        src = temp_dir / "source.txt"
        dst = temp_dir / "dest.txt"
        src.write_text("content")

        copy_file(src, dst)

        assert dst.exists()
        assert dst.read_text() == "content"

    def test_copy_file_creates_parent_dirs(self, temp_dir):
        src = temp_dir / "source.txt"
        dst = temp_dir / "subdir" / "nested" / "dest.txt"
        src.write_text("content")

        copy_file(src, dst)

        assert dst.exists()
        assert dst.read_text() == "content"

    def test_copy_file_preserves_metadata(self, temp_dir):
        src = temp_dir / "source.txt"
        dst = temp_dir / "dest.txt"
        src.write_text("content")

        # Get original mtime
        original_mtime = src.stat().st_mtime

        copy_file(src, dst)

        # shutil.copy2 should preserve mtime
        assert dst.stat().st_mtime == original_mtime


class TestCopyError:
    """Tests for CopyError class and safe_copy_file function."""

    def test_copy_error_attributes(self):
        error = CopyError("rel/path.txt", "/src/path.txt", "/dst/path.txt", "File not found")
        assert error.relative_path == "rel/path.txt"
        assert error.src_path == "/src/path.txt"
        assert error.dst_path == "/dst/path.txt"
        assert error.error == "File not found"

    def test_safe_copy_file_success(self, temp_dir):
        src = temp_dir / "source.txt"
        dst = temp_dir / "dest.txt"
        src.write_text("content")

        error = safe_copy_file(src, dst, "source.txt")

        assert error is None
        assert dst.exists()
        assert dst.read_text() == "content"

    def test_safe_copy_file_nonexistent_source(self, temp_dir):
        src = temp_dir / "nonexistent.txt"
        dst = temp_dir / "dest.txt"

        error = safe_copy_file(src, dst, "nonexistent.txt")

        assert error is not None
        assert error.relative_path == "nonexistent.txt"
        assert "nonexistent" in error.src_path
        assert not dst.exists()

    def test_safe_copy_file_returns_copy_error_on_failure(self, temp_dir):
        src = temp_dir / "source.txt"
        dst = temp_dir / "dest.txt"
        src.write_text("content")

        with patch('folder_merger.merger.copy_file', side_effect=OSError("Simulated error")):
            error = safe_copy_file(src, dst, "source.txt")

        assert error is not None
        assert isinstance(error, CopyError)
        assert error.relative_path == "source.txt"
        assert "Simulated error" in error.error


class TestResolveConflict:
    """Tests for resolve_conflict function."""

    @pytest.fixture
    def conflict_files(self):
        """Create two conflicting FileInfo objects."""
        file1 = FileInfo(
            relative_path="conflict.txt",
            absolute_path="/folder1/conflict.txt",
            hash="hash1",
            size=100,
            modified_time=2000.0  # More recent
        )
        file2 = FileInfo(
            relative_path="conflict.txt",
            absolute_path="/folder2/conflict.txt",
            hash="hash2",
            size=200,
            modified_time=1000.0  # Older
        )
        return file1, file2

    def test_resolve_conflict_prefer_recent(self, conflict_files, checkpoint_db):
        file1, file2 = conflict_files

        with patch("builtins.input", return_value="1"):
            chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file1  # file1 is more recent
        assert resolution == "prefer_recent"

    def test_resolve_conflict_prefer_oldest(self, conflict_files, checkpoint_db):
        file1, file2 = conflict_files

        with patch("builtins.input", return_value="2"):
            chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file2  # file2 is older
        assert resolution == "prefer_oldest"

    def test_resolve_conflict_open_files_then_choose(self, conflict_files, checkpoint_db):
        file1, file2 = conflict_files

        with patch("builtins.input", side_effect=["3", "1"]):
            with patch("folder_merger.merger.open_file_in_viewer"):
                chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file1
        assert resolution == "prefer_recent"

    def test_resolve_conflict_invalid_then_valid(self, conflict_files, checkpoint_db):
        file1, file2 = conflict_files

        with patch("builtins.input", side_effect=["invalid", "x", "1"]):
            chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file1
        assert resolution == "prefer_recent"

    def test_resolve_conflict_logs_to_db(self, conflict_files, checkpoint_db):
        file1, file2 = conflict_files

        with patch("builtins.input", return_value="1"):
            resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert checkpoint_db.get_conflict_count() == 1
        prev = checkpoint_db.get_previous_resolution("conflict.txt")
        assert prev is not None
        assert prev.resolution == "prefer_recent"

    def test_resolve_conflict_uses_previous_resolution_folder1(self, conflict_files, checkpoint_db):
        """Test using previous resolution that chose folder1."""
        file1, file2 = conflict_files

        # Log a previous resolution that chose folder1
        record = ConflictRecord(
            relative_path="conflict.txt",
            file1_info=file1.to_dict(),
            file2_info=file2.to_dict(),
            resolution="prefer_recent",
            chosen_source="folder1",  # Previous chose folder1
            resolved_at="2024-01-01T00:00:00"
        )
        checkpoint_db.log_conflict(record)

        # Should use previous resolution without prompting
        chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file1  # Should return file1
        assert resolution == "prefer_recent"

    def test_resolve_conflict_same_time_prefers_file1(self, checkpoint_db):
        file1 = FileInfo("test.txt", "/f1/test.txt", "h1", 100, 1000.0)
        file2 = FileInfo("test.txt", "/f2/test.txt", "h2", 200, 1000.0)  # Same time

        with patch("builtins.input", return_value="1"):  # Prefer recent
            chosen, _ = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        # When same time, file1 is chosen (>= comparison)
        assert chosen == file1

    def test_resolve_conflict_uses_previous_resolution_folder2(self, checkpoint_db):
        """Test using previous resolution that chose folder2."""
        file1 = FileInfo("test.txt", "/f1/test.txt", "h1", 100, 2000.0)
        file2 = FileInfo("test.txt", "/f2/test.txt", "h2", 200, 1000.0)

        # Log a previous resolution that chose folder2
        record = ConflictRecord(
            relative_path="test.txt",
            file1_info=file1.to_dict(),
            file2_info=file2.to_dict(),
            resolution="prefer_oldest",
            chosen_source="folder2",  # Previous chose folder2
            resolved_at="2024-01-01T00:00:00"
        )
        checkpoint_db.log_conflict(record)

        chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file2  # Should return file2
        assert resolution == "prefer_oldest"

    def test_resolve_conflict_file2_more_recent(self, checkpoint_db):
        """Test when file2 is more recent."""
        file1 = FileInfo("test.txt", "/f1/test.txt", "h1", 100, 1000.0)  # Older
        file2 = FileInfo("test.txt", "/f2/test.txt", "h2", 200, 2000.0)  # More recent

        with patch("builtins.input", return_value="1"):  # Prefer recent
            chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file2  # file2 is more recent
        assert resolution == "prefer_recent"

    def test_resolve_conflict_prefer_oldest_file2_older(self, checkpoint_db):
        """Test prefer oldest when file2 is older."""
        file1 = FileInfo("test.txt", "/f1/test.txt", "h1", 100, 2000.0)  # More recent
        file2 = FileInfo("test.txt", "/f2/test.txt", "h2", 200, 1000.0)  # Older

        with patch("builtins.input", return_value="2"):  # Prefer oldest
            chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file2  # file2 is older
        assert resolution == "prefer_oldest"

    def test_resolve_conflict_prefer_oldest_file1_older(self, checkpoint_db):
        """Test prefer oldest when file1 is older."""
        file1 = FileInfo("test.txt", "/f1/test.txt", "h1", 100, 1000.0)  # Older
        file2 = FileInfo("test.txt", "/f2/test.txt", "h2", 200, 2000.0)  # More recent

        with patch("builtins.input", return_value="2"):  # Prefer oldest
            chosen, resolution = resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        assert chosen == file1  # file1 is older
        assert resolution == "prefer_oldest"

    def test_resolve_conflict_same_time_label(self, checkpoint_db, capsys):
        """Test that 'Same time' label is shown when files have same mtime."""
        file1 = FileInfo("test.txt", "/f1/test.txt", "h1", 100, 1000.0)
        file2 = FileInfo("test.txt", "/f2/test.txt", "h2", 200, 1000.0)

        with patch("builtins.input", return_value="1"):
            resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        captured = capsys.readouterr()
        assert "Same time" in captured.out

    def test_resolve_conflict_folder2_more_recent_label(self, checkpoint_db, capsys):
        """Test that 'Folder 2' label is shown when file2 is more recent."""
        file1 = FileInfo("test.txt", "/f1/test.txt", "h1", 100, 1000.0)
        file2 = FileInfo("test.txt", "/f2/test.txt", "h2", 200, 2000.0)

        with patch("builtins.input", return_value="1"):
            resolve_conflict(file1, file2, checkpoint_db, 1, 1)

        captured = capsys.readouterr()
        assert "More recent: Folder 2" in captured.out


class TestMergeFolders:
    """Tests for merge_folders function."""

    def test_merge_folders_basic(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        db = CheckpointDB(temp_dir / "test.db")

        with patch("builtins.input", return_value="1"):  # Always prefer recent
            merge_folders(folder1, folder2, output, db)

        # Check output contains all expected files
        assert (output / "only_in_1.txt").exists()
        assert (output / "only_in_2.txt").exists()
        assert (output / "identical.txt").exists()
        assert (output / "conflict.txt").exists()

    def test_merge_folders_creates_output_dir(self, sample_folders, temp_dir):
        folder1, folder2, _ = sample_folders
        output = temp_dir / "new" / "nested" / "output"
        db = CheckpointDB(temp_dir / "test.db")

        with patch("builtins.input", return_value="1"):
            merge_folders(folder1, folder2, output, db)

        assert output.exists()
        assert output.is_dir()

    def test_merge_folders_preserves_subdir_structure(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        db = CheckpointDB(temp_dir / "test.db")

        with patch("builtins.input", return_value="1"):
            merge_folders(folder1, folder2, output, db)

        # Check nested file exists
        nested_path = output / "subdir" / "nested.txt"
        assert nested_path.exists()

    def test_merge_folders_resume_from_checkpoint(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        db_path = temp_dir / "test.db"

        # First run - simulate interruption by creating partial state
        db1 = CheckpointDB(db_path)
        from folder_merger.scanner import scan_folder
        files1, _ = scan_folder(folder1)
        db1.save_scanned_files_batch(1, files1)
        db1.mark_folder_scanned(1)
        db1.close()

        # Second run - should resume
        db2 = CheckpointDB(db_path)

        with patch("builtins.input", return_value="1"):
            merge_folders(folder1, folder2, output, db2)

        assert (output / "only_in_1.txt").exists()

    def test_merge_folders_no_conflicts(self, temp_dir):
        folder1 = temp_dir / "f1"
        folder2 = temp_dir / "f2"
        output = temp_dir / "out"
        folder1.mkdir()
        folder2.mkdir()

        (folder1 / "file1.txt").write_text("content1")
        (folder2 / "file2.txt").write_text("content2")

        db = CheckpointDB(temp_dir / "test.db")
        merge_folders(folder1, folder2, output, db)

        assert (output / "file1.txt").exists()
        assert (output / "file2.txt").exists()

    def test_merge_folders_identical_files_copied_once(self, temp_dir):
        folder1 = temp_dir / "f1"
        folder2 = temp_dir / "f2"
        output = temp_dir / "out"
        folder1.mkdir()
        folder2.mkdir()

        (folder1 / "same.txt").write_text("identical")
        (folder2 / "same.txt").write_text("identical")

        db = CheckpointDB(temp_dir / "test.db")
        merge_folders(folder1, folder2, output, db)

        assert (output / "same.txt").exists()
        assert (output / "same.txt").read_text() == "identical"

    def test_merge_folders_clears_db_on_completion(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        db_path = temp_dir / "test.db"
        db = CheckpointDB(db_path)

        with patch("builtins.input", return_value="1"):
            merge_folders(folder1, folder2, output, db)

        # DB should be cleared/deleted on successful completion
        assert not db_path.exists()

    def test_merge_folders_all_non_conflicting_already_copied(self, temp_dir):
        """Test when all non-conflicting files are already copied (resume scenario)."""
        folder1 = temp_dir / "f1"
        folder2 = temp_dir / "f2"
        output = temp_dir / "out"
        folder1.mkdir()
        folder2.mkdir()
        output.mkdir()

        (folder1 / "file1.txt").write_text("content1")
        (folder2 / "file2.txt").write_text("content2")

        # Pre-populate output and mark as processed
        (output / "file1.txt").write_text("content1")
        (output / "file2.txt").write_text("content2")

        db_path = temp_dir / "test.db"
        db = CheckpointDB(db_path)

        # Scan folders first
        from folder_merger.scanner import scan_folder
        files1, _ = scan_folder(folder1)
        files2, _ = scan_folder(folder2)
        db.save_scanned_files_batch(1, files1)
        db.save_scanned_files_batch(2, files2)
        db.mark_folder_scanned(1)
        db.mark_folder_scanned(2)

        # Mark files as already processed
        db.mark_file_processed("file1.txt")
        db.mark_file_processed("file2.txt")

        merge_folders(folder1, folder2, output, db)

        # Should complete successfully
        assert (output / "file1.txt").exists()
        assert (output / "file2.txt").exists()

    def test_merge_folders_conflicts_already_resolved(self, temp_dir):
        """Test when all conflicts are already resolved (resume scenario)."""
        folder1 = temp_dir / "f1"
        folder2 = temp_dir / "f2"
        output = temp_dir / "out"
        folder1.mkdir()
        folder2.mkdir()
        output.mkdir()

        (folder1 / "conflict.txt").write_text("content1")
        (folder2 / "conflict.txt").write_text("content2")
        (output / "conflict.txt").write_text("content1")

        db_path = temp_dir / "test.db"
        db = CheckpointDB(db_path)

        # Scan folders
        from folder_merger.scanner import scan_folder
        files1, _ = scan_folder(folder1)
        files2, _ = scan_folder(folder2)
        db.save_scanned_files_batch(1, files1)
        db.save_scanned_files_batch(2, files2)
        db.mark_folder_scanned(1)
        db.mark_folder_scanned(2)

        # Mark conflict as already processed
        db.mark_file_processed("conflict.txt")

        merge_folders(folder1, folder2, output, db)

        assert (output / "conflict.txt").exists()

    def test_merge_folders_partial_copy_resume(self, temp_dir):
        """Test resuming when some files from each category are already copied."""
        folder1 = temp_dir / "f1"
        folder2 = temp_dir / "f2"
        output = temp_dir / "out"
        folder1.mkdir()
        folder2.mkdir()
        output.mkdir()

        # Files only in folder1
        (folder1 / "only1_a.txt").write_text("a")
        (folder1 / "only1_b.txt").write_text("b")

        # Files only in folder2
        (folder2 / "only2_a.txt").write_text("a")
        (folder2 / "only2_b.txt").write_text("b")

        # Identical files
        (folder1 / "identical.txt").write_text("same")
        (folder2 / "identical.txt").write_text("same")

        db_path = temp_dir / "test.db"
        db = CheckpointDB(db_path)

        # Pre-scan
        from folder_merger.scanner import scan_folder
        files1, _ = scan_folder(folder1)
        files2, _ = scan_folder(folder2)
        db.save_scanned_files_batch(1, files1)
        db.save_scanned_files_batch(2, files2)
        db.mark_folder_scanned(1)
        db.mark_folder_scanned(2)

        # Pre-copy and mark some as processed
        (output / "only1_a.txt").write_text("a")
        db.mark_file_processed("only1_a.txt")
        (output / "only2_a.txt").write_text("a")
        db.mark_file_processed("only2_a.txt")

        merge_folders(folder1, folder2, output, db)

        # All should exist
        assert (output / "only1_a.txt").exists()
        assert (output / "only1_b.txt").exists()
        assert (output / "only2_a.txt").exists()
        assert (output / "only2_b.txt").exists()
        assert (output / "identical.txt").exists()