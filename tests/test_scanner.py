"""Tests for folder_merger.scanner module."""

import pytest

from folder_merger.scanner import compute_file_hash, get_file_info, scan_folder


class TestComputeFileHash:
    """Tests for compute_file_hash function."""

    def test_hash_small_file(self, temp_dir):
        file_path = temp_dir / "small.txt"
        file_path.write_text("hello world")

        hash1 = compute_file_hash(file_path)
        assert isinstance(hash1, str)
        assert len(hash1) == 16  # xxhash64 produces 16 hex chars

    def test_hash_empty_file(self, temp_dir):
        file_path = temp_dir / "empty.txt"
        file_path.write_text("")

        hash_result = compute_file_hash(file_path)
        assert isinstance(hash_result, str)
        assert len(hash_result) == 16

    def test_hash_deterministic(self, temp_dir):
        file_path = temp_dir / "test.txt"
        file_path.write_text("test content")

        hash1 = compute_file_hash(file_path)
        hash2 = compute_file_hash(file_path)
        assert hash1 == hash2

    def test_different_content_different_hash(self, temp_dir):
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_text("content 1")
        file2.write_text("content 2")

        hash1 = compute_file_hash(file1)
        hash2 = compute_file_hash(file2)
        assert hash1 != hash2

    def test_same_content_same_hash(self, temp_dir):
        file1 = temp_dir / "file1.txt"
        file2 = temp_dir / "file2.txt"
        file1.write_text("identical content")
        file2.write_text("identical content")

        hash1 = compute_file_hash(file1)
        hash2 = compute_file_hash(file2)
        assert hash1 == hash2

    def test_hash_binary_file(self, temp_dir):
        file_path = temp_dir / "binary.bin"
        file_path.write_bytes(bytes(range(256)))

        hash_result = compute_file_hash(file_path)
        assert isinstance(hash_result, str)
        assert len(hash_result) == 16

    def test_hash_large_file(self, temp_dir):
        file_path = temp_dir / "large.txt"
        # Create file larger than default chunk size (65536)
        file_path.write_text("x" * 100000)

        hash_result = compute_file_hash(file_path)
        assert isinstance(hash_result, str)
        assert len(hash_result) == 16

    def test_hash_with_custom_chunk_size(self, temp_dir):
        file_path = temp_dir / "test.txt"
        file_path.write_text("test content")

        hash1 = compute_file_hash(file_path, chunk_size=1024)
        hash2 = compute_file_hash(file_path, chunk_size=65536)
        assert hash1 == hash2  # Same result regardless of chunk size

    def test_hash_nonexistent_file_raises(self, temp_dir):
        file_path = temp_dir / "nonexistent.txt"
        with pytest.raises(FileNotFoundError):
            compute_file_hash(file_path)


class TestGetFileInfo:
    """Tests for get_file_info function."""

    def test_get_file_info_basic(self, temp_dir):
        file_path = temp_dir / "test.txt"
        file_path.write_text("test content")

        info = get_file_info(temp_dir, "test.txt")

        assert info.relative_path == "test.txt"
        assert info.absolute_path == str(file_path)
        assert isinstance(info.hash, str)
        assert info.size == len("test content")
        assert isinstance(info.modified_time, float)

    def test_get_file_info_nested(self, temp_dir):
        subdir = temp_dir / "subdir"
        subdir.mkdir()
        file_path = subdir / "nested.txt"
        file_path.write_text("nested content")

        info = get_file_info(temp_dir, "subdir/nested.txt")

        assert info.relative_path == "subdir/nested.txt"
        assert info.absolute_path == str(file_path)

    def test_get_file_info_nonexistent_raises(self, temp_dir):
        with pytest.raises(FileNotFoundError):
            get_file_info(temp_dir, "nonexistent.txt")


class TestScanFolder:
    """Tests for scan_folder function."""

    def test_scan_empty_folder(self, temp_dir):
        empty_dir = temp_dir / "empty"
        empty_dir.mkdir()

        files, errors = scan_folder(empty_dir)
        assert files == {}
        assert errors == []

    def test_scan_flat_folder(self, temp_dir):
        folder = temp_dir / "flat"
        folder.mkdir()
        (folder / "file1.txt").write_text("content 1")
        (folder / "file2.txt").write_text("content 2")

        files, errors = scan_folder(folder)

        assert len(files) == 2
        assert "file1.txt" in files
        assert "file2.txt" in files
        assert errors == []

    def test_scan_nested_folder(self, temp_dir):
        folder = temp_dir / "nested"
        folder.mkdir()
        (folder / "root.txt").write_text("root")
        (folder / "sub1").mkdir()
        (folder / "sub1" / "file1.txt").write_text("sub1 file")
        (folder / "sub1" / "sub2").mkdir()
        (folder / "sub1" / "sub2" / "deep.txt").write_text("deep file")

        files, errors = scan_folder(folder)

        assert len(files) == 3
        assert "root.txt" in files
        assert "sub1/file1.txt" in files  # Always POSIX-style
        assert errors == []

    def test_scan_folder_with_desc(self, temp_dir):
        folder = temp_dir / "test"
        folder.mkdir()
        (folder / "file.txt").write_text("content")

        files, errors = scan_folder(folder, desc="Custom Description")
        assert len(files) == 1
        assert errors == []

    def test_scan_folder_returns_file_info(self, temp_dir):
        folder = temp_dir / "test"
        folder.mkdir()
        (folder / "file.txt").write_text("content")

        files, _ = scan_folder(folder)
        file_info = files["file.txt"]

        assert file_info.relative_path == "file.txt"
        assert file_info.size == len("content")
        assert isinstance(file_info.hash, str)
        assert isinstance(file_info.modified_time, float)

    def test_scan_sample_folders(self, sample_folders):
        folder1, folder2, _ = sample_folders

        files1, _ = scan_folder(folder1)
        files2, _ = scan_folder(folder2)

        # folder1 should have: only_in_1.txt, subdir/nested.txt, identical.txt, conflict.txt
        assert len(files1) == 4
        assert "only_in_1.txt" in files1
        assert "identical.txt" in files1
        assert "conflict.txt" in files1

        # folder2 should have: only_in_2.txt, identical.txt, conflict.txt
        assert len(files2) == 3
        assert "only_in_2.txt" in files2
        assert "identical.txt" in files2
        assert "conflict.txt" in files2

        # Check identical files have same hash
        assert files1["identical.txt"].hash == files2["identical.txt"].hash

        # Check conflicting files have different hash
        assert files1["conflict.txt"].hash != files2["conflict.txt"].hash

    def test_scan_folder_uses_posix_paths(self, temp_dir):
        """Test that relative paths always use forward slashes (POSIX style)."""
        folder = temp_dir / "nested"
        folder.mkdir()
        (folder / "level1").mkdir()
        (folder / "level1" / "level2").mkdir()
        (folder / "level1" / "level2" / "deep.txt").write_text("deep")

        files, _ = scan_folder(folder)

        # Path should use forward slashes regardless of OS
        assert "level1/level2/deep.txt" in files
        # Should NOT have backslashes
        assert "level1\\level2\\deep.txt" not in files


class TestScanError:
    """Tests for ScanError class and error handling."""

    def test_scan_error_attributes(self):
        from folder_merger.scanner import ScanError
        error = ScanError("path/to/file.txt", "/abs/path/to/file.txt", "Permission denied")
        assert error.relative_path == "path/to/file.txt"
        assert error.absolute_path == "/abs/path/to/file.txt"
        assert error.error == "Permission denied"

    def test_scan_folder_returns_errors_for_unreadable_files(self, temp_dir):
        """Test that scan errors are captured for files that can't be read."""
        from unittest.mock import patch
        from folder_merger.scanner import ScanError

        folder = temp_dir / "test"
        folder.mkdir()
        (folder / "good.txt").write_text("good content")
        (folder / "bad.txt").write_text("bad content")

        # Mock get_file_info to fail on bad.txt
        original_get_file_info = __import__('folder_merger.scanner', fromlist=['get_file_info']).get_file_info

        def mock_get_file_info(base_path, rel_path):
            if "bad.txt" in rel_path:
                raise OSError("Simulated read error")
            return original_get_file_info(base_path, rel_path)

        with patch('folder_merger.scanner.get_file_info', side_effect=mock_get_file_info):
            files, errors = scan_folder(folder)

        assert len(files) == 1
        assert "good.txt" in files
        assert len(errors) == 1
        assert errors[0].relative_path == "bad.txt"
        assert "Simulated read error" in errors[0].error

    def test_scan_folder_on_error_fail_raises(self, temp_dir):
        """Test that on_error='fail' raises exceptions."""
        from unittest.mock import patch

        folder = temp_dir / "test"
        folder.mkdir()
        (folder / "file.txt").write_text("content")

        def mock_get_file_info(base_path, rel_path):
            raise OSError("Simulated error")

        with patch('folder_merger.scanner.get_file_info', side_effect=mock_get_file_info):
            with pytest.raises(OSError, match="Simulated error"):
                scan_folder(folder, on_error="fail")