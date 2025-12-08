"""Tests for folder_merger.__main__ module."""

import sys
from unittest.mock import patch

import pytest


class TestMainModule:
    """Tests for __main__.py entry point."""

    def test_main_module_runs(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        db_path = temp_dir / "test.db"

        with patch.object(
            sys, "argv",
            ["prog", str(folder1), str(folder2), str(output), "--db", str(db_path)]
        ):
            with patch("builtins.input", return_value="1"):
                from folder_merger.__main__ import main
                main()

        assert (output / "only_in_1.txt").exists()

    def test_main_module_importable(self):
        """Test that __main__ can be imported."""
        import folder_merger.__main__ as main_module
        assert hasattr(main_module, "main")