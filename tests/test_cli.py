"""Tests for folder_merger.cli module."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from folder_merger.cli import parse_args, validate_args, confirm_output_overwrite, main


class TestParseArgs:
    """Tests for parse_args function."""

    def test_parse_basic_args(self):
        with patch.object(sys, "argv", ["prog", "folder1", "folder2", "output"]):
            args = parse_args()

        assert args.folder1 == Path("folder1")
        assert args.folder2 == Path("folder2")
        assert args.output == Path("output")

    def test_parse_default_db(self):
        with patch.object(sys, "argv", ["prog", "f1", "f2", "out"]):
            args = parse_args()

        assert args.db == Path("merge_checkpoint.db")

    def test_parse_custom_db(self):
        with patch.object(sys, "argv", ["prog", "--db", "custom.db", "f1", "f2", "out"]):
            args = parse_args()

        assert args.db == Path("custom.db")

    def test_parse_db_short_flag(self):
        with patch.object(sys, "argv", ["prog", "-d", "short.db", "f1", "f2", "out"]):
            args = parse_args()

        assert args.db == Path("short.db")

    def test_parse_reset_flag(self):
        with patch.object(sys, "argv", ["prog", "--reset", "f1", "f2", "out"]):
            args = parse_args()

        assert args.reset is True

    def test_parse_no_reset_flag(self):
        with patch.object(sys, "argv", ["prog", "f1", "f2", "out"]):
            args = parse_args()

        assert args.reset is False


class TestValidateArgs:
    """Tests for validate_args function."""

    def test_validate_valid_folders(self, sample_folders):
        folder1, folder2, _ = sample_folders

        class Args:
            pass

        args = Args()
        args.folder1 = folder1
        args.folder2 = folder2

        # Should not raise
        validate_args(args)

    def test_validate_folder1_not_exists(self, temp_dir):
        class Args:
            pass

        args = Args()
        args.folder1 = temp_dir / "nonexistent"
        args.folder2 = temp_dir

        with pytest.raises(SystemExit) as exc_info:
            validate_args(args)
        assert exc_info.value.code == 1

    def test_validate_folder2_not_exists(self, temp_dir):
        folder1 = temp_dir / "f1"
        folder1.mkdir()

        class Args:
            pass

        args = Args()
        args.folder1 = folder1
        args.folder2 = temp_dir / "nonexistent"

        with pytest.raises(SystemExit) as exc_info:
            validate_args(args)
        assert exc_info.value.code == 1

    def test_validate_folder1_is_file(self, temp_dir):
        file1 = temp_dir / "file.txt"
        file1.write_text("content")
        folder2 = temp_dir / "f2"
        folder2.mkdir()

        class Args:
            pass

        args = Args()
        args.folder1 = file1
        args.folder2 = folder2

        with pytest.raises(SystemExit) as exc_info:
            validate_args(args)
        assert exc_info.value.code == 1

    def test_validate_folder2_is_file(self, temp_dir):
        folder1 = temp_dir / "f1"
        folder1.mkdir()
        file2 = temp_dir / "file.txt"
        file2.write_text("content")

        class Args:
            pass

        args = Args()
        args.folder1 = folder1
        args.folder2 = file2

        with pytest.raises(SystemExit) as exc_info:
            validate_args(args)
        assert exc_info.value.code == 1


class TestConfirmOutputOverwrite:
    """Tests for confirm_output_overwrite function."""

    def test_confirm_empty_output(self, temp_dir):
        output = temp_dir / "empty_output"
        output.mkdir()

        result = confirm_output_overwrite(output)
        assert result is True

    def test_confirm_nonexistent_output(self, temp_dir):
        output = temp_dir / "nonexistent"

        result = confirm_output_overwrite(output)
        assert result is True

    def test_confirm_nonempty_output_yes(self, temp_dir):
        output = temp_dir / "nonempty"
        output.mkdir()
        (output / "file.txt").write_text("content")

        with patch("builtins.input", return_value="y"):
            result = confirm_output_overwrite(output)

        assert result is True

    def test_confirm_nonempty_output_no(self, temp_dir):
        output = temp_dir / "nonempty"
        output.mkdir()
        (output / "file.txt").write_text("content")

        with patch("builtins.input", return_value="n"):
            result = confirm_output_overwrite(output)

        assert result is False

    def test_confirm_nonempty_output_empty_input(self, temp_dir):
        output = temp_dir / "nonempty"
        output.mkdir()
        (output / "file.txt").write_text("content")

        with patch("builtins.input", return_value=""):
            result = confirm_output_overwrite(output)

        assert result is False


class TestMain:
    """Tests for main function."""

    def test_main_success(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        db_path = temp_dir / "test.db"

        with patch.object(
            sys, "argv",
            ["prog", str(folder1), str(folder2), str(output), "--db", str(db_path)]
        ):
            with patch("builtins.input", return_value="1"):
                main()

        assert (output / "only_in_1.txt").exists()

    def test_main_with_reset(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        db_path = temp_dir / "test.db"

        # Create existing db
        db_path.write_text("dummy")

        with patch.object(
            sys, "argv",
            ["prog", "--reset", str(folder1), str(folder2), str(output), "--db", str(db_path)]
        ):
            with patch("builtins.input", return_value="1"):
                main()

        assert (output / "only_in_1.txt").exists()

    def test_main_invalid_folder(self, temp_dir):
        with patch.object(
            sys, "argv",
            ["prog", str(temp_dir / "nonexistent"), str(temp_dir), str(temp_dir / "out")]
        ):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    def test_main_output_exists_abort(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        output.mkdir()
        (output / "existing.txt").write_text("existing")

        with patch.object(
            sys, "argv",
            ["prog", str(folder1), str(folder2), str(output)]
        ):
            with patch("builtins.input", return_value="n"):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 0

    def test_main_keyboard_interrupt(self, sample_folders, temp_dir):
        folder1, folder2, output = sample_folders
        db_path = temp_dir / "test.db"

        with patch.object(
            sys, "argv",
            ["prog", str(folder1), str(folder2), str(output), "--db", str(db_path)]
        ):
            with patch("folder_merger.cli.merge_folders", side_effect=KeyboardInterrupt):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1